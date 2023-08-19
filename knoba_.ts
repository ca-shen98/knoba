import assert from 'node:assert';
import { v4 as uuidv4 } from 'uuid'
import { axios } from "@pipedream/platform"
var process;

type KnobaProps = { knobaId: string, content: string, externalIds: Set<string> };
type KnobaBlockMatch = { score: number, props: KnobaProps };
type KnobaBlockChange = { newContent: string, newContentEmbedding: number[], knobaMatch: KnobaBlockMatch };
type StagedKnobaBlockChange = { knobaBlockChange: KnobaBlockChange, externalIdsDelta: { u: Set<string>, d: Set<string> } };

    async function getIngestContentsBlocks(qExternalId: string, $): Promise<string[]> { // q for qualified
      const qSplitIdx = qExternalId.indexOf("_");
      const externalTypeQ = qExternalId.substring(0, qSplitIdx);
      const rawExternalId = qExternalId.substring(qSplitIdx + 1);
      switch (externalTypeQ) {
        case "notion":
          const notionGetBlockResp = await axios($, {
            url: `https://api.notion.com/v1/blocks/${rawExternalId}`,
            headers: {
              Authorization: `Bearer ${$.notion.$auth.oauth_access_token}`,
              "Notion-Version": `2022-06-28`,
            },
          });
          console.log(notionGetBlockResp);
          return [notionGetBlockResp.paragraph.rich_text[0].plain_text.trim()];
        case "gdocs":
          const gdocsGetBodyResp = await axios($, {
            method: "GET",
            url: `https://docs.googleapis.com/v1/documents/${rawExternalId}`,
            headers: {
              Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
            },
          }).body.content;
          console.log(gdocsGetBodyResp);
          return gdocsGetBodyResp.filter((content) => content.paragraph)
            .map(({ paragraph }) => paragraph.elements.map((element) => element.textRun.content).filter((content) => content && content.trim()).join('').trim())
            .filter((content) => content && content.trim());
          default:
            return [];
      }
    };

    async function getKnobaBlockChangesBatch(qExternalIdsBatch: string[], $): Promise<KnobaBlockChange[][]> { // q for qualified
      const ingestContentsBlocksBatch = await Promise.all(qExternalIdsBatch.map(async (qExternalId) => await getIngestContentsBlocks(qExternalId, $)));
      const flattenedIngestContentsBlocksBatch = ingestContentsBlocksBatch.flat();
      const inBlockEmbeddings = flattenedIngestContentsBlocksBatch.length > 0
        ? (await axios($, {
          method: "POST",
          url: "https://api.openai.com/v1/embeddings",
          headers: {
            Authorization: `Bearer ${$.openai.$auth.api_key}`,
          },
          data: {
            model: "text-embedding-ada-002",
            input: flattenedIngestContentsBlocksBatch,
          },
        })).data : [];
      console.log(inBlockEmbeddings);
      const knobaBlockChangesBatch: KnobaBlockChange[][] = [];
      assert(inBlockEmbeddings.length == flattenedIngestContentsBlocksBatch.length);
      for (const { embedding } of inBlockEmbeddings) {
        var lidx = knobaBlockChangesBatch.length - 1;
        assert(knobaBlockChangesBatch.length <= ingestContentBlocksBatch.length && (knobaBlockChangesBatch.length == 0 || knobaBlockChangesBatch[lidx].length <= ingestContentBlocksBatch[lidx].length), "valid inductive prior state");
        assert(knobaBlockChangesBatch.length < ingestContentBlocksBatch.length || (knobaBlockChangesBatch.length > 0 && knobaBlockChangesBatch[lidx].length < ingestContentBlocksBatch[lidx].length), "can push next item");
        while (knobaBlockChangesBatch.length == 0 || knobaBlockChangesBatch[lidx].length == knobaBlockChangesBatch[lidx].length) {
          knobaBlockChangesBatch.push([]);
          lidx += 1;
        }
        const queryMatch = (await axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/query`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: {
            topK: 1,
            vector: embedding,
            includeValues: false,
            includeMetadata: true,
          },
        })).matches;
        console.log(queryMatch);
        const wrappedMaybeMatch = queryMatch.filter((match) => Math.abs(match.score - 1) < 0.1).map((match) => ({
          score: match.score,
          props: {
            knobaId: match.id,
            content: match.metadata.content,
            externalIds: new Set(JSON.parse(match.metadata.external_ids)),
          },
        }));
        const knobaMatch = wrappedMaybeMatch.length == 0
          ? {
            score: 1,
            props: {
              knobaId: uuidv4(), // ignoring collision handling
              content: ingestContentBlocksBatch[lidx][knobaBlockChangesBatch[lidx].length],
              externalIds: new Set([qExternalIdsBatch[lidx]]),
            },
          } : wrappedMaybeMatch[0];
          assert(knobaMatch.props.content.trim().length > 0);
          assert(knobaMatch.props.externalIds.size > 0);
        if (wrappedMaybeMatch.length == 0) {
          // upsert inline immediately instead of batch for later because subsequent "new" content may be same
          await axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              vectors: [{
                id: knobaMatch.knobaId,
                values: embedding,
                metadata: {
                  content: knobaMatch.content,
                  knoba_id: knobaMatch.knobaId,
                  external_ids: JSON.stringify(Array.from(knobaMatch.externalIds)),
                },
              }],
            },
          });
        }
        knobaBlockChangesBatch[lidx].push({
          newContent: ingestContentBlocksBatch[lidx][knobaBlockChangesBatch[lidx].length],
          newContentEmbedding: embedding,
          knobaMatch,
        });
      }
      assert(knobaBlockChangesBatch.length == ingestContentsBlocksBatch.length && knobaBlockChangesBatch.every((knobaBlockChanges, index) => knobaBlockChanges.length == ingestContentsBlocksBatch[index].length));
      return knobaBlockChangesBatch;
    };
    async function pivotAndFillKnobaIdBlockChanges(knobaBlockChangesBatch: KnobaBlockChange[][], oldRKnobaIdsBatch: Set<string>[], allQExternalIdsBatch: string[], $): Promise<{ [knobaId: string]: StagedKnobaBlockChange }> { // R for referenced; u for upsert, d for drop; q for qualified
      assert(knobaBlockChangesBatch.length == allQExternalIdsBatch.length && oldRKnobaIdsBatch.length == allQExternalIdsBatch.length);
      const knobaIdBlockChanges: { [knobaId: string]: StagedKnobaBlockChange } = knobaBlockChangesBatch
        .reduce((knobaIdBlockChanges, knobaBlockChanges, index) => {
          knobaBlockChanges.forEach((knobaBlockChange) => {
            if (!(knobaBlockChange.knobaMatch.props.knobaId in knobaIdBlockChanges)) {
              knobaIdBlockChanges[knobaBlockChange.knobaMatch.props.knobaId] = {
                knobaBlockChange,
                externalIdsDelta: { u: new Set(), d: new Set() },
              };
            }
            knobaIdBlockChanges[knobaBlockChange.knobaMatch.props.knobaId].externalIdsDelta.u.add(allQExternalIdsBatch[index]);
          });
          return knobaIdBlockChanges;
        }, {});
      const droppedKnobaIds = Array.from(new Set(oldRKnobaIdsBatch.map((knobaIds) => Array.from(knobaIds)).flat())).filter((knobaId) => !(knobaId in knobaIdBlockChanges));
      const knobaBlocksForDroppedIds = droppedKnobaIds.length > 0
        ? (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: droppedKnobaIds },
        })).vectors : {};
      console.log(knobaBlocksForDroppedIds);
      oldRKnobaIdsBatch.forEach((oldRKnobaIds, index) => oldRKnobaIds.forEach((knobaId) => {
        if (!(knobaId in knobaIdBlockChanges)) {
          assert(knobaId in knobaBlocksForDroppedIds);
          knobaIdBlockChanges[knobaId] = {
            knobaBlockChange: {
              newContent: knobaBlocksForDroppedIds[knobaId].metadata.content,
              newContentEmbedding: knobaBlocksForDroppedIds[knobaId].values,
              knobaMatch: {
                score: 1,
                props: {
                  knobaId: knobaId,
                  content: knobaBlocksForDroppedIds[knobaId].metadata.content,
                  externalIds: new Set(JSON.parse(knobaBlocksForDroppedIds[knobaId].metadata.external_ids)),
                },
              },
            },
            externalIdsDelta: { u: new Set(), d: new Set() },
          };
          assert(knobaIdBlockChanges[knobaId].knobaBlockChange.knobaMatch.props.externalIds.size > 0);
        }
        if (!knobaIdBlockChanges[knobaId].externalIdsDelta.u.has(allQExternalIdsBatch[index])) {
          knobaIdBlockChanges[knobaId].externalIdsDelta.d.add(allQExternalIdsBatch[index]);
        }
      }));
      return knobaIdBlockChanges;
    };
    async function handlReceive(uqExternalIdsBatch: string[], dqExternalIdsBatch: string[], $): Promise<void> { // u for upsert, d for drop; q for qualified
      assert(uqExternalIdsBatch.length + dqExternalIdsBatch.length == new Set(uqExternalIdsBatch.concat(dqExternalIdsBatch)).size, "no duplicates (implies partitioned as well)");
      const knobaBlockChangesBatch = (await getKnobaBlockChangesBatch(uqExternalIdsBatch, $)).concat(dqExternalIdsBatch.map(() => []));
      const allQExternalIdsBatch = uqExternalIdsBatch.concat(dqExternalIdsBatch);
      const oldRKnobaIdsBatch: Set<string>[] = await Promise.all(allQExternalIdsBatch.map(async (qExternalId) => { // r for referenced
        const knobaIdsStr = await $.myDatastore.get(qExternalId);
        return knobaIdsStr ? new Set(JSON.parse(knobaIdsStr)) : new Set();
      })); // need to get initial state before following mutate
      assert(knobaBlockChangesBatch.length == allQExternalIdsBatch.length);
      await Promise.all(knobaBlockChangesBatch.map(async (knobaBlockChanges, index) => {
        const newKnobaIds = knobaBlockChanges.map(({ knobaMatch: { props : { knobaId } } }) => knobaId);
        if (newKnobaIds.length > 0) {
          await $.myDatastore.set(allQExternalIdsBatch[index], JSON.stringify(newKnobaIds));
        } else {
          await $.myDatastore.delete(allQExternalIdsBatch[index]);
        }
      })); // upsert now so that subsequent fetches for materialization are up to date
      const knobaIdBlockChanges = await pivotAndFillKnobaIdBlockChanges(knobaBlockChangesBatch, oldRKnobaIdsBatch, allQExternalIdsBatch, $);
      assert(Array.from(new Set(oldRKnobaIdsBatch.map((knobaIds) => Array.from(knobaIds)).flat().concat(knobaBlockChangesBatch.flat().map(({ knobaMatch: { props: { knobaId } } }) => knobaId)))).every((knobaId) => knobaId in knobaIdBlockChanges));

      if (dKnobaIds.size > 0) {
        await axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: dKnobaIds },
        });
      } // delete before sqrBatch to avoid unneccessary computing for to-be-deleted with empty externalIds

      const stagedKnobaReplacements = await getNewKnobaReplacements();
      
    };
