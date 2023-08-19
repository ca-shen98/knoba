import isEqual from 'lodash.isequal';
import assert from 'node:assert';
import { v4 as uuidv4 } from 'uuid';
import { axios } from "@pipedream/platform";
export default defineComponent({
  props: {
    openai: {
      type: "app",
      app: "openai",
    },
    pinecone: {
      type: "app",
      app: "pinecone",
    },
  },
  async run({ steps, $ }) {
    var process;
    type KnobaBlockProps = { knobaId: string, content: string, externalIds: Set<string> };
    type KnobaBlockMatch = { score: number, props: KnobaBlockProps };
    type KnobaBlockChange = { newContentForMatchOrTrigger: string, newContentEmbeddingOrTriggerOldContent: number[] | string, knobaMatch: KnobaBlockMatch };
    async function getKnobaBlockChangesBatch(qExternalIdIngestContentsBlocksBatch: { [qExternalId: string]: string[] }, $): Promise<{ [qExternalId: string]: KnobaBlockChange[] }> {
      // indexed input map entries to be able to remap (zip) back flattened embeddings with input contents
      // flatten to be able to do one batch embeddings generation call
      const qExternalIdIngestContentsBlocksBatchEntries = Object.entries(qExternalIdIngestContentsBlocksBatch);
      const flattenedIngestContentsBlocks = qExternalIdIngestContentsBlocksBatchEntries
        .map(([_, qExternalIdIngestContentsBlocks]) => qExternalIdIngestContentsBlocks).flat();
      assert(flattenedIngestContentsBlocks.every((content) => content.trim()));
      const flatIngestContentsBlocksEmbeddings = flattenedIngestContentsBlocks.length > 0
        ? (await axios($, {
          method: "POST",
          url: "https://api.openai.com/v1/embeddings",
          headers: {
            Authorization: `Bearer ${$.openai.$auth.api_key}`,
          },
          data: {
            model: "text-embedding-ada-002",
            input: flattenedIngestContentsBlocks,
          },
        })).data : [];
      console.log(flatIngestContentsBlocksEmbeddings);
      assert(flatIngestContentsBlocksEmbeddings.length == flattenedIngestContentsBlocks.length);
      const knobaBlockChangesBatch: { [qExternalId: string]: KnobaBlockChange[] } = {};
      for (const { embedding } of flatIngestContentsBlocksEmbeddings) {
        var offidx = Object.keys(knobaBlockChangesBatch).length - 1;
        assert(offidx < qExternalIdIngestContentsBlocksBatchEntries.length);
        var wrappedMaybeLastQExternalId = offidx >= 0 ? [qExternalIdIngestContentsBlocksBatchEntries[offidx][0]] : [];
        assert(offidx < 0 || knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].length
          <= qExternalIdIngestContentsBlocksBatchEntries[offidx][1].length, "valid inductive prior state");
        assert(offidx + 1 < qExternalIdIngestContentsBlocksBatchEntries.length
            // in loop implies qExternalIdIngestContentsBlocksBatchEntries.length > 0,
            // short circuit false implies offidx > 0 implies wrappedMaybeLastQExternalId non-empty
            || knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].length
              < qExternalIdIngestContentsBlocksBatchEntries[offidx][1].length,
          "can push next item (current `embedding` iteratee)");
        // if completed last active qExternalId, iterate to next non-empty one to start zipping embeddings with
        while (offidx < 0 || knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].length
          == qExternalIdIngestContentsBlocksBatchEntries[offidx][1].length) {
          offidx += 1;
          assert(offidx < qExternalIdIngestContentsBlocksBatchEntries.length);
          wrappedMaybeLastQExternalId = [qExternalIdIngestContentsBlocksBatchEntries[offidx][0]];
          assert (!(wrappedMaybeLastQExternalId[0] in knobaBlockChangesBatch));
          knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]] = [];
        }
        assert(offidx >= 0 && wrappedMaybeLastQExternalId.length > 0
          && wrappedMaybeLastQExternalId[0] in knobaBlockChangesBatch);
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
        const wrappedMaybeMatch = queryMatch
          .filter((match) => Math.abs(match.score - 1) < 0.1).map((match) => ({
            score: match.score,
            props: {
              knobaId: match.id,
              content: match.metadata.content,
              externalIds: new Set(JSON.parse(match.metadata.external_ids)),
            },
          }));
        const knobaMatch = wrappedMaybeMatch.length > 0 ? wrappedMaybeMatch[0] : {
          score: 1,
          props: {
            knobaId: uuidv4(), // ignoring collision handling
            content: qExternalIdIngestContentsBlocksBatchEntries[offidx][1]
              [knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].length],
            externalIds: new Set([qExternalIdIngestContentsBlocksBatchEntries[offidx][0]]),
          },
        };
        assert(knobaMatch.props.content.trim().length > 0);
        assert(knobaMatch.props.externalIds.size > 0);
        if (wrappedMaybeMatch.length == 0) {
          // upsert inline immediately instead of batch for later because subsequent "new" content may match
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
        knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].push({
          newContentForMatchOrTrigger: qExternalIdIngestContentsBlocksBatchEntries[offidx][1]
            [knobaBlockChangesBatch[wrappedMaybeLastQExternalId[0]].length],
          newContentEmbeddingOrTriggerOldContent: embedding,
          knobaMatch,
        });
      }
      assert(Object.keys(knobaBlockChangesBatch).length == Object.keys(qExternalIdIngestContentsBlocksBatch).length
        && Object.keys(qExternalIdIngestContentsBlocksBatch).every((qExternalId) =>
          qExternalId in knobaBlockChangesBatch
            && knobaBlockChangesBatch[qExternalId].length
              == qExternalIdIngestContentsBlocksBatch[qExternalId].length));
      return knobaBlockChangesBatch;
    };
    const qExternalIdIngestContentsBlocksBatch = {
      "notion_284d10157fbf43cca485bf30d908add3": ["Sample paragraph"],
    };
    const knobaBlockChangesBatch = await getKnobaBlockChangesBatch(qExternalIdIngestContentsBlocksBatch, this);
    return knobaBlockChangesBatch;
  },
})