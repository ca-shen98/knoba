import { v4 as uuidv4 } from 'uuid'
import { axios } from "@pipedream/platform"
var process, assert;
    
    type KnobaIdContent = {
      knobaId: string,
      embedding: number[],
      content: string,
      externalIds: Set<string>,
    };
    export async function handleRemove(qExternalIdsBatch: string[], $): Promise<void> {
      const mKnobaIdsBatch: Set<string>[] = await Promise.all(qExternalIdsBatch.map(async (qExternalId) => {
        const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
        return mKnobaIdsStr ? new Set(JSON.parse(mKnobaIdsStr)) : new Set();
      })); // need to get initial state before following mutate
      await Promise.all(qExternalIdsBatch.map(async (qExternalId) => await $.myDatastore.delete(qExternalId)));
      const fmKnobaIds = new Set(mKnobaIdsBatch.map((mKnobaIds) => Array.from(mKnobaIds)).flat());
      const fetchedContent = fmKnobaIds.size > 0
        ? (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: fmKnobaIds },
        })).vectors : {};
      console.log(fetchedContent);
      assert(Object.keys(fetchedContent).length == fmKnobaIds.size);
      type GDUKnobaIdContents = { deletes: Set<string>, dupserts: { [knobaId: string]: KnobaIdContent } };
      const gduKnobaIdContents: GDUKnobaIdContents = mKnobaIdsBatch
        .reduce((gduKnobaIdContents, mKnobaIds, index) => {
          mKnobaIds.forEach((mKnobaId) => {
            assert(!gduKnobaIdContents.deletes.has(mKnobaId));
            const externalIds = mKnobaId in gduKnobaIdContents.dupserts
              ? gduKnobaIdContents.dupserts[mKnobaId].externalIds
              : new Set(JSON.parse(fetchedContent[mKnobaId].metadata.external_ids));
            if (externalIds.size == 1 && externalIds.has(qExternalIdsBatch[index])) {
              delete gduKnobaIdContents.dupserts[mKnobaId];
              gduKnobaIdContents.deletes.add(mKnobaId);
            } else {
              if (!(mKnobaId in gduKnobaIdContents.dupserts)) {
                gduKnobaIdContents.dupserts[mKnobaId] = {
                  knobaId: fetchedContent[mKnobaId].id,
                  embedding: fetchedContent[mKnobaId].values,
                  content: fetchedContent[mKnobaId].metadata.content,
                  externalIds: new Set(externalIds),
                };
              }
              gduKnobaIdContents.dupserts[mKnobaId].externalIds.delete(qExternalIdsBatch[index]);
            }
          });
          return gduKnobaIdContents;
        }, { deletes: new Set<string>(), dupserts: {} });
      assert(gduKnobaIdContents.deletes.size + Object.keys(gduKnobaIdContents.dupserts).length == fmKnobaIds.size);
      assert(new Set(Array.from(gduKnobaIdContents.deletes)
        .concat(Object.keys(gduKnobaIdContents.dupserts)))
        .size == fmKnobaIds.size)
      assert(Object.values(gduKnobaIdContents.dupserts).every(({ externalIds }) => externalIds.size > 0));
      const emptyPromises: Promise<any>[] = [];
      await Promise.all(emptyPromises
        .concat(gduKnobaIdContents.deletes.size > 0
          ? [axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: { ids: gduKnobaIdContents.deletes },
          })] : [])
        .concat(Object.values(gduKnobaIdContents.dupserts).length > 0
          ? [axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              vectors: Object.values(gduKnobaIdContents.dupserts).map((gduKnobaIdContent) => ({
                id: gduKnobaIdContent.knobaId,
                values: gduKnobaIdContent.embedding,
                metadata: {
                  content: gduKnobaIdContent.content,
                  external_ids: gduKnobaIdContent.externalIds,
                },
              })),
            },
          })]: [])
      );
    };
    async function getContentsBatch(qExternalIdsBatch: string[], $): Promise<string[][]> {
      return await Promise.all(qExternalIdsBatch.map(async (qExternalId) => {
        const prefixSplitIdx = qExternalId.indexOf("_");
        const externalTypeQ = qExternalId.substring(0, prefixSplitIdx);
        const rawExternalId = qExternalId.substring(prefixSplitIdx + 1);
        switch (externalTypeQ) {
          case "notion":
            const notionGetResp = await axios($, {
              url: `https://api.notion.com/v1/blocks/${rawExternalId}`,
              headers: {
                Authorization: `Bearer ${$.notion.$auth.oauth_access_token}`,
                "Notion-Version": `2022-06-28`,
              },
            });
            console.log(notionGetResp);
            return [notionGetResp.paragraph.rich_text[0].plain_text.trim()];
          case "gdocs":
            const gdocsGetResp = await axios($, {
              method: "GET",
              url: `https://docs.googleapis.com/v1/documents/${rawExternalId}`,
              headers: {
                Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
              },
            });
            console.log(gdocsGetResp);
            return gdocsGetResp.body.content
              .filter((content) => content.paragraph)
              .map(({ paragraph }) =>
                paragraph.elements
                  .map((element) => element.textRun.content)
                  .filter((content) => content && content.trim())
                  .join('').trim())
              .filter((content) => content && content.trim());
            default:
              return [];
        }
      }));
    };
    type KnobaMatch = { score: number, match: KnobaIdContent };
    type IngestedContentDataItem = { embedding: number[], content: string, maybeKnobaMatch?: KnobaMatch };
    async function getIngestedContentDataBatch(qExternalIdsBatch: string[], $): Promise<IngestedContentDataItem[][]> {
      const contentsBatch = await getContentsBatch(qExternalIdsBatch, $);
      const fContentsBatch = contentsBatch.flat();
      if (fContentsBatch.length == 0) {
        return contentsBatch.map(() => []);
      }
      const fEmbeddingsBatch = (await axios($, {
        method: "POST",
        url: `https://api.openai.com/v1/embeddings`,
        headers: {
          Authorization: `Bearer ${$.openai.$auth.api_key}`,
        },
        data: {
          model: "text-embedding-ada-002",
          input: fContentsBatch,
        },
      })).data;
      console.log(fEmbeddingsBatch);
      assert(fEmbeddingsBatch.length == fContentsBatch.length);
      return await fEmbeddingsBatch.reduce(async (contentEmbeddingsBatch, embedding) => {
        while (contentEmbeddingsBatch[-1].length >= contentsBatch[contentEmbeddingsBatch.length - 1].length) {
          contentEmbeddingsBatch.push([]);
        }
        const tQueriedMatch = await axios($, {
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
        }).matches;
        console.log(tQueriedMatch);
        const wrMaybeMatch = tQueriedMatch.filter((match) => Math.abs(match.score - 1) < 0.25)
          .map(({ score, id, values, metadata }) => ({
            score,
            match: {
              knobaId: id,
              embedding: values,
              content: metadata.content,
              externalIds: new Set(JSON.parse(metadata.external_ids)),
            },
          }));
        contentEmbeddingsBatch[-1].push({
          embedding,
          content: contentsBatch[contentEmbeddingsBatch.length - 1][contentEmbeddingsBatch[-1].length],
          maybeKnobaMatch: wrMaybeMatch.length > 0 ? wrMaybeMatch[0] : undefined,
        });
        return contentEmbeddingsBatch;
      }, []);
    };
    export async function handleUpsert(qExternalIdsBatch: string[], $): Promise<void> {
      const udKnobaIdContentsBatch = (await getIngestedContentDataBatch(qExternalIdsBatch, $))
        .map((inContentData, index) => inContentData.map(({ content, embedding, maybeKnobaMatch }) => {
          if (maybeKnobaMatch && Math.abs(maybeKnobaMatch.score - 1) < 0.1) {
            return {
              uKnobaIdContent: {
                knobaId: maybeKnobaMatch.match.knobaId,
                embedding,
                content,
                externalIds: new Set(maybeKnobaMatch.match.externalIds).add(qExternalIdsBatch[index]),
              },
              ...(Math.abs(maybeKnobaMatch.score - 1) < 0.01 && { dKnobaIdContent: {
                knobaId: maybeKnobaMatch.match.knobaId,
                embedding: maybeKnobaMatch.match.embedding,
                content: maybeKnobaMatch.match.content,
                externalIds: maybeKnobaMatch.match.externalIds,
              } }),
            };
          } else {
            return {
              uKnobaIdContent: {
                knobaId: uuidv4(),
                embedding,
                content,
                externalIds: new Set(qExternalIdsBatch[index]),
              },
            };
          }
        }));
      assert(udKnobaIdContentsBatch.length == qExternalIdsBatch.length);
      const mKnobaIdsBatch: Set<string>[] = await Promise.all(qExternalIdsBatch.map(async (qExternalId) => {
        const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
        return mKnobaIdsStr ? new Set(JSON.parse(mKnobaIdsStr)) : new Set();
      })); // need to get initial state before following mutate
      await Promise.all(udKnobaIdContentsBatch.map(async (udKnobaIdContents, index) => {
        const onKnobaIds = udKnobaIdContents.map(({ uKnobaIdContent: { knobaId } }) => knobaId);
        if (onKnobaIds.length > 0) {
          await $.myDatastore.set(qExternalIdsBatch[index], JSON.stringify(onKnobaIds));
        } else {
          await $.myDatastore.delete(qExternalIdsBatch[index]);
        }
      }));
      type MUDKnobaIdContent = { uKnobaIdContent: KnobaIdContent, dKnobaIdContent?: KnobaIdContent };
      type MUDKnobaIdContents = { [knobaId: string]: {
        knobaIdContents?: MUDKnobaIdContent,
        ud: { u?: Set<string>, d?: Set<string> },
      } };
      const mudKnobaIdContents: MUDKnobaIdContents = udKnobaIdContentsBatch
        .reduce((mudKnobaIdContents, udKnobaIdContents, index) => {
          udKnobaIdContents.forEach((udKnobaIdContent) => {
            if (!(udKnobaIdContent.uKnobaIdContent.knobaId in mudKnobaIdContents)) {
              mudKnobaIdContents[udKnobaIdContent.uKnobaIdContent.knobaId] = {
                knobaIdContents: udKnobaIdContent,
                ud: { u: new Set() },
              };
            }
            mudKnobaIdContents[udKnobaIdContent.uKnobaIdContent.knobaId].u.add(qExternalIdsBatch[index]);
          });
          return mudKnobaIdContents;
        }, {});
      // combine remove into here?
      mKnobaIdsBatch.forEach((mKnobaIds, index) => mKnobaIds.forEach((mKnobaId) => {
        if (!(mKnobaId in mudKnobaIdContents)) {
          mudKnobaIdContents[mKnobaId] = { ud: {} };
        }
        if (!("u" in mudKnobaIdContents[mKnobaId].ud)
          || !mudKnobaIdContents[mKnobaId].ud.u?.has(qExternalIdsBatch[index])) {
          if (!("d" in mudKnobaIdContents[mKnobaId].ud)) {
            mudKnobaIdContents[mKnobaId].ud.d = new Set();
          }
          mudKnobaIdContents[mKnobaId].ud.d?.add(qExternalIdsBatch[index]);
        }
      }));
      assert(Array.from(new Set(mKnobaIdsBatch.map((mKnobaIds) => Array.from(mKnobaIds)).flat()))
        .every((mKnobaId) => mKnobaId in mudKnobaIdContents));
      const dKnobaIds = Object.keys(mudKnobaIdContents)
        .filter((mudKnobaId) => !("u" in mudKnobaIdContents[mudKnobaId].ud));
      const fetchedContent = dKnobaIds.length > 0
        ? (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: dKnobaIds },
        })).vectors : {};
      console.log(fetchedContent);
      assert(Object.keys(fetchedContent).length == dKnobaIds.length);
      type GUDKnobaIdContents = { deletes: string[], upserts: MUDKnobaIdContent[] };
      const gudKnobaIdContents = Object.entries(mudKnobaIdContents)
        .reduce((gudKnobaIdContents: GUDKnobaIdContents,
          [mudKnobaId, { knobaIdContents, ud: { u: uqExternalIds, d: dqExternalIds } }]) => {
          if (!uqExternalIds) {
            assert(dqExternalIds && dqExternalIds.size > 0);
            assert(mudKnobaId in fetchedContent);
            const fExternalIds: Set<string> = new Set(JSON.parse(fetchedContent[mudKnobaId].metadata.external_ids));
            if (fExternalIds.size == dqExternalIds?.size
              && Array.from(fExternalIds).every((fExternalId) => dqExternalIds?.has(fExternalId))) {
                gudKnobaIdContents.deletes.push(mudKnobaId);
            } else { // >
              dqExternalIds?.forEach((dqExternalId) => fExternalIds.delete(dqExternalId));
              gudKnobaIdContents.upserts.push({
                uKnobaIdContent: {
                  knobaId: mudKnobaId,
                  embedding: fetchedContent[mudKnobaId].embedding,
                  content: fetchedContent[mudKnobaId].metadata.content,
                  externalIds: fExternalIds,
                },
              });
            }
          } else {
            assert(uqExternalIds.size > 0 && (!dqExternalIds || Array.from(dqExternalIds)
              .every((dqExternalId) => !uqExternalIds.has(dqExternalId))));
            assert(knobaIdContents);
            if (knobaIdContents) {
              const udExternalIds = knobaIdContents.uKnobaIdContent.externalIds;
              uqExternalIds.forEach((uqExternalId) => udExternalIds?.add(uqExternalId));
              dqExternalIds?.forEach((dqExternalId) => udExternalIds?.delete(dqExternalId));
              gudKnobaIdContents.upserts.push({
                uKnobaIdContent: {
                  knobaId: knobaIdContents.uKnobaIdContent.knobaId,
                  embedding: knobaIdContents.uKnobaIdContent.embedding,
                  content: knobaIdContents.uKnobaIdContent.content,
                  externalIds: udExternalIds,
                },
                ...(knobaIdContents.dKnobaIdContent && { dKnobaIdContent: knobaIdContents.dKnobaIdContent }),
              });
            }
          }
          return gudKnobaIdContents;
        }, { deletes: [], upserts: [] });
      assert(gudKnobaIdContents.deletes.length + gudKnobaIdContents.upserts.length
        == Object.keys(mudKnobaIdContents).length);
      assert(new Set(gudKnobaIdContents.deletes
        .concat(gudKnobaIdContents.upserts.map(({ uKnobaIdContent: { knobaId }}) => knobaId)))
        .size == Object.keys(mudKnobaIdContents).length)
      assert(gudKnobaIdContents.upserts.every(({ uKnobaIdContent: { externalIds } }) => externalIds.size > 0));
      const contentUpserts = gudKnobaIdContents.upserts
        .filter((udKnobaIdContent) => "dKnobaIdContent" in udKnobaIdContent);
      const emptyPromises: Promise<any>[] = [];
      await Promise.all(emptyPromises
        .concat(gudKnobaIdContents.upserts.length > 0
          ? [axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              vectors: gudKnobaIdContents.upserts.map(({ uKnobaIdContent }) => ({
                id: uKnobaIdContent.knobaId,
                values: uKnobaIdContent.embedding,
                metadata: {
                  content: uKnobaIdContent.content,
                  external_ids: uKnobaIdContent.externalIds,
                },
              })),
            },
          })] : [])
        .concat(gudKnobaIdContents.deletes.length > 0
          ? [axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: { ids: gudKnobaIdContents.deletes },
          })] : [])
        .concat(contentUpserts.length > 0
          ? [Promise.all(contentUpserts.map(async ({ uKnobaIdContent, dKnobaIdContent }) => await Promise.all(
            Array.from(uKnobaIdContent.externalIds)
              .map(async (externalId) => {
                if (externalId.startsWith("notion_")) {
                  await axios($, {
                    method: "PATCH",
                    url: `https://api.notion.com/v1/blocks/${externalId.substring(7)}`,
                    headers: {
                      Authorization: `Bearer ${$.notion.$auth.oauth_access_token}`,
                      "Notion-Version": `2022-06-28`,
                    },
                    data: {
                      paragraph: {
                        rich_text: [
                          {
                            text: {
                              content: uKnobaIdContent.content,
                            },
                          },
                        ],
                      },
                    },
                  });
                } else if (externalId.startsWith("gdocs_")) {
                  await axios($, {
                    method: "POST",
                    url: `https://docs.googleapis.com/v1/documents/${externalId.substring(6)}:batchUpdate`,
                    headers: {
                      Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
                    },
                    data: {
                      requests: [
                        {
                          replaceAllText: {
                            containsText: {
                              text: dKnobaIdContent?.content,
                              matchCase: true,
                            },
                            replaceText: uKnobaIdContent.content,
                          },
                        },
                      ],
                    },
                  });
                }
              })
          )))] : [])
      );
    };
