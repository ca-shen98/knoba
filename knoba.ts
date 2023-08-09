import { v4 as uuidv4 } from 'uuid'
import { axios } from "@pipedream/platform"
var process;
    
    export async function deleteIfExists(qExternalId: string, $): Promise<void> {
      const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
      if (!mKnobaIdsStr) {
        return;
      }
      const mKnobaIds = new Set(JSON.parse(mKnobaIdsStr));
      await $.myDatastore.delete(qExternalId);
      await Promise.all(Array.from(mKnobaIds).map(async (knobaId) => {
        const fetchResp = await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch?ids=${knobaId}`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
        });
        console.log(fetchResp);
        const externalIds: string[] = JSON.parse(fetchResp.vectors[knobaId].metadata.external_ids)
          .filter((externalId) => externalId != qExternalId);
        if (externalIds.length > 0) {
          await axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              vectors: [{
                id: knobaId,
                values: fetchResp.vectors[knobaId].values,
                metadata: {
                  content: fetchResp.vectors[knobaId].metadata.content,
                  external_ids: externalIds,
                },
              }],
            },
          });
        } else {
          await axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: { ids: [knobaId] },
          });
        }
      }));
    };
    type ContentEmbedding = { embedding: number[], content: string };
    async function getContentEmbeddings(qExternalId: string, $): Promise<Array<ContentEmbedding>> {
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
          const content = notionGetResp.paragraph.rich_text[0].plain_text.trim();
          const embedding = (await axios($, {
            method: "POST",
            url: `https://api.openai.com/v1/embeddings`,
            headers: {
              Authorization: `Bearer ${$.openai.$auth.api_key}`,
            },
            data: {
              model: "text-embedding-ada-002",
              input: content,
            },
          })).data[0].embedding;
          return [{ embedding, content }];
        case "gdocs":
          const gdocsGetResp = await axios($, {
            method: "GET",
            url: `https://docs.googleapis.com/v1/documents/${rawExternalId}`,
            headers: {
              Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
            },
          });
          console.log(gdocsGetResp);
          const gdocsContents = gdocsGetResp.body.content
            .filter((content) => content.paragraph)
            .map(({ paragraph }) =>
              paragraph.elements
                .map((element) => element.textRun.content)
                .filter((content) => content && content.trim())
                .join('').trim())
            .filter((content) => content && content.trim());
          const gdocsEmbeddings = (await axios($, {
            method: "POST",
            url: `https://api.openai.com/v1/embeddings`,
            headers: {
              Authorization: `Bearer ${$.openai.$auth.api_key}`,
            },
            data: {
              model: "text-embedding-ada-002",
              input: gdocsContents,
            },
          })).data;
          console.log(gdocsEmbeddings);
          return gdocsEmbeddings.map(({ embedding }, index) => ({ embedding, content: gdocsContents[index] }));
          default:
            return [];
      }
    };
    type KnobaIdContent = {
      knobaId: string,
      embedding: number[],
      content: string,
      externalIds: Set<string>,
    };
    type KnobaMatch = { score: number, match: KnobaIdContent };
    type IngestedContentDataItem = { content: string, embedding: number[], maybeKnobaMatch?: KnobaMatch };
    async function getIngestedContentData(qExternalId: string, $): Promise<Array<IngestedContentDataItem>> {
      const contentEmbeddings = await getContentEmbeddings(qExternalId, $);
      return await Promise.all(contentEmbeddings.map(async ({ embedding, content }) => {
        const queriedMatches = await axios($, {
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
        console.log(queriedMatches);
        const wrMaybeMatch = queriedMatches.filter((match) => Math.abs(match.score - 1) < 0.25)
          .map(({ score, id, values, metadata }) => ({
            score,
            match: {
              knobaId: id,
              embedding: values,
              content: metadata.content,
              externalIds: new Set(JSON.parse(metadata.external_ids)),
            },
          }));
        return {
          content,
          embedding,
          maybeKnobaMatch: wrMaybeMatch.length > 0 ? wrMaybeMatch[0] : undefined,
        };
      }));
    };
    async function upsertContent(qExternalId: string, mKnobaIds: Set<string>, inContentData: IngestedContentDataItem[], $): Promise<void> {
      const udKnobaIdContents = inContentData.map(({ content, embedding, maybeKnobaMatch }) => {
        if (maybeKnobaMatch && Math.abs(maybeKnobaMatch.score - 1) < 0.1) {
          return {
            uKnobaIdContent: {
              knobaId: maybeKnobaMatch.match.knobaId,
              embedding,
              content,
              externalIds: new Set(maybeKnobaMatch.match.externalIds).add(qExternalId),
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
              externalIds: new Set(qExternalId),
            },
          };
        }
      });
      const onKnobaIds = udKnobaIdContents.map(({ uKnobaIdContent: { knobaId } }) => knobaId);
      await $.myDatastore.set(qExternalId, JSON.stringify(onKnobaIds));
      const snKnobaIds = new Set(onKnobaIds);
      const dKnobaIds = Array.from(mKnobaIds).filter((mKnobaId) => snKnobaIds.has(mKnobaId))
      type KnobaVector = { id: string, values: number[], metadata: { content: string, external_ids: string } };
      const fetchedContent: KnobaVector[] = (await axios($, {
        method: "GET",
        url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
        headers: {
          "Api-Key": `${$.pinecone.$auth.api_key}`,
        },
        data: { ids: dKnobaIds },
      })).vectors;
      console.log(fetchedContent);
      type UDKnobaIdContent = { knobaId: string, embedding: number[], content: string, externalIds: Set<string> };
      type GDKnobaIds = { dKnobaIdDeletes: string[], dKnobaIdUpserts: { uKnobaIdContent: UDKnobaIdContent }[] };
      const gdKnobaIds = Object.values(fetchedContent).reduce((gdKnobaIds: GDKnobaIds, fetchedContentItem) => {
        const externalIds: Set<string> = new Set(JSON.parse(fetchedContentItem.metadata.external_ids));
        if (externalIds.size == 1 && externalIds.has(qExternalId)) {
          gdKnobaIds.dKnobaIdDeletes.push(fetchedContentItem.id);
        } else {
          externalIds.delete(qExternalId);
          gdKnobaIds.dKnobaIdUpserts.push({
            uKnobaIdContent: {
              knobaId: fetchedContentItem.id,
              embedding: fetchedContentItem.values,
              content: fetchedContentItem.metadata.content,
              externalIds,
            },
          });
        }
        return gdKnobaIds;
      }, { dKnobaIdDeletes: [], dKnobaIdUpserts: [] });
      type MUDKnobaIdContents = { [knobaId: string]: { uKnobaIdContent: UDKnobaIdContent, dKnobaIdContent?: UDKnobaIdContent } };
      const mudKnobaIdContents = udKnobaIdContents.reduce((mudKnobaIdContents: MUDKnobaIdContents, udKnobaIdContent) => {
        if (!(udKnobaIdContent.uKnobaIdContent.knobaId in mudKnobaIdContents)
          || !("dKnobaIdContent" in mudKnobaIdContents[udKnobaIdContent.uKnobaIdContent.knobaId])
          || "dKnobaIdContent" in udKnobaIdContent) {
          mudKnobaIdContents[udKnobaIdContent.uKnobaIdContent.knobaId] = udKnobaIdContent;
        }
        return mudKnobaIdContents;
      }, {});
      const knobaUpserts = [ ...Object.values(mudKnobaIdContents), ...gdKnobaIds.dKnobaIdUpserts ];
      const contentUpserts = Object.values(mudKnobaIdContents).filter((udKnobaIdContent) => "dKnobaIdContent" in udKnobaIdContent);
      await Promise.all(
        [axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: {
            vectors: knobaUpserts.map(({ uKnobaIdContent }) => ({
              id: uKnobaIdContent.knobaId,
              values: uKnobaIdContent.embedding,
              metadata: {
                content: uKnobaIdContent.content,
                external_ids: uKnobaIdContent.externalIds,
              },
            })),
          },
        })]
          .concat(gdKnobaIds.dKnobaIdDeletes.length > 0
            ? [axios($, {
              method: "POST",
              url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
              headers: {
                "Api-Key": `${$.pinecone.$auth.api_key}`,
              },
              data: { ids: gdKnobaIds.dKnobaIdDeletes },
            })] : [])
          .concat(contentUpserts.length > 0
            ? [Promise.all(contentUpserts.map(async ({ uKnobaIdContent, dKnobaIdContent }) => await Promise.all(
              Array.from(uKnobaIdContent.externalIds)
                .filter((externalId) => externalId != qExternalId)
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
    export async function handleUpsert(qExternalId: string, $): Promise<void> {
      const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
      const mKnobaIds: Set<string> = mKnobaIdsStr ? new Set(JSON.parse(mKnobaIdsStr)) : new Set();
      const inContentData = await getIngestedContentData(qExternalId, $);
      await upsertContent(qExternalId, mKnobaIds, inContentData, $);
    };
