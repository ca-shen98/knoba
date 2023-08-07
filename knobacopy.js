import { v4 as uuidv4 } from 'uuid'
import { axios } from "@pipedream/platform"
var process;

    async function deleteIfExists(externalTypeQ, rawExternalId, $) {
      const qExternalId = `${externalTypeQ}_${rawExternalId}`;
      const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
      if (!mKnobaIdsStr) {
        return;
      }
      const mKnobaIds = JSON.parse(mKnobaIdsStr);
      await $.myDatastore.delete(qExternalId);
      await Promise.all(mKnobaIds?.map(async (knobaId) => {
        const fetchResp = await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch?ids=${knobaId}`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
        });
        console.log(fetchResp);
        const externalIds = JSON.parse(fetchResp.vectors[knobaId].metadata.external_ids)
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
            data: {
              ids: [knobaId],
            },
          });
        }
      }));
    };
    async function getContentEmbeddings(externalTypeQ, rawExternalId, $) {
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
    async function getIngestedContentData(externalTypeQ, rawExternalId, $) {
      const contentEmbeddings = await getContentEmbeddings(externalTypeQ, rawExternalId, $);
      return await Promise.all(contentEmbeddings.map(async ({ embedding, content }) => {
        const matchResp = await axios($, {
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
        });
        console.log(matchResp);
        const wrMaybeMatch = matchResp.matches.filter((match) => Math.abs(match.score - 1) < 0.05)
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
    async function upsertContent(qExternalId, mKnobaIds, inContentData, $) {
      const fetchedContent = (await axios($, {
        method: "GET",
        url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
        headers: {
          "Api-Key": `${$.pinecone.$auth.api_key}`,
        },
        data: {
          ids: mKnobaIds,
        },
      })).vectors;
      console.log(fetchedContent);
      const udKnobaIdContents = mKnobaIds.map((mKnobaId, index) => {
        const maybeKnobaMatch = inContentData[index].maybeKnobaMatch;
        const fetchedExternalIds = new Set(JSON.parse(fetchedContent[mKnobaId].metadata.external_ids));
        if (maybeKnobaMatch && Math.abs(maybeKnobaMatch.score - 1) < 0.05) {
          fetchedExternalIds.delete(qExternalId);
          return {
            isContentUpsert: false,
            uKnobaIdContent: {
              knobaId: maybeKnobaMatch.match.knobaId,
              embedding: maybeKnobaMatch.match.embedding,
              content: maybeKnobaMatch.match.content,
              externalIds: maybeKnobaMatch.match.externalIds.add(qExternalId),
            },
            dKnobaIdContent: {
              knobaId: fetchedContent[mKnobaId].id,
              embedding: fetchedContent[mKnobaId].values,
              content: fetchedContent[mKnobaId].metadata.content,
              externalIds: fetchedExternalIds,
            },
          };
        } else {
          return {
            isContentUpsert: true,
            uKnobaIdContent: {
              knobaId: fetchedContent[mKnobaId].id,
              embedding: inContentData[index].embedding,
              content: inContentData[index].content,
              externalIds: fetchedExternalIds,
            },
            dKnobaIdContent: {
              knobaId: fetchedContent[mKnobaId].id,
              embedding: fetchedContent[mKnobaId].values,
              content: fetchedContent[mKnobaId].metadata.content,
              externalIds: fetchedExternalIds,
            },
          };
        }
      });
      await $.myDatastore.set(qExternalId, JSON.stringify(udKnobaIdContents.map(({ uKnobaIdContent: { knobaId } }) => knobaId)));
      const gUDKnobaIdContents = udKnobaIdContents.reduce(
        (gUDKnobaIdContents, udKnobaIdContent) => {
          if (udKnobaIdContent.isContentUpsert) {
            gUDKnobaIdContents.isContentUpsert.push(udKnobaIdContent);
          } else if (udKnobaIdContent.dKnobaIdContent.externalIds.size > 0) {
            gUDKnobaIdContents.notContentUpsert.withDUpsert.push(udKnobaIdContent);
          } else {
            gUDKnobaIdContents.notContentUpsert.withDelete.push(udKnobaIdContent);
          }
          return gUDKnobaIdContents;
        },
        { isContentUpsert: [], notContentUpsert: { withDelete: [], withDUpsert: [] } }
      );
      if (gUDKnobaIdContents.notContentUpsert.withDelete.length > 0) {
        await axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: {
            ids: gUDKnobaIdContents.notContentUpsert.withDelete.map(({ dKnobaIdContent: { knobaId } }) => knobaId),
          },
        });
      }
      const emptyUpsertVectors = [];
      await Promise.all([
        axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: {
            vectors: emptyUpsertVectors.concat(gUDKnobaIdContents.isContentUpsert)
              .concat(gUDKnobaIdContents.notContentUpsert.withDelete)
              .concat(gUDKnobaIdContents.notContentUpsert.withDUpsert)
              .concat(gUDKnobaIdContents.notContentUpsert.withDUpsert
                .map(({ dKnobaIdContent }) => ({ uKnobaIdContent: dKnobaIdContent })))
              .map(({ uKnobaIdContent }) => ({
                id: uKnobaIdContent.knobaId,
                values: uKnobaIdContent.embedding,
                metadata: {
                  content: uKnobaIdContent.content,
                  external_ids: Array.from(uKnobaIdContent.externalIds),
                },
              })),
          },
        }),
        Promise.all(gUDKnobaIdContents.isContentUpsert.map(async ({ uKnobaIdContent, dKnobaIdContent }) => await Promise.all(
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
                            text: dKnobaIdContent.content,
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
        ))),
      ]);
    };
    async function knobaIngest(qExternalId, inContentData, $) {
      const toUpsertContent = await Promise.all(inContentData.map(async ({ content, embedding, maybeKnobaMatch }) => {
        const [knobaId, externalIds] = maybeKnobaMatch
          ? [maybeKnobaMatch.knobaId, maybeKnobaMatch.externalIds.add(qExternalId)]
          : [uuidv4(), new Set([qExternalId])];
        return { knobaId, embedding, content, externalIds };
      }));
      await $.myDatastore.set(qExternalId, JSON.stringify(toUpsertContent.map(({ knobaId }) => knobaId)));
      await axios($, {
        method: "POST",
        url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
        headers: {
          "Api-Key": `${$.pinecone.$auth.api_key}`,
        },
        data: {
          vectors: toUpsertContent.map(({ knobaId, embedding, content, externalIds }) => ({
            id: knobaId,
            values: embedding,
            metadata: {
              content: content,
              external_ids: Array.from(externalIds),
            },
          })),
        },
      });
    };
    async function handleUpsert(externalTypeQ, rawExternalId, $) {
      const qExternalId = `${externalTypeQ}_${rawExternalId}`;
      const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
      const mKnobaIds = mKnobaIdsStr ? JSON.parse(mKnobaIdsStr) : mKnobaIdsStr;
      const inContentData = await getIngestedContentData(externalTypeQ, rawExternalId, $);
      if (mKnobaIds) {
        await upsertContent(qExternalId, mKnobaIds, inContentData, $);
      } else {
        await knobaIngest(qExternalId, inContentData, $);
      }
    };
