    export async function deleteIfExists(externalTypeQ: string, rawExternalId: string, $): Promise<void> {
      const qExternalId = `${externalTypeQ}_${rawExternalId}`;
      const mKnobaIds: string[] | undefined = await $.myDatastore.get(qExternalId);
      if (!mKnobaIds) {
        return;
      }
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
                  external_ids: externalIds,
                  content: fetchResp.vectors[knobaId].metadata.content,
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
    type ContentEmbedding = { content: string, embedding: number[] };
    async function getContentEmbeddings(externalTypeQ: string, rawExternalId: string, $): Promise<Array<ContentEmbedding>> {
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
          const content: string = notionGetResp.paragraph.rich_text[0].plain_text.trim();
          const embedding: number[] = (await axios($, {
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
          return [{ content, embedding }];
        case "gdocs":
          const gdocsGetResp = await axios($, {
            method: "GET",
            url: `https://docs.googleapis.com/v1/documents/${rawExternalId}`,
            headers: {
              Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
            },
          });
          console.log(gdocsGetResp);
          return await Promise.all(gdocsGetResp.body.content
            .filter((content) => content.paragraph)
            .map(({ paragraph }) =>
              paragraph.elements
                .map((element) => element.textRun.content)
                .filter((content) => content && content.trim())
                .join('').trim())
            .filter((content) => content && content.trim())
            .map(async (content: string) => {
              const embedding: number[] = (await axios($, {
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
              return { content, embedding };
            }));
          default:
            return [];
      }
    };
    type KnobaMatch = {
      knobaId: string,
      score: number,
      embedding: number[],
      content: string,
      externalIds: Set<string>,
    };
    type IngestedContentDataItem = { content: string, embedding: number[], maybeKnobaMatch: KnobaMatch | undefined };
    async function getIngestedContentData(externalTypeQ: string, rawExternalId: string, $): Promise<Array<IngestedContentDataItem>> {
      return await Promise.all((await getContentEmbeddings(externalTypeQ, rawExternalId, $)).map(async ({ content, embedding }) => {
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
          .map(({ id, values, metadata }) => ({
            knobaId: id,
            embedding: values,
            content: metadata.content,
            externalIds: new Set(JSON.parse(metadata.external_ids)),
          }));
        return {
          content,
          embedding,
          knobaMatch: wrMaybeMatch.length > 0 ? wrMaybeMatch[0] : undefined,
        };
      }));
    };
    async function upsertContent(vectorId: string, knobaIds: string[], inContentData: IngestedContentDataItem[], $): Promise<void> {
      const fetchedContent = (await axios($, {
        method: "GET",
        url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
        headers: {
          "Api-Key": `${$.pinecone.$auth.api_key}`,
        },
        data: {
          ids: knobaIds,
        },
      })).vectors;
      console.log(fetchedContent);
      // what if instead of changing content of knoba id,
      // should change doc to point to different existing knoba id
      // would you change all references of old knoba id to be new knoba id as well?
      await Promise.all([
        axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: {
            vectors: knobaIds.map((knobaId, index) => ({
              id: knobaId,
              values: inContentData[index].embedding,
              metadata: {
                content: inContentData[index].content,
                external_ids: fetchedContent[knobaId].metadata.external_ids,
              }
            })),
          },
        }),
        Promise.all(knobaIds.map(async (knobaId, index) => await Promise.all(
          JSON.parse(fetchedContent[knobaId].metadata.external_ids)
            .filter((externalId) => externalId != vectorId)
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
                            content: inContentData[index].content,
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
                            text: fetchedContent[knobaId].metadata.content,
                            matchCase: true,
                          },
                          replaceText: inContentData[index].content,
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
    async function knobaIngest(vectorId: string, inContentData: IngestedContentDataItem[], $): Promise<void> {
      const toUpsertContent = await Promise.all(inContentData.map(async ({ content, embedding, maybeKnobaMatch }) => {
        const [knobaId, externalIds] = maybeKnobaMatch
          ? [maybeKnobaMatch.knobaId, maybeKnobaMatch.externalIds.add(vectorId)]
          : [uuidv4(), new Set([vectorId])];
        return { knobaId, embedding, content, externalIds };
      }));
      await $.myDatastore.set(vectorId, JSON.stringify(toUpsertContent.map(({ knobaId }) => knobaId)));
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
              external_ids: JSON.stringify(Array.from(externalIds)),
              content: content,
            },
          })),
        },
      });
    };
    export async function handleUpsert(externalTypeQ: string, rawExternalId: string, $): Promise<void> {
      const qExternalId = `${externalTypeQ}_${rawExternalId}`;
      const mKnobaIds: string[] | undefined = await $.myDatastore.get(qExternalId);
      const inContentData = await getIngestedContentData(externalTypeQ, rawExternalId, $);
      if (mKnobaIds) {
        await upsertContent(qExternalId, mKnobaIds, inContentData, $);
      } else {
        await knobaIngest(qExternalId, inContentData, $);
      }
    };
