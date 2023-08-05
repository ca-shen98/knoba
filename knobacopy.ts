    async function deleteIfExists(externalType, rawExternalId, $) {
      const vectorId = `${externalType}_${rawExternalId}`;
      const knobaIds = await $.myDatastore.get(vectorId);
      if (!knobaIds) {
        return;
      }
      await $.myDatastore.delete(vectorId);
      await Promise.all(knobaIds?.map(async (knobaId) => {
        const fetch = (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch?ids=${knobaId}`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
        })).vectors[knobaId];
        console.log(fetch);
        const externalIds = JSON.parse(fetch.metadata.external_ids)
          .filter((externalId) => externalId != vectorId);
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
                values: fetch.values,
                metadata: {
                  external_ids: externalIds,
                  content: fetch.metadata.content,
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
    async function getContentEmbeddings(externalType, rawExternalId, $) {
      switch (externalType) {
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
            .map(async (content) => {
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
              return { content, embedding };
            }));
          default:
            return [];
      }
    };
    async function upsertContent(vectorId, knobaIds, contentEmbeddings, $) {
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
              values: contentEmbeddings[index].embedding,
              metadata: {
                content: contentEmbeddings[index].content,
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
                            content: contentEmbeddings[index].content,
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
                          replaceText: contentEmbeddings[index].content,
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
    async function knobaIngest(vectorId, contentEmbeddings, $) {
      const toUpsertContent = await Promise.all(contentEmbeddings.map(async ({ content, embedding }) => {
        const matches = await axios($, {
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
        console.log(matches);
        const [knobaId, externalIds] = matches.matches.filter((match) => Math.abs(match.score - 1) < 0.05)
          .reduce(
            (prev, curr) => [curr.id, JSON.stringify(Array.from(new Set(JSON.parse(curr.metadata.external_ids)).add(vectorId)))],
            [uuidv4(), JSON.stringify([vectorId])]
          );
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
              external_ids: externalIds,
              content: content,
            },
          })),
        },
      });
    };
    async function handleUpsert(externalType, rawExternalId, $) {
      const vectorId = `${externalType}_${rawExternalId}`;
      const knobaIds = await $.myDatastore.get(vectorId);
      const contentEmbeddings = await getContentEmbeddings(externalType, rawExternalId, $);
      if (knobaIds) {
        await upsertContent(vectorId, knobaIds, contentEmbeddings, $);
      } else {
        await knobaIngest(vectorId, contentEmbeddings, $);
      }
    };
    