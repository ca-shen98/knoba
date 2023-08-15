import assert from 'node:assert';
import { v4 as uuidv4 } from 'uuid'
import { axios } from "@pipedream/platform"
var process;
    
    type KnobaBasics = { knobaId: string, content: string, externalIds: Set<string> };
    type KnobaMatch = { score: number, vals: KnobaBasics };
    type UDKnobaContent = { embedding: number[], content: string, knobaMatch: KnobaMatch };
    type MUDKnobaChange = { udKnobaContent: UDKnobaContent, mudCandDeltExtIds: { u: Set<string>, d: Set<string> } };

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
    async function handlReceive(uqExternalIdsBatch: string[], dqExternalIdsBatch: string[], $): Promise<void> {
      assert(uqExternalIdsBatch.length + dqExternalIdsBatch.length
        == new Set(uqExternalIdsBatch.concat(dqExternalIdsBatch)).size,
        "no duplicates (implies partitioned as well)");
      const contentsBatch = await getContentsBatch(uqExternalIdsBatch, $);
      const fContentsBatch = contentsBatch.flat();
      const fEmbeddingsBatch = fContentsBatch.length > 0
        ? (await axios($, {
          method: "POST",
          url: "https://api.openai.com/v1/embeddings",
          headers: {
            Authorization: `Bearer ${$.openai.$auth.api_key}`,
          },
          data: {
            model: "text-embedding-ada-002",
            input: fContentsBatch,
          },
        })).data : [];
      console.log(fEmbeddingsBatch);
      const udKnobaContentsBatch: UDKnobaContent[][] = [];
      assert(fEmbeddingsBatch.length == fContentsBatch.length);
      for (const { embedding } of fEmbeddingsBatch) {
        var lidx = udKnobaContentsBatch.length - 1;
        assert(udKnobaContentsBatch.length <= contentsBatch.length && (udKnobaContentsBatch.length == 0
          || udKnobaContentsBatch[lidx].length <= contentsBatch[lidx].length),
          "valid inductive prior state");
        assert(udKnobaContentsBatch.length < contentsBatch.length || (udKnobaContentsBatch.length > 0
          && udKnobaContentsBatch[lidx].length < contentsBatch[lidx].length),
          "can push next item");
        while (udKnobaContentsBatch.length == 0
          || udKnobaContentsBatch[lidx].length == contentsBatch[lidx].length) {
            udKnobaContentsBatch.push([]);
          lidx += 1;
        }
        const tQueriedMatch = (await axios($, {
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
        console.log(tQueriedMatch);
        const wrMaybeMatch = tQueriedMatch.filter((match) => Math.abs(match.score - 1) < 0.1).map((match) => ({
          score: match.score,
          vals: {
            knobaId: match.id,
            content: match.metadata.content,
            externalIds: new Set(JSON.parse(match.metadata.external_ids)),
          },
        }));
        const knobaMatch = wrMaybeMatch.length > 0 ? wrMaybeMatch[0]
          : {
            score: 1,
            vals: {
              knobaId: uuidv4(), // ignoring collision handling
              content: contentsBatch[lidx][udKnobaContentsBatch[lidx].length],
              externalIds: new Set([uqExternalIdsBatch[lidx]]),
            },
          };
        assert(knobaMatch.vals.content.trim().length > 0);
        assert(knobaMatch.vals.externalIds.size > 0);
        if (wrMaybeMatch.length == 0) {
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
                  content: contentsBatch[lidx][udKnobaContentsBatch[lidx].length],
                  knoba_id: knobaMatch.knobaId,
                  external_ids: JSON.stringify(Array.from(knobaMatch.externalIds)),
                },
              }],
            },
          });
        }
        udKnobaContentsBatch[lidx].push({
          embedding,
          content: contentsBatch[lidx][udKnobaContentsBatch[lidx].length],
          knobaMatch,
        });
      }
      assert(udKnobaContentsBatch.length == contentsBatch.length
        && udKnobaContentsBatch.every((udKnobaContents, index) =>
          udKnobaContents.length == contentsBatch[index].length));
      dqExternalIdsBatch.forEach(() => udKnobaContentsBatch.push([]));
      const tqExternalIdsBatch = uqExternalIdsBatch.concat(dqExternalIdsBatch)
      const mKnobaIdsBatch: Set<string>[] = await Promise.all(tqExternalIdsBatch.map(async (qExternalId) => {
        const mKnobaIdsStr = await $.myDatastore.get(qExternalId);
        return mKnobaIdsStr ? new Set(JSON.parse(mKnobaIdsStr)) : new Set();
      })); // need to get initial state before following mutate
      assert(udKnobaContentsBatch.length == tqExternalIdsBatch.length);
      await Promise.all(udKnobaContentsBatch.map(async (udKnobaContents, index) => {
        const onKnobaIds = udKnobaContents.map(({ knobaMatch: { vals : { knobaId } } }) => knobaId);
        if (onKnobaIds.length > 0) {
          await $.myDatastore.set(tqExternalIdsBatch[index], JSON.stringify(onKnobaIds));
        } else {
          await $.myDatastore.delete(tqExternalIdsBatch[index]);
        }
      }));
      const mudKnobaIdChanges: { [knobaId: string]: MUDKnobaChange } = udKnobaContentsBatch
        .reduce((mudKnobaIdChanges, udKnobaContents, index) => {
          udKnobaContents.forEach((udKnobaContent) => {
            if (!(udKnobaContent.knobaMatch.vals.knobaId in mudKnobaIdChanges)) {
              mudKnobaIdChanges[udKnobaContent.knobaMatch.vals.knobaId] = {
                udKnobaContent,
                mudCandDeltExtIds: { u: new Set(), d: new Set() },
              };
            }
            mudKnobaIdChanges[udKnobaContent.knobaMatch.vals.knobaId].mudCandDeltExtIds.u.add(tqExternalIdsBatch[index]);
          });
          return mudKnobaIdChanges;
        }, {}); // need to map knobaIds from udBatch before duBatch to be able to use udBatch as filter,
                // don't update inline with sqrBatch(wrMaybeMatch) because need complete externalId states
      const duKnobaIds = Array.from(new Set(mKnobaIdsBatch.map((mKnobaIds) => Array.from(mKnobaIds)).flat()))
        .filter((dKnobaId) => !(dKnobaId in mudKnobaIdChanges));
      const fdKnobaContent = duKnobaIds.length > 0
        ? (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: duKnobaIds },
        })).vectors : {};
      console.log(fdKnobaContent);
      const fmKnobaIdsSet = Array.from(new Set(mKnobaIdsBatch.map((mKnobaIds) => Array.from(mKnobaIds)).flat()
        .concat(udKnobaContentsBatch.flat().map(({ knobaMatch: { vals: { knobaId } } }) => knobaId))))
      assert(fmKnobaIdsSet.every((fmKnobaId) => fmKnobaId in mudKnobaIdChanges || fmKnobaId in fdKnobaContent));
      mKnobaIdsBatch.forEach((mKnobaIds, index) => mKnobaIds.forEach((mKnobaId) => {
        if (!(mKnobaId in mudKnobaIdChanges)) {
          assert(mKnobaId in fdKnobaContent);
          mudKnobaIdChanges[mKnobaId] = {
            udKnobaContent: {
              embedding: fdKnobaContent[mKnobaId].values,
              content: fdKnobaContent[mKnobaId].metadata.content,
              knobaMatch: {
                score: 1,
                vals: {
                  knobaId: mKnobaId,
                  content: fdKnobaContent[mKnobaId].metadata.content,
                  externalIds: new Set(JSON.parse(fdKnobaContent[mKnobaId].metadata.external_ids)),
                },
              },
            },
            mudCandDeltExtIds: { u: new Set(), d: new Set() },
          };
          assert(mudKnobaIdChanges[mKnobaId].udKnobaContent.knobaMatch.vals.externalIds.size > 0);
        }
        if (!mudKnobaIdChanges[mKnobaId].mudCandDeltExtIds.u.has(tqExternalIdsBatch[index])) {
          mudKnobaIdChanges[mKnobaId].mudCandDeltExtIds.d.add(tqExternalIdsBatch[index]);
        }
      }));
      assert(fmKnobaIdsSet.every((fmKnobaId) => fmKnobaId in mudKnobaIdChanges));
      // TODO asserts/invariants/assumptions, sequencing comments
      const dKnobaIds = new Set();
      const uKnobaContents: { [knobaId: string]: UDKnobaContent } = {};
      for (const mudKnobaId in mudKnobaIdChanges) {
        const { udKnobaContent, mudCandDeltExtIds } = mudKnobaIdChanges[mudKnobaId];
        assert(mudCandDeltExtIds.u.size + mudCandDeltExtIds.d.size > 0
          && mudCandDeltExtIds.u.size + mudCandDeltExtIds.d.size
            == new Set(Array.from(mudCandDeltExtIds.u).concat(Array.from(mudCandDeltExtIds.d))).size,
          "non empty and no duplicates (implies partitioned as well)");
        if (mudCandDeltExtIds.u.size == 0
          && udKnobaContent.knobaMatch.vals.externalIds.size == mudCandDeltExtIds.d.size) {
          assert(Array.from(mudCandDeltExtIds.d)
            .every((dCandDeltExtId) => udKnobaContent.knobaMatch.vals.externalIds.has(dCandDeltExtId)));
          dKnobaIds.add(mudKnobaId);
        } else {
          if (Array.from(mudCandDeltExtIds.u).concat(Array.from(mudCandDeltExtIds.d))
            .some((mudCandDeltExtId) => !udKnobaContent.knobaMatch.vals.externalIds.has(mudCandDeltExtId))) {
            mudCandDeltExtIds.u.forEach((uCandDeltExtIds) =>
              udKnobaContent.knobaMatch.vals.externalIds.add(uCandDeltExtIds));
            mudCandDeltExtIds.d.forEach((dCandDeltExtIds) =>
              udKnobaContent.knobaMatch.vals.externalIds.delete(dCandDeltExtIds));
            uKnobaContents[mudKnobaId] = udKnobaContent;
          } else if (Math.abs(udKnobaContent.knobaMatch.score - 1) > 0.01) {
            uKnobaContents[mudKnobaId] = udKnobaContent;
          }
        }
      }
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
      const stagedKnobaReplacements: { [knobaId: string]: KnobaMatch } = {};
      await Promise.all(Object.values(uKnobaContents)
        .filter(({ knobaMatch: { score }}) => Math.abs(score - 1) > 0.01)
        .map(async (udKnobaContent) => {
          var wrMaybeMatch: KnobaMatch[] = [];
          const fsKnobaIds = new Set();
          do {
            wrMaybeMatch.forEach((match) => fsKnobaIds.add(match.vals.knobaId));
            const tQueriedMatch = (await axios($, {
              method: "POST",
              url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/query`,
              headers: {
                "Api-Key": `${$.pinecone.$auth.api_key}`,
              },
              data: {
                topK: 1,
                id: udKnobaContent.knobaMatch.vals.knobaId,
                includeValues: false,
                includeMetadata: true,
                filter: { knoba_id: { "$nin": fsKnobaIds } },
              },
            })).matches;
            console.log(tQueriedMatch);
            wrMaybeMatch = tQueriedMatch.filter((match) => Math.abs(match.score - 1) < 0.1).map((match) => ({
              score: match.score,
              vals: {
                knobaId: match.id,
                content: match.metadata.content,
                externalIds: new Set(JSON.parse(match.metadata.external_ids)),
              },
            }));
            if (wrMaybeMatch.length > 0) {
              if (!(wrMaybeMatch[0].vals.knobaId in uKnobaContents)
                || !(wrMaybeMatch[0].vals.knobaId in stagedKnobaReplacements)) {
                assert(wrMaybeMatch[0].vals.externalIds.size > 0);
                assert(wrMaybeMatch[0].vals.content.trim().length > 0);
                const replacementCompletionChoices = (await axios($, {
                  method: "POST",
                  url: "https://api.openai.com/v1/completions",
                  headers: {
                    Authorization: `Bearer ${$.openai.$auth.api_key}`,
                  },
                  data: {
                    model: "text-davinci-003",
                    prompt: `Calculate the semantic diff between !<input_x> and !<input_y> and apply the same semantic transition to update !<input_z> as the !<output>.
                    !<input_x>: ${udKnobaContent.knobaMatch.vals.content}
                    !<input_y>: ${udKnobaContent.content}
                    !<input_z>: ${wrMaybeMatch[0].vals.content}
                    !<output>: `,
                  },
                })).choices;
                console.log(replacementCompletionChoices);
                stagedKnobaReplacements[wrMaybeMatch[0].vals.knobaId] = {
                  score: wrMaybeMatch[0].score,
                  vals: {
                    knobaId: wrMaybeMatch[0].vals.knobaId,
                    content: replacementCompletionChoices[0].text,
                    externalIds: wrMaybeMatch[0].vals.externalIds,
                  },
                };
              }
            }
          } while (wrMaybeMatch.length > 0);
        }));
      const stagedKnobaReplacementsBatch = Object.values(stagedKnobaReplacements);
      const replEmbeddingsBatch = stagedKnobaReplacementsBatch.length > 0
        ? (await axios($, {
          method: "POST",
          url: "https://api.openai.com/v1/embeddings",
          headers: {
            Authorization: `Bearer ${$.openai.$auth.api_key}`,
          },
          data: {
            model: "text-embedding-ada-002",
            input: stagedKnobaReplacementsBatch.map(({ vals: { content } }) => content),
          },
        })).data : [];
      console.log(replEmbeddingsBatch);
      assert(replEmbeddingsBatch.length == stagedKnobaReplacementsBatch.length);
      stagedKnobaReplacementsBatch.forEach((stagedKnobaReplacement, index) => {
        uKnobaContents[stagedKnobaReplacement.vals.knobaId] = {
          embedding: replEmbeddingsBatch[index].embedding,
          content: stagedKnobaReplacement.vals.content,
          knobaMatch: {
            score: stagedKnobaReplacement.score,
            vals: {
              knobaId: stagedKnobaReplacement.vals.knobaId,
              content: stagedKnobaReplacement.vals.content,
              externalIds: uKnobaContents[stagedKnobaReplacement.vals.knobaId]?.knobaMatch.vals.externalIds
                ?? stagedKnobaReplacement.vals.externalIds,
            },
          },
        }
      });
      assert(Object.values(uKnobaContents)
        .every(({ knobaMatch: { vals: { externalIds } } }) => externalIds.size > 0));
      await Promise.all(Object.values(uKnobaContents)
        .filter(({ knobaMatch: { score } }) => Math.abs(score - 1) > 0.01).map(async (uKnobaContent) =>
          await Promise.all(Array.from(uKnobaContent.knobaMatch.vals.externalIds).map(async (externalId) => {
            assert(Math.abs(uKnobaContent.knobaMatch.score - 1) > 0.01);
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
                          content: uKnobaContent.content,
                        },
                      },
                    ],
                  },
                },
              });
            } else if (externalId.startsWith("gdocs_")) {
              const mKnobaIdsStr = await $.myDatastore.get(externalId);
              assert(mKnobaIdsStr);
              const mKnobaIds = JSON.parse(mKnobaIdsStr);
              assert(mKnobaIds.length > 0);
              // TODO in memory request batch scoped content repository/cache beyond uKnobaContents
              const fKnobaIds = mKnobaIds.filter((mKnobaId) => !(mKnobaId in uKnobaContents));
              const fKnobaContents = fKnobaIds.length > 0
                ? (await axios($, {
                  method: "GET",
                  url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
                  headers: {
                    "Api-Key": `${$.pinecone.$auth.api_key}`,
                  },
                  data: { ids: fKnobaIds },
                })).vectors : {};
              assert(mKnobaIds.every((mKnobaId) => mKnobaId in uKnobaContents || mKnobaId in fKnobaContents));
              const materializedContent = mKnobaIds.map((mKnobaId) => {
                if (mKnobaId in uKnobaContents) {
                  return uKnobaContents[mKnobaId].content;
                } else {
                  return fKnobaContents[mKnobaId].metadata.content;
                }
              });
              const gdocsGetBodyResp = await axios($, {
                method: "GET",
                url: `https://docs.googleapis.com/v1/documents/${externalId.substring(6)}`,
                headers: {
                  Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
                },
              }).body;
              console.log(gdocsGetBodyResp);
              const endIndex = gdocsGetBodyResp.content.length > 0
                ? gdocsGetBodyResp.content[gdocsGetBodyResp.content.length - 1].endIndex - 1
                : 0;
              if (endIndex > 0) {
                await axios($, {
                  method: "POST",
                  url: `https://docs.googleapis.com/v1/documents/${externalId.substring(6)}:batchUpdate`,
                  headers: {
                    Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
                  },
                  data: {
                    requests: [
                      {
                        deleteContentRange: { range: { startIndex: 0, endIndex } },
                        insertText: {
                          text: materializedContent.join("\n"),
                          endOfSegmentLocation: {},
                        },
                      },
                    ],
                  },
                });
              }
            }
          })))
        .concat(Object.values(uKnobaContents).length > 0
          ? [axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/upsert`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              vectors: Object.values(uKnobaContents).map((uKnobaContent) => ({
                id: uKnobaContent.knobaMatch.vals.knobaId,
                values: uKnobaContent.embedding,
                metadata: {
                  content: uKnobaContent.content,
                  knoba_id: uKnobaContent.knobaMatch.vals.knobaId,
                  external_ids: JSON.stringify(Array.from(uKnobaContent.knobaMatch.vals.externalIds)),
                },
              })),
            },
          })] : [])
      );
    };
