import isEqual from 'lodash.isequal';
import assert from 'node:assert';
import { axios } from "@pipedream/platform";
export default defineComponent({
  props: {
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
    type StagedKnobaBlockChange = { knobaBlockChange: KnobaBlockChange, externalIdsDelta: { u: Set<string>, d: Set<string> } };
    async function applyDropKnobaIdsBatch(knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange }, oldReferencedKnobaIdsBatch: { [qExternalId: string]: Set<string> }, $): Promise<{ [knobaId: string]: StagedKnobaBlockChange }> {
      const notAlreadyFetchedKnobaIds = new Set();
      Object.values(oldReferencedKnobaIdsBatch).forEach((oldReferencedKnobaIds) => oldReferencedKnobaIds
        .forEach((knobaId) => {
          if (!(knobaId in knobaIdBlockChangesBatch)) {
            notAlreadyFetchedKnobaIds.add(knobaId);
          }
        }))
      const notAlreadyFetchedKnobaBlocks = notAlreadyFetchedKnobaIds.size > 0
        ? (await axios($, {
          method: "GET",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/fetch`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: notAlreadyFetchedKnobaIds },
        })).vectors : {};
      console.log(notAlreadyFetchedKnobaBlocks);
      const orphanedKnobaIds = new Set();
      Object.keys(oldReferencedKnobaIdsBatch).forEach((qExternalId) => oldReferencedKnobaIdsBatch[qExternalId]
        .forEach((knobaId) => {
          if (!orphanedKnobaIds.has(knobaId)) {
            if (!(knobaId in knobaIdBlockChangesBatch)) {
              assert(knobaId in notAlreadyFetchedKnobaBlocks);
              assert(notAlreadyFetchedKnobaBlocks[knobaId].metadata.content.trim());
              knobaIdBlockChangesBatch[knobaId] = {
                knobaBlockChange: {
                  newContentForMatchOrTrigger: notAlreadyFetchedKnobaBlocks[knobaId].metadata.content,
                  newContentEmbeddingOrTriggerOldContent: notAlreadyFetchedKnobaBlocks[knobaId].values,
                  knobaMatch: {
                    score: 1,
                    props: {
                      knobaId: knobaId,
                      content: notAlreadyFetchedKnobaBlocks[knobaId].metadata.content,
                      externalIds: new Set(JSON.parse(notAlreadyFetchedKnobaBlocks[knobaId].metadata.external_ids)),
                    },
                  },
                },
                externalIdsDelta: { u: new Set(), d: new Set() },
              };
              assert(knobaIdBlockChangesBatch[knobaId].knobaBlockChange.knobaMatch.props.externalIds.size > 0);
            }
            if (!knobaIdBlockChangesBatch[knobaId].externalIdsDelta.u.has(qExternalId)) {
              assert(knobaIdBlockChangesBatch[knobaId].externalIdsDelta.d.size
                < knobaIdBlockChangesBatch[knobaId].knobaBlockChange.knobaMatch.props.externalIds.size)
              assert(knobaIdBlockChangesBatch[knobaId].knobaBlockChange.knobaMatch.props.externalIds.has(qExternalId));
              knobaIdBlockChangesBatch[knobaId].externalIdsDelta.d.add(qExternalId);
              if (knobaIdBlockChangesBatch[knobaId].externalIdsDelta.d.size ==
                knobaIdBlockChangesBatch[knobaId].knobaBlockChange.knobaMatch.props.externalIds.size) {
                orphanedKnobaIds.add(knobaId);
                delete knobaIdBlockChangesBatch[knobaId];
              }
            }
          }
        }));
      if (orphanedKnobaIds.size > 0) {
        await axios($, {
          method: "POST",
          url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/vectors/delete`,
          headers: {
            "Api-Key": `${$.pinecone.$auth.api_key}`,
          },
          data: { ids: orphanedKnobaIds },
        });
      }
      return knobaIdBlockChangesBatch;
    };
    const knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange } = {};
    const oldReferencedKnobaIdsBatch = {};
    await applyDropKnobaIdsBatch(knobaIdBlockChangesBatch, oldReferencedKnobaIdsBatch, this);
    return knobaIdBlockChangesBatch;
  },
})