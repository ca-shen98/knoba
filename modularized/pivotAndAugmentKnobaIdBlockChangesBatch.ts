import isEqual from 'lodash.isequal';
import assert from 'node:assert';
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
    type StagedKnobaBlockChange = { knobaBlockChange: KnobaBlockChange, externalIdsDelta: { u: Set<string>, d: Set<string> } };
    async function pivotAndAugmentKnobaIdBlockChangesBatch(knobaBlockChangesBatch: { [qExternalId: string]: KnobaBlockChange[] }, $): Promise<{ [knobaId: string]: StagedKnobaBlockChange }> {
      const knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange } =
        Object.keys(knobaBlockChangesBatch).reduce((knobaIdBlockChangesBatch, qExternalId) => {
          knobaBlockChangesBatch[qExternalId].forEach((knobaBlockChange) => {
            const knobaId = knobaBlockChange.knobaMatch.props.knobaId;
            if (!(knobaId in knobaIdBlockChangesBatch)) {
              knobaIdBlockChangesBatch[knobaId] = {
                knobaBlockChange,
                externalIdsDelta: { u: new Set(), d: new Set() },
              };
            }
            knobaIdBlockChangesBatch[knobaId].externalIdsDelta.u.add(qExternalId);
          });
          return knobaIdBlockChangesBatch;
        }, {});
      const stagedAugmentedPropagateKnobaUpdates: { [knobaId: string]: StagedKnobaBlockChange } = {};
      await Promise.all(Object.values(knobaIdBlockChangesBatch).map(async ({ knobaBlockChange }) => {
        const filterSeenKnobaIds = new Set();
        var wrappedMaybeMatch: KnobaBlockMatch[] = [];
        do {
          wrappedMaybeMatch.forEach((match) => filterSeenKnobaIds.add(match.props.knobaId));
          const queryMatch = (await axios($, {
            method: "POST",
            url: `https://${process.env.pinecone_index}-${process.env.pinecone_project}.svc.${$.pinecone.$auth.environment}.pinecone.io/query`,
            headers: {
              "Api-Key": `${$.pinecone.$auth.api_key}`,
            },
            data: {
              topK: 1,
              id: knobaBlockChange.knobaMatch.props.knobaId,
              includeValues: false,
              includeMetadata: true,
              filter: { knoba_id: { "$nin": filterSeenKnobaIds } },
            },
          })).matches;
          console.log(queryMatch);
          wrappedMaybeMatch = queryMatch.filter((match) => Math.abs(match.score - 1) < 0.1).map((match) => ({
            score: match.score,
            props: {
              knobaId: match.id,
              content: match.metadata.content,
              externalIds: new Set(JSON.parse(match.metadata.external_ids)),
            },
          }));
          // TODO conflicts
          if (wrappedMaybeMatch.length > 0 && !(wrappedMaybeMatch[0].props.knobaId in knobaIdBlockChangesBatch)
            && !(wrappedMaybeMatch[0].props.knobaId in stagedAugmentedPropagateKnobaUpdates)) {
            assert(wrappedMaybeMatch[0].props.content.trim());
            assert(wrappedMaybeMatch[0].props.externalIds.size > 0);
            stagedAugmentedPropagateKnobaUpdates[wrappedMaybeMatch[0].props.knobaId] = {
              knobaBlockChange: {
                newContentForMatchOrTrigger: knobaBlockChange.newContentForMatchOrTrigger,
                newContentEmbeddingOrTriggerOldContent: knobaBlockChange.knobaMatch.props.content,
                knobaMatch: wrappedMaybeMatch[0],
              },
              externalIdsDelta: { u: new Set(), d: new Set() },
            };
          }
        } while (wrappedMaybeMatch.length > 0);
      }));
      Object.values(stagedAugmentedPropagateKnobaUpdates).forEach((stagedKnobaBlockChange) => {
        // TODO asserts
        const knobaId = stagedKnobaBlockChange.knobaBlockChange.knobaMatch.props.knobaId;
        assert(!(knobaId in knobaIdBlockChangesBatch));
        knobaIdBlockChangesBatch[knobaId] = stagedKnobaBlockChange;
      });
      return knobaIdBlockChangesBatch;
    };
    const knobaBlockChangesBatch: { [qExternalId: string]: KnobaBlockChange[] } = {};
    const knobaIdBlockChangesBatch = await pivotAndAugmentKnobaIdBlockChangesBatch(knobaBlockChangesBatch, this);
    Object.keys(knobaBlockChangesBatch).forEach((qExternalId) => knobaBlockChangesBatch[qExternalId]
      .forEach(({ knobaMatch: { props: { knobaId } } }) => {
        assert(knobaId in knobaIdBlockChangesBatch);
        assert(knobaIdBlockChangesBatch[knobaId].externalIdsDelta.u.has(qExternalId));
      }));
    Object.values(knobaIdBlockChangesBatch).forEach(({ externalIdsDelta }) => {
      assert(externalIdsDelta.d.size == 0);
      externalIdsDelta.u.forEach((qExternalId) => assert(qExternalId in knobaBlockChangesBatch));
    });
    return knobaIdBlockChangesBatch;
  },
})