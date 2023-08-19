import isEqual from 'lodash.isequal';
import assert from 'node:assert';
import { axios } from "@pipedream/platform";
export default defineComponent({
  props: {
    openai: {
      type: "app",
      app: "openai",
    },
  },
  async run({ steps, $ }) {
    var process;
    type KnobaBlockProps = { knobaId: string, content: string, externalIds: Set<string> };
    type KnobaBlockMatch = { score: number, props: KnobaBlockProps };
    type KnobaBlockChange = { newContentForMatchOrTrigger: string, newContentEmbeddingOrTriggerOldContent: number[] | string, knobaMatch: KnobaBlockMatch };
    type StagedKnobaBlockChange = { knobaBlockChange: KnobaBlockChange, externalIdsDelta: { u: Set<string>, d: Set<string> } };
    async function getGenerateAugmentKnobaIdBlockChangesBatch(knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange }, $): Promise<{ [knobaId: string]: StagedKnobaBlockChange }> {
      // indexed input map entries to be able to remap (zip/merge) back flattened update completions and embeddings
      const indexedStagedAugmentedKnobaUpdates = Object.values(knobaIdBlockChangesBatch)
        .filter(({ knobaBlockChange }) => typeof(knobaBlockChange.newContentEmbeddingOrTriggerOldContent)
          == typeof(knobaBlockChange.newContentForMatchOrTrigger));
      const indexedNewContentUpdateCompletions = await Promise.all(indexedStagedAugmentedKnobaUpdates
        .map(async ({ knobaBlockChange }) => {
          const updateCompletionChoices = (await axios($, {
            method: "POST",
            url: "https://api.openai.com/v1/completions",
            headers: {
                Authorization: `Bearer ${$.openai.$auth.api_key}`,
            },
            data: {
                model: "text-davinci-003",
                prompt: `Calculate the semantic diff between !<input_x> and !<input_y> and apply the same semantic transition to update !<input_z> as the !<output>.
                !<input_x>: ${knobaBlockChange.newContentEmbeddingOrTriggerOldContent}
                !<input_y>: ${knobaBlockChange.newContentForMatchOrTrigger}
                !<input_z>: ${knobaBlockChange.knobaMatch.props.content}
                !<output>: `,
            },
          })).choices;
          console.log(updateCompletionChoices)
          return updateCompletionChoices[0].text;
        }));
      const indexedNewContentEmbeddings = indexedNewContentUpdateCompletions.length > 0
        ? (await axios($, {
          method: "POST",
          url: "https://api.openai.com/v1/embeddings",
          headers: {
            Authorization: `Bearer ${$.openai.$auth.api_key}`,
          },
          data: {
            model: "text-embedding-ada-002",
            input: indexedNewContentUpdateCompletions,
          },
        })).data : [];
      console.log(indexedNewContentEmbeddings);
      assert(indexedNewContentEmbeddings.length == indexedStagedAugmentedKnobaUpdates.length);
      indexedStagedAugmentedKnobaUpdates.forEach((stagedAugmentedKnobaUpdate, index) => {
        const knobaId = stagedAugmentedKnobaUpdate.knobaBlockChange.knobaMatch.props.knobaId;
        stagedAugmentedKnobaUpdate.knobaBlockChange.newContentForMatchOrTrigger =
          indexedNewContentUpdateCompletions[index];
        stagedAugmentedKnobaUpdate.knobaBlockChange.newContentEmbeddingOrTriggerOldContent =
          indexedNewContentEmbeddings[index].embedding;
        // TODO asserts
        knobaIdBlockChangesBatch[knobaId] = stagedAugmentedKnobaUpdate;
      });
      return knobaIdBlockChangesBatch;
    };
    const knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange } = {};
    await getGenerateAugmentKnobaIdBlockChangesBatch(knobaIdBlockChangesBatch, this);
    return knobaIdBlockChangesBatch;
  },
})