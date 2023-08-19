import isEqual from 'lodash.isequal';
import assert from 'node:assert';
import { axios } from "@pipedream/platform";
export default defineComponent({
  props: {
    google_docs: {
      type: "app",
      app: "google_docs",
    },
    notion: {
      type: "app",
      app: "notion",
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
    async function exgressKnobaIdBlockChangesBatch(knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange }, $): Promise<void> {
      const notAlreadyFetchedKnobaIds = new Set();
      Object.values(knobaIdBlockChangesBatch).forEach(({ knobaBlockChange, externalIdsDelta }) =>
        new Set(Array.from(knobaBlockChange.knobaMatch.props.externalIds).concat(Array.from(externalIdsDelta.u)))
          .forEach((qExternalId) => {
            if (!(qExternalId in externalIdsDelta.d)) {
              const qSplitIdx = qExternalId.indexOf("_");
              const externalTypeQ = qExternalId.substring(0, qSplitIdx);
              
            }
          }))
    };
    const knobaIdBlockChangesBatch: { [knobaId: string]: StagedKnobaBlockChange } = {};
    await exgressKnobaIdBlockChangesBatch(knobaIdBlockChangesBatch, this);
  },
})