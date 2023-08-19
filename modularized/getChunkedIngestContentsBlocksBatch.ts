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
  },
  async run({ steps, $ }) {
    async function getChunkedIngestContentsBlocksBatch(qExternalIdsBatch: Set<string>, $): Promise<{ [qExternalId: string]: string[] }> {
      // group by type first to map fetches to perform subsequently in case some type allows for batch retrieval
      const tgRawExternalIdsBatch: { [externalTypeQ: string]: string[] } = {}; // tg for type-grouped
      qExternalIdsBatch.forEach((qExternalId) => {
        const qSplitIdx = qExternalId.indexOf("_");
        const externalTypeQ = qExternalId.substring(0, qSplitIdx);
        const rawExternalId = qExternalId.substring(qSplitIdx + 1);
        if (!(externalTypeQ in tgRawExternalIdsBatch)) {
          tgRawExternalIdsBatch[externalTypeQ] = [];
        }
        tgRawExternalIdsBatch[externalTypeQ].push(rawExternalId);
      });
      const qExternalIdIngestContentsBlocksBatch: { [qExternalId: string]: string[] } = {};
      await Promise.all(Object.keys(tgRawExternalIdsBatch).map(async (externalTypeQ) => {
        switch (externalTypeQ) {
          case "notion":
            await Promise.all(tgRawExternalIdsBatch[externalTypeQ].map(async (rawExternalId) => {
              const notionGetResp = await axios($, {
                url: `https://api.notion.com/v1/blocks/${rawExternalId}`,
                headers: {
                  Authorization: `Bearer ${$.notion.$auth.oauth_access_token}`,
                  "Notion-Version": `2022-06-28`,
                },
              });
              console.log(notionGetResp);
              const qExternalId = `${externalTypeQ}_${rawExternalId}`;
              qExternalIdIngestContentsBlocksBatch[qExternalId] = [
                notionGetResp.paragraph.rich_text[0].plain_text.trim()
              ];
            }));
            break;
          case "gdocs":
            await Promise.all(tgRawExternalIdsBatch[externalTypeQ].map(async (rawExternalId) => {
              const gdocsGetResp = await axios($, {
                method: "GET",
                url: `https://docs.googleapis.com/v1/documents/${rawExternalId}`,
                headers: {
                  Authorization: `Bearer ${$.google_docs.$auth.oauth_access_token}`,
                },
              });
              console.log(gdocsGetResp);
              const qExternalId = `${externalTypeQ}_${rawExternalId}`;
              qExternalIdIngestContentsBlocksBatch[qExternalId] = gdocsGetResp.body.content
                .map(({ paragraph }) => paragraph?.elements.map((element) => element.textRun.content)
                  .filter((content) => content && content.trim()).join('').trim())
                .filter((content) => content && content.trim());
            }));
            break;
          default:
            break;
        }
      }));
      assert(Object.keys(qExternalIdIngestContentsBlocksBatch).length == qExternalIdsBatch.size);
      assert(Object.values(qExternalIdIngestContentsBlocksBatch).every((qExternalIdIngestContentsBlocks) =>
        qExternalIdIngestContentsBlocks.every((block) => block.trim())))
      return qExternalIdIngestContentsBlocksBatch;
    };
    const qExternalIdsBatch: Set<string> = new Set(["notion_284d10157fbf43cca485bf30d908add3"]);
    const qExternalIdIngestContentsBlocksBatch = await getChunkedIngestContentsBlocksBatch(qExternalIdsBatch, this);
    assert(isEqual(qExternalIdIngestContentsBlocksBatch, {
      "notion_284d10157fbf43cca485bf30d908add3": ["Sample paragraph"],
    }));
    return qExternalIdIngestContentsBlocksBatch;
  },
});
