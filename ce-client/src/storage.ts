class StorageMatch {
  location: string;
  content: string;
  embedding: string;
  constructor(location: string, content: string, embedding: string) {
    this.location = location;
    this.content = content;
    this.embedding = embedding;
  };
};
class StorageUpdate {
  location: string;
  oldContent: string;
  newContent: string;
  oldContentEmbedding: string;
  newContentEmbedding: string;
  constructor(location: string, oldContent: string, newContent: string, oldContentEmbedding: string, newContentEmbedding: string) {
    this.location = location;
    this.oldContent = oldContent;
    this.newContent = newContent;
    this.oldContentEmbedding = oldContentEmbedding;
    this.newContentEmbedding = newContentEmbedding;
  };
};

interface StorageInterface {
  // TODO upsert, extra id?
  addTrackedContent(location: string, content: string): Promise<void>;
  getMatches(content: string): Promise<Array<Promise<StorageMatch>>>;
  genUpdate(oldContent: string, newContent: string, match: StorageMatch): Promise<StorageUpdate>;
  // TODO wrapper with optional location id assert location oldcontent?
  genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<StorageUpdate>>>;
  updateContent(update: StorageUpdate): Promise<void>;
};

abstract class BaseStorageMixin implements StorageInterface {
  private _presleepForTest: number = 0; // hacky
  get presleepForTest(): number {
    return this._presleepForTest;
  };
  set presleepForTest(presleepForTest: number) {
    this._presleepForTest = presleepForTest;
  }
  protected async awaitablePresleepForTest(): Promise<void> {
    if (this.presleepForTest > 0) {
      await new Promise(r => setTimeout(r, this.presleepForTest));
    };
  };
  protected computeIfAbsent<K, V>(m: Map<K, V>, k: K, dv: V): V {
    return m.get(k) ?? (m.set(k, dv), dv);
  };
  abstract addTrackedContent(location: string, content: string): Promise<void>;
  abstract getMatches(content: string): Promise<Array<Promise<StorageMatch>>>;
  abstract genUpdate(oldContent: string, newContent: string, match: StorageMatch): Promise<StorageUpdate>;
  async genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<StorageUpdate>>> {
    return (await this.getMatches(oldContent)).map(getMatch => getMatch.then(match => this.genUpdate(oldContent, newContent, match)));
  };
  abstract updateContent(update: StorageUpdate): Promise<void>;
}

export class DirectEqualityInMemoryMapStorage extends BaseStorageMixin implements StorageInterface { // thread-safety?
  private locationContents = new Map<string, Set<string>>;
  private embeddingContentLocations = new Map<string, Map<string, string>>;
  override async addTrackedContent(location: string, content: string): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.computeIfAbsent(this.locationContents, location, new Set).add(content))(),
      (async () => {
        const embedding = content;
        this.computeIfAbsent(this.embeddingContentLocations, embedding, new Map).set(location, content)
      })()
    );
  };
  override async getMatches(content: string): Promise<Array<Promise<StorageMatch>>> {
    const embedding = content;
    return [...(await (async () => this.embeddingContentLocations.get(embedding)?.entries())() ?? [])].map(async ([location, content]) => {
      return new StorageMatch(location, content, embedding);
    });
  };
  override async genUpdate(oldContent: string, newContent: string, match: StorageMatch): Promise<StorageUpdate> {
    console.assert(match.content == oldContent);
    const newMatchContent = newContent;
    const newMatchContentEmbedding = newMatchContent;
    return new StorageUpdate(match.location, match.content, newMatchContent, match.embedding, newMatchContentEmbedding);
  };
  override async updateContent(update: StorageUpdate): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.locationContents.get(update.location)?.add(update.newContent))(),
      (async () => this.locationContents.get(update.location)?.delete(update.oldContent))(),
      (async () => this.computeIfAbsent(this.embeddingContentLocations, update.newContentEmbedding, new Map).set(update.location, update.newContent))(),
      (async () => {
        this.embeddingContentLocations.get(update.oldContentEmbedding)?.delete(update.location);
        if (this.embeddingContentLocations.get(update.oldContentEmbedding)?.size == 0) {
          this.embeddingContentLocations.delete(update.oldContentEmbedding);
        };
      })()
    );
  };
};

import { VectorOperationsApi } from '@pinecone-database/pinecone/dist/pinecone-generated-ts-fetch'; // integrate
// import { PineconeClient } from '@pinecone-database/pinecone';
// const pinecone = new PineconeClient();
// const pineconeInit = {
//   environment: 'us-west1-gcp-free',
//   apiKey: '<pinecone key>',
// };
// await pinecone.init(pineconeInit);
// const index = pinecone.Index('knoba');
const { Configuration, OpenAIApi } = require('openai'); // lint
const configuration = new Configuration({
  apiKey: '<openai key>', // env
});
const openai = new OpenAIApi(configuration);
export class SemanticMatchingExternalStorage extends BaseStorageMixin implements StorageInterface { // thread-safety?
  index: VectorOperationsApi | null = null; // hacky
  override async addTrackedContent(location: string, content: string): Promise<void> { // batch
    await this.awaitablePresleepForTest();
    (async () => {
      const embedding = (await openai.createEmbedding({ // cache
          model: 'text-embedding-ada-002',
          input: content,
      }))?.data?.data[0]?.embedding; // error handling
      const upsertRequest = {
        vectors: [
          {
            id: `${location} - ${content}`, // id
            values: embedding,
            metadata: {
              location: location,
              content: content,
            },
          },
        ],
      };
      this.index?.upsert({ upsertRequest });
    })();
  };
  override async getMatches(content: string): Promise<Array<Promise<StorageMatch>>> {
    return (async () => {
      const embedding = (await openai.createEmbedding({ // cache
        model: 'text-embedding-ada-002',
        input: content,
      }))?.data?.data[0]?.embedding; // error handling
      // get close, but not equivalent, matches for embedding from vector db
      // where to filter if embedding of matches and `content` of interest is not same/semantically equivalent
      const queryRequest = { // pagination
        vector: embedding,
        topK: 10,
        includeValues: true,
        includeMetadata: true,
      };
      const queryResponse = await this.index?.query({ queryRequest });
      // TODO map results to InMemoryStorageMatch
      return [];
    })();
  };
  override async genUpdate(oldContent: string, newContent: string, match: StorageMatch): Promise<StorageUpdate> {
    const newMatchContent = newContent; // TODO llm generate `newMatchContent` based on semantic change (`oldContent`, `newContent`) and old `match.content`
    const newMatchContentEmbedding = (await openai.createEmbedding({ // cache
      model: 'text-embedding-ada-002',
      input: newMatchContent,
    }))?.data?.data[0]?.embedding; // error handling
    return new StorageUpdate(match.location, match.content, newMatchContent, match.embedding, newMatchContentEmbedding);
  };
  override async updateContent(update: StorageUpdate): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => {
        // TODO put `update.newContentEmbedding` -> (`update.location`, `update.newContent`) in vector db
      })(),
      (async () => {
        // TODO remove `update.oldContentEmbedding` -> `update.location` from vector db
      })()
    );
  };
};
