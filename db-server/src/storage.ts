const assert = require('assert');

// types/interfaces
class StorageMatch<T> {
  location: string;
  content: string;
  embedding: T;
  constructor(location: string, content: string, embedding: T) {
    this.location = location;
    this.content = content;
    this.embedding = embedding;
  };
};
export class StorageUpdate<T> {
  location: string;
  newContent: string;
  newContentEmbedding: T;
  oldContent?: string;
  oldContentEmbedding?: T;
  constructor(location: string, newContent: string, newContentEmbedding: T, oldContent?: string, oldContentEmbedding?: T) {
    this.location = location;
    this.newContent = newContent;
    this.newContentEmbedding = newContentEmbedding;
    this.oldContent = oldContent;
    this.oldContentEmbedding = oldContentEmbedding;
  };
};

interface StorageInterface<T> {
  getMatches(content: string): Promise<Array<Promise<StorageMatch<T>>>>;
  genUpdate(oldContent: string, newContent: string, match: StorageMatch<T>): Promise<StorageUpdate<T>>;
  // TODO wrapper with optional location id assert location oldcontent?
  genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<StorageUpdate<T>>>>;
  upsertContent(update: StorageUpdate<T>): Promise<void>;
};

abstract class BaseStorageMixin<T> implements StorageInterface<T> {
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
  abstract getMatches(content: string): Promise<Array<Promise<StorageMatch<T>>>>;
  abstract genUpdate(oldContent: string, newContent: string, match: StorageMatch<T>): Promise<StorageUpdate<T>>;
  async genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<StorageUpdate<T>>>> {
    return (await this.getMatches(oldContent)).map(getMatch => getMatch.then(match => this.genUpdate(oldContent, newContent, match)));
  };
  abstract upsertContent(update: StorageUpdate<T>): Promise<void>;
}

export class DirectEqualityInMemoryMapStorage extends BaseStorageMixin<string> implements StorageInterface<string> { // thread-safety?
  private locationContents = new Map<string, string>;
  private embeddedLocationContents = new Map<string, Map<string, string>>;
  override async getMatches(content: string): Promise<Array<Promise<StorageMatch<string>>>> {
    const embedding = content;
    return [...(await (async () => this.embeddedLocationContents.get(embedding)?.entries())() ?? [])].map(async ([location, content]) => {
      return new StorageMatch(location, content, embedding);
    });
  };
  override async genUpdate(oldContent: string, newContent: string, match: StorageMatch<string>): Promise<StorageUpdate<string>> {
    assert(match.content == oldContent);
    const newMatchContent = newContent;
    const newMatchContentEmbedding = newMatchContent;
    return new StorageUpdate(match.location, match.content, newMatchContent, match.embedding, newMatchContentEmbedding);
  };
  override async upsertContent(update: StorageUpdate<string>): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.locationContents.set(update.location, update.newContent))(),
      (async () => {
        if (update.oldContentEmbedding) {
          this.embeddedLocationContents.get(update.oldContentEmbedding)?.delete(update.location);
        }
        this.computeIfAbsent(this.embeddedLocationContents, update.newContentEmbedding, new Map).set(update.location, update.newContent)
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
  apiKey: '<openai key>',
});
const openai = new OpenAIApi(configuration);
export class SemanticMatchingExternalStorage extends BaseStorageMixin<number[]> implements StorageInterface<number[]> { // thread-safety?
  index: VectorOperationsApi | null = null; // hacky
  override async getMatches(content: string): Promise<Array<Promise<StorageMatch<number[]>>>> {
    const embedding = (await openai.createEmbedding({ // cache
      model: 'text-embedding-ada-002',
      input: content,
    }))?.data?.data[0]?.embedding; // error handling
    // TODO get close, but not equivalent, matches for embedding from vector db
    // TODO where to filter if embedding of matches and `content` of interest is not same/semantically equivalent
    const queryRequest = { // pagination
      vector: embedding,
      topK: 10,
      includeValues: true,
      includeMetadata: true,
    };
    const queryResponse = await this.index?.query({ queryRequest });
    // TODO map results to StorageMatch
    return [];
  };
  override async genUpdate(oldContent: string, newContent: string, match: StorageMatch<number[]>): Promise<StorageUpdate<number[]>> {
    const newMatchContent = newContent; // TODO llm generate `newMatchContent` based on semantic change (`oldContent`, `newContent`) and old `match.content`
    const newMatchContentEmbedding = (await openai.createEmbedding({ // cache
      model: 'text-embedding-ada-002',
      input: newMatchContent,
    }))?.data?.data[0]?.embedding; // error handling
    return new StorageUpdate(match.location, newMatchContent, newMatchContentEmbedding, match.content, match.embedding);
  };
  override async upsertContent(update: StorageUpdate<number[]>): Promise<void> {
    await this.awaitablePresleepForTest();
    (async () => {
      const upsertRequest = {
        vectors: [
          {
            id: update.location, // id
            values: update.newContentEmbedding,
            metadata: {
              location: update.location,
              content: update.newContent,
            },
          },
        ],
      };
      this.index?.upsert({ upsertRequest });
    })();
  };
};
