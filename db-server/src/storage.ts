function isTruthy<T>(v: T | undefined): v is T { return !!v }; // syntax style

// types/interfaces
export class StorageUnit<T> {
  location: string; // location is unique id
  content: string;
  embedding: T;
  constructor(location: string, content: string, embedding: T) {
    this.location = location;
    this.content = content;
    this.embedding = embedding;
  };
};

interface StorageInterface<T> {
  getMatches(query: StorageUnit<T>): Promise<Array<StorageUnit<T>>>;
  genUpdate(
    query: StorageUnit<T>,
    diff: StorageUnit<T>,
    match: StorageUnit<T>
  ): Promise<StorageUnit<T>>;
  genUpdates(query: StorageUnit<T>, diff: StorageUnit<T>)
    : Promise<Array<Promise<StorageUnit<T>>>>;
  upsertContent(query: StorageUnit<T>): Promise<void>;
};

abstract class BaseStorageMixin<T> implements StorageInterface<T> {
  abstract getMatches(query: StorageUnit<T>): Promise<Array<StorageUnit<T>>>;
  abstract genUpdate(
    query: StorageUnit<T>,
    diff: StorageUnit<T>,
    match: StorageUnit<T>
  ): Promise<StorageUnit<T>>;
  async genUpdates(query: StorageUnit<T>, diff: StorageUnit<T>)
    : Promise<Array<Promise<StorageUnit<T>>>> { // inner promise
    return (await this.getMatches(query)).map(match => this.genUpdate(query, diff, match));
  };
  abstract upsertContent(query: StorageUnit<T>): Promise<void>;
  
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
};

export class DirectEqualityInMemoryMapStorage
  extends BaseStorageMixin<string>
  implements StorageInterface<string> { // thread-safety?

  private locationContents = new Map<string, StorageUnit<string>>;
  private embeddedLocationContents = new Map<string, Map<string, StorageUnit<string>>>;

  override async getMatches(query: StorageUnit<string>): Promise<Array<StorageUnit<string>>> {
    return [...(await (async () =>
      this.embeddedLocationContents.get(query.embedding)?.entries()
    )() ?? [])]
      .map(([_location, match]) => match); // copy by value?
  };

  override async genUpdate(
    query: StorageUnit<string>,
    _diff: StorageUnit<string>,
    match: StorageUnit<string>
  ): Promise<StorageUnit<string>> {
    const newMatchContent = query.content;
    const newMatchContentEmbedding = newMatchContent;
    return new StorageUnit(match.location, newMatchContent, newMatchContentEmbedding);
  };

  override async upsertContent(query: StorageUnit<string>): Promise<void> {
    await this.awaitablePresleepForTest();
    const oldContentEmbedding = this.locationContents.get(query.location)?.embedding;
    await Promise.all([ // strong/weak error handling
      (async () => this.locationContents.set(query.location, query))(),
      (async () => {
        if (oldContentEmbedding) { // need delete before set to prevent set then delete of same
          this.embeddedLocationContents.get(oldContentEmbedding)?.delete(query.location);
        }
        // computeIfAbsent<K, V>(m: Map<K, V>, k: K, dv: V): V {
        //   return m.get(k) ?? (m.set(k, dv), dv);
        // };
        // computeIfAbsent(this.embeddedLocationContents, query.embedding, new Map).set(query.location, query)
        const existingEmbedding = this.embeddedLocationContents.get(query.embedding);
        if (existingEmbedding) {
          existingEmbedding.set(query.location, query);
        } else {
          const newEmbedding = new Map;
          newEmbedding.set(query.location, query);
          this.embeddedLocationContents.set(query.embedding, newEmbedding);
        }
      })()
    ]);
  };
};

import {
  VectorOperationsApi
} from '@pinecone-database/pinecone/dist/pinecone-generated-ts-fetch'; // integrate
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

export class SemanticMatchingExternalStorage
  extends BaseStorageMixin<number[]>
  implements StorageInterface<number[]> { // thread-safety?

  index: VectorOperationsApi | null = null; // hacky
  
  override async getMatches(query: StorageUnit<number[]>): Promise<Array<StorageUnit<number[]>>> {
    return [...((await this.index?.query({
      queryRequest: { // topk, pagination
        vector: query.embedding, // coupling/cohesion embedding responsibility
        topK: 10,
        includeValues: true,
        includeMetadata: true,
      },
    }))?.matches ?? [])]
      // TODO get close, but not equivalent, matches for embedding from vector db
      // where to filter if embedding of matches and `content` of interest is same
      .map(match =>
        match.metadata &&
        match.values &&
        new StorageUnit<number[]>(
          match.metadata['location'],
          match.metadata['content'],
          match.values
        )
      )
      .filter(isTruthy);
  };

  override async genUpdate(
    query: StorageUnit<number[]>,
    _diff: StorageUnit<number[]>,
    match: StorageUnit<number[]>
  ): Promise<StorageUnit<number[]>> {
    // TODO llm generate `newMatchContent` based on semantic change (diff, query)
    // but if equivalent, do direct replacement
    const newMatchContent = query.content;
    const newMatchContentEmbedding = (await openai.createEmbedding({ // cache, bulk/batch
      model: 'text-embedding-ada-002',
      input: newMatchContent,
    }))?.data?.data[0]?.embedding; // error handling
    return new StorageUnit(match.location, newMatchContent, newMatchContentEmbedding);
  };

  override async upsertContent(query: StorageUnit<number[]>): Promise<void> {
    await this.awaitablePresleepForTest();
    await this.index?.upsert({ // bulk/batch
      upsertRequest: {
        vectors: [
          {
            id: query.location,
            values: query.embedding,
            metadata: {
              location: query.location,
              content: query.content,
            },
          },
        ],
      },
    });
  };
};
