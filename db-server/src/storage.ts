const assert = require('assert');

type StorageUnit<T> = {
  location: string;
  content: string;
  embedding: T;
};

abstract class BaseStorageInterfaceMixin<T> {
  // TODO match -> update -> upsert atomic for consistency/isolation if remove?
  abstract getMatches(query: StorageUnit<T>): Promise<Array<StorageUnit<T>>>;
  abstract genUpdate(
    query: StorageUnit<T>,
    diff: StorageUnit<T>,
    match: StorageUnit<T>
  ): Promise<StorageUnit<T>>;
  async genUpdates(query: StorageUnit<T>, diff: StorageUnit<T>)
    : Promise<Array<Promise<StorageUnit<T>>>> {
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

export class DirectEqualityInMemoryMapStorage extends BaseStorageInterfaceMixin<string> {

  private locationContents = new Map<string, StorageUnit<string>>;
  private embeddedLocationContents = new Map<string, Map<string, StorageUnit<string>>>;

  override async getMatches(query: StorageUnit<string>): Promise<Array<StorageUnit<string>>> {
    return [...(await (async () =>
      this.embeddedLocationContents.get(query.embedding)?.entries()
    )() ?? [])]
      .map(([location, match]) => {
        assert(location == match.location);
        return { location: match.location, content: match.content, embedding: match.embedding };
      });
  };

  override async genUpdate(
    query: StorageUnit<string>,
    _diff: StorageUnit<string>,
    match: StorageUnit<string>
  ): Promise<StorageUnit<string>> {
    const updateContent = query.content;
    const updateEmbedding = query.embedding;
    return { location: match.location, content: updateContent, embedding: updateEmbedding };
  };

  override async upsertContent(query: StorageUnit<string>): Promise<void> {
    await this.awaitablePresleepForTest();
    // get old value before upsert set locationContents(query.location, query)
    const oldContentEmbedding = this.locationContents.get(query.location)?.embedding;
    await Promise.all([ // strong/weak error handling atomicity
      (async () => this.locationContents.set(query.location, query))(),
      (async () => {
        if (oldContentEmbedding) { // need delete before set to prevent set then delete of same
          this.embeddedLocationContents.get(oldContentEmbedding)?.delete(query.location);
        }
        /*
         * computeIfAbsent<K, V>(m: Map<K, V>, k: K, dv: V): V {
         *   return m.get(k) ?? (m.set(k, dv), dv);
         * };
         * computeIfAbsent(this.embeddedLocationContents, query.embedding, new Map).set(query.location, query)
         */
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

/*
 * import { PineconeClient } from '@pinecone-database/pinecone';
 * const pinecone = new PineconeClient();
 * const pineconeInit = {
 *   environment: 'us-west1-gcp-free',
 *   apiKey: '<pinecone key>',
 * };
 * await pinecone.init(pineconeInit);
 * const index = pinecone.Index('knoba');
 */
const { Configuration, OpenAIApi } = require('openai');
const configuration = new Configuration({
  apiKey: '<openai key>',
});
const openai = new OpenAIApi(configuration);

import {
  VectorOperationsApi
} from '@pinecone-database/pinecone/dist/pinecone-generated-ts-fetch';

export class SemanticMatchingExternalStorage extends BaseStorageInterfaceMixin<number[]> {

  index: VectorOperationsApi | null = null; // hacky, integration
  
  override async getMatches(query: StorageUnit<number[]>): Promise<Array<StorageUnit<number[]>>> {
    return [...((await this.index?.query({
      queryRequest: {
        vector: query.embedding, // coupling/cohesion embedding responsibility
        topK: 10, // topk, pagination, completeness, retries
        includeValues: true,
        includeMetadata: true,
      },
    }))?.matches ?? [])]
      // TODO where to filter equivalent matches for embedding from vector db
      .flatMap(match =>
        (match.metadata && match.values) ? {
          location: match.metadata['location'],
          content: match.metadata['content'],
          embedding: match.values
        } : []
      );
  };

  override async genUpdate(
    query: StorageUnit<number[]>,
    _diff: StorageUnit<number[]>,
    match: StorageUnit<number[]>
  ): Promise<StorageUnit<number[]>> {
    // TODO llm generate `updateContent` based on semantic change (diff, query) but if equivalent, do direct replacement
    /* // cache, bulk/batch, error handling
     * const updateEmbedding = (await openai.createEmbedding({
     *   model: 'text-embedding-ada-002',
     *   input: updateContent,
     * }))?.data?.data[0]?.embedding;
     */
    const updateContent = query.content;
    const updateEmbedding = query.embedding;
    return {
      location: match.location,
      content: updateContent,
      embedding: updateEmbedding,
    };
  };

  override async upsertContent(query: StorageUnit<number[]>): Promise<void> {
    await this.awaitablePresleepForTest();
    await this.index?.upsert({ // retries, bulk/batch
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
