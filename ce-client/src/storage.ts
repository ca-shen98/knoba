class InMemoryMapStorageMatch {
  location: string;
  content: string;
  embedding: string;
  constructor(location: string, content: string, embedding: string) {
    this.location = location;
    this.content = content;
    this.embedding = embedding;
  };
};
class InMemoryMapStorageUpdate {
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
  addTrackedContent(location: string, content: string): Promise<void>;
  getMatches(content: string): Promise<Array<Promise<InMemoryMapStorageMatch>>>;
  genUpdate(oldContent: string, newContent: string, match: InMemoryMapStorageMatch): Promise<InMemoryMapStorageUpdate>;
  genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<InMemoryMapStorageUpdate>>>;
  updateContent(update: InMemoryMapStorageUpdate): Promise<void>;
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
  abstract getMatches(content: string): Promise<Array<Promise<InMemoryMapStorageMatch>>>;
  abstract genUpdate(oldContent: string, newContent: string, match: InMemoryMapStorageMatch): Promise<InMemoryMapStorageUpdate>;
  async genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<InMemoryMapStorageUpdate>>> {
    return (await this.getMatches(oldContent)).map(getMatch => getMatch.then(match => this.genUpdate(oldContent, newContent, match)));
  };
  abstract updateContent(update: InMemoryMapStorageUpdate): Promise<void>;
}

export class InMemoryMapStorage extends BaseStorageMixin implements StorageInterface {
  private inMemoryMapStorage = { // thread-safety?
    embeddingContentLocations: new Map<string, Map<string, string>>,
    locationContents: new Map<string, Set<string>>,
  };
  override async addTrackedContent(location: string, content: string): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.computeIfAbsent(this.inMemoryMapStorage.locationContents, location, new Set).add(content))(),
      (async () => {
        const embedding = content;
        this.computeIfAbsent(this.inMemoryMapStorage.embeddingContentLocations, embedding, new Map).set(location, content)
      })()
    );
  };
  override async getMatches(content: string): Promise<Array<Promise<InMemoryMapStorageMatch>>> {
    const embedding = content;
    return [...(await (async () => this.inMemoryMapStorage.embeddingContentLocations.get(embedding)?.entries())() ?? [])].map(async ([location, content]) => {
      return new InMemoryMapStorageMatch(location, content, embedding);
    });
  };
  override async genUpdate(oldContent: string, newContent: string, match: InMemoryMapStorageMatch): Promise<InMemoryMapStorageUpdate> {
    console.assert(match.content == oldContent);
    const newMatchContent = newContent;
    const newMatchContentEmbedding = newMatchContent;
    return new InMemoryMapStorageUpdate(match.location, match.content, newMatchContent, match.embedding, newMatchContentEmbedding);
  };
  override async updateContent(update: InMemoryMapStorageUpdate): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.inMemoryMapStorage.locationContents.get(update.location)?.add(update.newContent))(),
      (async () => this.inMemoryMapStorage.locationContents.get(update.location)?.delete(update.oldContent))(),
      (async () => this.computeIfAbsent(this.inMemoryMapStorage.embeddingContentLocations, update.newContentEmbedding, new Map).set(update.location, update.newContent))(),
      (async () => {
        this.inMemoryMapStorage.embeddingContentLocations.get(update.oldContentEmbedding)?.delete(update.location);
        if (this.inMemoryMapStorage.embeddingContentLocations.get(update.oldContentEmbedding)?.size == 0) {
          this.inMemoryMapStorage.embeddingContentLocations.delete(update.oldContentEmbedding);
        };
      })()
    );
  };
};

export class ExternalStorage extends BaseStorageMixin implements StorageInterface {
  private externalStorage = { // thread-safety?
    locationContents: new Map<string, Set<string>>, // TODO externalize, currently for test/stub, don't need to keep track content in external sources (e.g. notion), access via apis
  };
  override async addTrackedContent(location: string, content: string): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.computeIfAbsent(this.externalStorage.locationContents, location, new Set).add(content))(),
      (async () => {
        const embedding = content; // llm calculate `embedding` for `content` - cache?
        // put `embedding` -> (`location`, `content`) in vector db
      })()
    );
  };
  override async getMatches(content: string): Promise<Array<Promise<InMemoryMapStorageMatch>>> {
    return (async () => {
      // llm calculate embedding for `content` - cache?
      // get close matches for embedding from vector db
      // map results to InMemoryStorageMatch
      return [];
    })();
  };
  override async genUpdate(oldContent: string, newContent: string, match: InMemoryMapStorageMatch): Promise<InMemoryMapStorageUpdate> {
    const newMatchContent = newContent; // llm generate `newMatchContent` based on semantic change (`oldContent`, `newContent`) and old `match.content`
    const newMatchContentEmbedding = newMatchContent; // llm calculate `newMatchContentEmbedding` for `newMatchContent` - cache?
    return new InMemoryMapStorageUpdate(match.location, match.content, newMatchContent, match.embedding, newMatchContentEmbedding);
  };
  override async updateContent(update: InMemoryMapStorageUpdate): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.externalStorage.locationContents.get(update.location)?.add(update.newContent))(),
      (async () => this.externalStorage.locationContents.get(update.location)?.delete(update.oldContent))(),
      (async () => {
        // put `update.newContentEmbedding` -> (`update.location`, `update.newContent`) in vector db
      })(),
      (async () => {
        // remove `update.oldContentEmbedding` -> `update.location` from vector db
      })()
    );
  };
};
