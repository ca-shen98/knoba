class InMemoryMapStorageMatch {
  location: string;
  content: string;
  constructor(location: string, content: string) {
    this.location = location;
    this.content = content;
  };
};
class InMemoryMapStorageUpdate {
  location: string;
  oldContent: string;
  newContent: string;
  constructor(location: string, oldContent: string, newContent: string) {
    this.location = location;
    this.oldContent = oldContent;
    this.newContent = newContent;
  };
};

export class InMemoryMapStorage {
  presleepForTest = 0
  // thread-safety?
  private inMemoryMapStorage = {
    contentLocations: new Map<string, Set<string>>,
    locationContents: new Map<string, Set<string>>,
  };
  private computeIfAbsent<K, V>(m: Map<K, V>, k: K, dv: V): V {
    return m.get(k) ?? (m.set(k, dv), dv);
  };
  // hacky
  private async awaitablePresleepForTest(): Promise<void> {
    if (this.presleepForTest > 0) {
      await new Promise(r => setTimeout(r, this.presleepForTest));
    };
  };
  async addTrackedContent(location: string, content: string): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.computeIfAbsent(this.inMemoryMapStorage.contentLocations, content, new Set).add(location))(),
      (async () => this.computeIfAbsent(this.inMemoryMapStorage.locationContents, location, new Set).add(content))()
    );
  };
  async getMatches(content: string): Promise<Array<Promise<InMemoryMapStorageMatch>>> {
    return [...(await (async () => this.inMemoryMapStorage.contentLocations.get(content))() ?? [])].map(async (location) => {
      return new InMemoryMapStorageMatch(location, content);
    });
  };
  private async genUpdate(oldContent: string, newContent: string, match: InMemoryMapStorageMatch): Promise<InMemoryMapStorageUpdate> {
    console.assert(match.content == oldContent);
    return new InMemoryMapStorageUpdate(match.location, match.content, newContent);
  };
  async genUpdates(oldContent: string, newContent: string): Promise<Array<Promise<InMemoryMapStorageUpdate>>> {
    return (await this.getMatches(oldContent)).map(getMatch => getMatch.then(match => this.genUpdate(oldContent, newContent, match)));
  };
  async updateContent(update: InMemoryMapStorageUpdate, presleepForTest: number = 0): Promise<void> {
    await this.awaitablePresleepForTest();
    (
      (async () => this.inMemoryMapStorage.locationContents.get(update.location)?.add(update.newContent))(),
      (async () => this.inMemoryMapStorage.locationContents.get(update.location)?.delete(update.oldContent))(),
      (async () => this.computeIfAbsent(this.inMemoryMapStorage.contentLocations, update.newContent, new Set).add(update.location))(),
      (async () => {
        this.inMemoryMapStorage.contentLocations.get(update.oldContent)?.delete(update.location);
        if (this.inMemoryMapStorage.contentLocations.get(update.oldContent)?.size == 0) {
          this.inMemoryMapStorage.contentLocations.delete(update.oldContent);
        };
      })()
    );
  };
};
