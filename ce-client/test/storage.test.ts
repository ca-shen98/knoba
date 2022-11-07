import { InMemoryMapStorage } from "../src/storage";

// addTrackedContent side effect without await is async
await (async () => {
    const inMemoryMapStorage = new InMemoryMapStorage();
    inMemoryMapStorage.presleepForTest = 100;
    inMemoryMapStorage.addTrackedContent('a', 'abc');
    console.assert((await inMemoryMapStorage.getMatches('abc')).length == 0);
})();

// addTrackedContent side effect
await (async () => {
    const inMemoryMapStorage = new InMemoryMapStorage();
    inMemoryMapStorage.presleepForTest = 100;
    await inMemoryMapStorage.addTrackedContent('a', 'abc');
    console.assert((await inMemoryMapStorage.getMatches('abc')).length == 1);
    const genUpdates = (await inMemoryMapStorage.genUpdates('abc', 'alphabet'));
    await Promise.all(genUpdates.map(genUpdate => genUpdate.then(update => inMemoryMapStorage.updateContent(update))));
    console.assert((await inMemoryMapStorage.getMatches('abc')).length == 0);
    console.assert((await inMemoryMapStorage.getMatches('alphabet')).length == 1);
})();
