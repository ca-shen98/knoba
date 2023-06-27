import { DirectEqualityInMemoryMapStorage } from '../src/storage';
// import { DirectEqualityInMemoryMapStorage } from './src/storage';

// addTrackedContent side effect without await is async (should fail)
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  directEqualityInMemoryMapStorage.addTrackedContent('a', 'abc');
  console.assert((await directEqualityInMemoryMapStorage.getMatches('abc')).length == 1); // should fail
})();

// addTrackedContent side effect
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  await directEqualityInMemoryMapStorage.addTrackedContent('a', 'abc');
  console.assert((await directEqualityInMemoryMapStorage.getMatches('abc')).length == 1);
  const genUpdates = (await directEqualityInMemoryMapStorage.genUpdates('abc', 'alphabet'));
  await Promise.all(genUpdates.map(genUpdate => genUpdate.then(update => directEqualityInMemoryMapStorage.updateContent(update))));
  console.assert((await directEqualityInMemoryMapStorage.getMatches('abc')).length == 0);
  console.assert((await directEqualityInMemoryMapStorage.getMatches('alphabet')).length == 1);
})();
