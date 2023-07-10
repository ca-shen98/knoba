import { StorageMatch, StorageUpdate, DirectEqualityInMemoryMapStorage } from '../src/storage';
// import { DirectEqualityInMemoryMapStorage } from './src/storage';

const assert = require('assert'); // require.js

// addTrackedContent side effect without await is async (should fail)
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  directEqualityInMemoryMapStorage.upsertContent(new StorageUpdate('a', 'abc', 'abc', '', ''));
  assert((await directEqualityInMemoryMapStorage.getMatches(new StorageMatch('a', 'abc', 'abc'))).length == 1); // should fail
})();

// addTrackedContent side effect
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  directEqualityInMemoryMapStorage.upsertContent(new StorageUpdate('a', 'abc', 'abc', '', ''));
  assert((await directEqualityInMemoryMapStorage.getMatches(new StorageMatch('a', 'abc', 'abc'))).length == 1);
  const genUpdates = (await directEqualityInMemoryMapStorage.genUpdates(new StorageUpdate('a', 'abc', 'abc', 'alphabet', 'alphabet')));
  await Promise.all(genUpdates.map(genUpdate => genUpdate.then(update => directEqualityInMemoryMapStorage.upsertContent(update))));
  assert((await directEqualityInMemoryMapStorage.getMatches(new StorageMatch('a', 'abc', 'abc'))).length == 0);
  assert((await directEqualityInMemoryMapStorage.getMatches(new StorageMatch('a', 'alphabet', 'alphabet'))).length == 1);
})();
