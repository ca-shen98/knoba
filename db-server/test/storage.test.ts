import { DirectEqualityInMemoryMapStorage } from '../src/storage';
// import { DirectEqualityInMemoryMapStorage } from './src/storage';

const assert = require('assert');

// addTrackedContent side effect without await is async (should fail)
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  directEqualityInMemoryMapStorage.upsertContent({ location: 'a', content: 'abc', embedding: 'abc' });
  assert((await directEqualityInMemoryMapStorage.getMatches({ location: 'a', content: 'abc', embedding: 'abc' })).length == 1); // should fail
})();

// addTrackedContent side effect
// await
(async () => {
  const directEqualityInMemoryMapStorage = new DirectEqualityInMemoryMapStorage();
  directEqualityInMemoryMapStorage.presleepForTest = 100;
  directEqualityInMemoryMapStorage.upsertContent({ location: 'a', content: 'abc', embedding: 'abc' });
  assert((await directEqualityInMemoryMapStorage.getMatches({ location: 'a', content: 'abc', embedding: 'abc' })).length == 1);
  const genUpdates = (await directEqualityInMemoryMapStorage.genUpdates({ location: 'a', content: 'abc', embedding: 'abc' }, { location: 'a', content: 'alphabet', embedding: 'alphabet' }));
  await Promise.all(genUpdates.map(genUpdate => genUpdate.then(update => directEqualityInMemoryMapStorage.upsertContent(update))));
  assert((await directEqualityInMemoryMapStorage.getMatches({ location: 'a', content: 'abc', embedding: 'abc' })).length == 0);
  assert((await directEqualityInMemoryMapStorage.getMatches({ location: 'a', content: 'alphabet', embedding: 'alphabet' })).length == 1);
})();
