### flow for individual inputExternalId: string (within batch-list has extra machinations)
```
type KnobaProps = { knobaId: string, content: string, externalIds: Set<string> }
type KnobaBlockMatch = { score: number, vals: KnobaProps }
type KnobaBlockChange = { newContent: string, newContentEmbedding: number[], match: KnobaBlockMatch }

inputExternalId -fetch-> ingestContentBlocks: string[] // ingestContentBlocks[i].trim().length > 0
ingestContentBlocks -compute-> inBlockEmbeddings: number[][] // inBlockEmbeddings.length == ingestContentBlocks.length
inBlockEmbeddings -fetch-> knobaBlockChanges: KnobaBlockChange[] // knobaBlockChanges.length == ingestContentBlocks.length
 - if inBlockEmbeddings has match with score - 1 < 0.1, treat as change to block content
 - otherwise, need to upsert new knoba block/id (inline because subsequent blocks might match)

inputExternalId -fetch-> oldKnobaBlockIds: string[] -map-> Set<string> // fetch old list before upserting new one
// if knobaBlockChanges.length == 0, can be a delete
knobaBlockChanges.map(({...}) => knobaId) -upsert-> // update persistent externalId -> knobaIdsList datastore

knobaBlockChanges -pivot-> knobaIdBlockChanges: { [knobaId]: { KnobaBlockChange, externalIdsDelta: { upserts: string[], drops: string[] } } } // initially construct where knobaIdBlockChanges[id].externalIdsDelta.drops.length == 0, inputExternalId in upserts
oldKnobaBlockIds, keys(knobaBlockChanges) -diff-> droppedKnobaIds: Set<string>
droppedKnobaIds -fetch-> knobaBlocksForDroppedIds: { [knobaId]: KnobaProps }
knobaBlocksForDroppedIds -> append keys to knobaIdBlockChanges with inputExternalId in drops

knobaBlockChanges -generate-> propagatedChanges: { [knobaId]: KnobaBlockChange }
 - for each knobaId in knobaBlockChanges where match.score - 1 between 0.01 and 0.1
   - query for all other similar knoba blocks/ids of the old content for knobaId and foreach
     - apply the semantic transition to generate new content (+ embedding) for the secondary block
propagatedChanges, knobaBlockChanges -> merge into knobaBlockChanges
knobaBlockChanges -upsert-> pinecone

knobaBlockChanges -flatten,fetch-> toChangeExternalIds: { [externalId]: knobaIdsList: string[] }
toChangeExternalIds, knobaBlockChanges -> sync/propagate to external sources
 - for each externalId's knobaIdsList, fetch contents and append to upsert external location
``````
