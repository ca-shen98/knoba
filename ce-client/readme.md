chrome extension client

```
brew install node
brew install yarn

npm install typescript
yarn add typescript
yarn add ts-node

npx tsc --init --outDir ./built
npx tsc ./src/*.ts --outDir ./built --target es2015
yarn tsc --init --outDir ./built
yarn tsc

npx ts-node
yarn ts-node
```

need to figure out the prompt
- attempt/approach 1: https://platform.openai.com/playground/p/fQknB4kOWmTHv2Ws5QwLsJdH?model=text-davinci-003
- attempt/approach 2: https://platform.openai.com/playground/p/GKX9KIClOlB3Xr53Kz6p1cLx?model=text-davinci-003

need examples

implement semantic matching

pinecone and embeddings api (openai) integration

need secrets/tokens

llm latencies, caching

check out langchain
