database server

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

chrome storage; integrations; chrome extension

backend remote client; authentication, secrets.env; firebase/lambdas?

prompt engineering, need to figure out the prompt (check out filters/edit api), prevent prompt injection
- attempt/approach 1: https://platform.openai.com/playground/p/fQknB4kOWmTHv2Ws5QwLsJdH?model=text-davinci-003
- attempt/approach 2: https://platform.openai.com/playground/p/GKX9KIClOlB3Xr53Kz6p1cLx?model=text-davinci-003
need examples

pinecone and embeddings api (openai) integration implement semantic matching

llm latencies, caching, message queues?

check out langchain
