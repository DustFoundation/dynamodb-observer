# @dustfoundation/dynamodb-observer

![CI](https://github.com/DustFoundation/dynamodb-observer/actions/workflows/ci.yml/badge.svg)
[![NPM Version](https://badgen.net/npm/v/@dustfoundation/dynamodb-observer)](https://npmjs.com/package/@dustfoundation/dynamodb-observer)
[![Minimum Node.js Version](https://badgen.net/npm/node/@dustfoundation/dynamodb-observer)](https://npmjs.com/package/@dustfoundation/dynamodb-observer)

**DynamoDB Observer** to monitor Unit Capacity usage.

## Installation

```sh
npm install --save @dustfoundation/dynamodb-observer
```

## Usage

### Default

```ts
import { DynamoDB } from '@dustfoundation/dynamodb-observer';

const client = new DynamoDB({
  region: 'eu-central-1',
  keys: { ['table-name']: 'userId' },
});
```

### Dynamoose

```ts
import { DynamoDB } from '@dustfoundation/dynamodb-observer';
import { aws, Schema } from 'dynamoose';

const SomeSchema = new Schema({
  userId: { type: String, hashKey: true },
});

const client = new DynamoDB({
  region: 'eu-central-1',
  keys: { ['table-name']: SomeSchema.hashKey },
});
aws.ddb.set(client);
```
