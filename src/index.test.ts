import { expect } from 'chai';
import { randomUUID } from 'node:crypto';
import { DynamoDB, DynamoDBConfig } from '.';

describe('DynamoDB', () => {
  let ddb: DynamoDB;
  const TableName = 'table-name';
  const TableNameSecond = 'table-name-2';

  beforeEach(async () => {
    ddb = new DynamoDB({
      keys: { [TableName]: { hashKey: 'hashKey' }, [TableNameSecond]: { hashKey: 'hashKey' } },
      region: 'eu-central-1',
      endpoint: 'http://localhost:5000',
      credentials: {
        accessKeyId: 'accessKeyId',
        secretAccessKey: 'secretAccessKey',
      },
    });
    setDdbHook(ddb, []);
    await ddb.createTable({
      TableName,
      KeySchema: [{ AttributeName: 'hashKey', KeyType: 'HASH' }],
      AttributeDefinitions: [{ AttributeName: 'hashKey', AttributeType: 'S' }],
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
    });
    await ddb.createTable({
      TableName: TableNameSecond,
      KeySchema: [{ AttributeName: 'hashKey', KeyType: 'HASH' }],
      AttributeDefinitions: [{ AttributeName: 'hashKey', AttributeType: 'S' }],
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
    });
  });

  afterEach(() =>
    Promise.all([ddb.deleteTable({ TableName }), ddb.deleteTable({ TableName: TableNameSecond })]),
  );

  it('getItem', async () => {
    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.getItem({ TableName, Key: { hashKey: { S: hashKey } } });

    expect(response).eql([
      {
        method: 'getItem',
        capacityUnits: 0.5,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('putItem', async () => {
    const response: Response = [];
    setDdbHook(ddb, response);

    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    expect(response).eql([
      {
        method: 'putItem',
        capacityUnits: 1,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('updateItem', async () => {
    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.updateItem({ TableName, Key: { hashKey: { S: hashKey } } });

    expect(response).eql([
      {
        method: 'updateItem',
        capacityUnits: 1,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('deleteItem', async () => {
    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.deleteItem({ TableName, Key: { hashKey: { S: hashKey } } });

    expect(response).eql([
      {
        method: 'deleteItem',
        capacityUnits: 1,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('batchGetItem', async () => {
    const hashKeys = Array.from<any>({ length: 5 }).map(() => randomUUID());
    await ddb.batchWriteItem({
      RequestItems: {
        [TableName]: hashKeys.map((hashKey) => ({ PutRequest: { Item: { hashKey: { S: hashKey } } } })),
        [TableNameSecond]: hashKeys.map((hashKey) => ({ PutRequest: { Item: { hashKey: { S: hashKey } } } })),
      },
    });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.batchGetItem({
      RequestItems: {
        [TableName]: {
          Keys: hashKeys.map((hashKey) => ({ hashKey: { S: hashKey } })),
        },
        [TableNameSecond]: {
          Keys: hashKeys.map((hashKey) => ({ hashKey: { S: hashKey } })),
        },
      },
    });

    expect(response).eql(
      Array.from({ length: 2 }).map(() => ({
        method: 'batchGetItem',
        capacityUnits: 2.5,
        hashKeys,
      })),
    );
  });

  it('batchWriteItem', async () => {
    const response: Response = [];
    setDdbHook(ddb, response);

    const hashKeys = Array.from<any>({ length: 5 }).map(() => randomUUID());
    await ddb.batchWriteItem({
      RequestItems: {
        [TableName]: hashKeys.map((hashKey) => ({ PutRequest: { Item: { hashKey: { S: hashKey } } } })),
        [TableNameSecond]: hashKeys.map((hashKey) => ({ PutRequest: { Item: { hashKey: { S: hashKey } } } })),
      },
    });

    expect(response).eql(
      Array.from({ length: 2 }).map(() => ({
        method: 'batchWriteItem',
        capacityUnits: 5,
        hashKeys,
      })),
    );
  });

  it('query', async () => {
    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.query({
      TableName,
      KeyConditionExpression: 'hashKey = :hashKey',
      ExpressionAttributeValues: {
        ':hashKey': { S: hashKey },
      },
    });

    expect(response).eql([
      {
        method: 'query',
        capacityUnits: 0.5,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('scan', async () => {
    const hashKey = randomUUID();
    await ddb.putItem({ TableName, Item: { hashKey: { S: hashKey } } });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.scan({
      TableName,
      FilterExpression: 'hashKey = :hashKey',
      ExpressionAttributeValues: {
        ':hashKey': { S: hashKey },
      },
    });

    expect(response).eql([
      {
        method: 'scan',
        capacityUnits: 0.5,
        hashKeys: [hashKey],
      },
    ]);
  });

  it('transactGetItems', async () => {
    const hashKey = randomUUID();
    await ddb.transactWriteItems({
      TransactItems: [TableName, TableNameSecond].map((tableName) => ({
        Put: { TableName: tableName, Item: { hashKey: { S: hashKey } } },
      })),
    });

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.transactGetItems({
      TransactItems: [TableName, TableNameSecond].map((tableName) => ({
        Get: { TableName: tableName, Key: { hashKey: { S: hashKey } } },
      })),
    });

    expect(response).eql(
      Array.from({ length: 2 }).map(() => ({
        method: 'transactGetItems',
        capacityUnits: 2,
        hashKeys: [hashKey],
      })),
    );
  });

  it('transactWriteItems', async () => {
    const hashKeys = Array.from<any>({ length: 4 }).map(() => randomUUID());
    await Promise.all(
      [TableName, TableNameSecond].map((tableName) =>
        ddb.putItem({ TableName: tableName, Item: { hashKey: { S: hashKeys[3] } } }),
      ),
    );

    const response: Response = [];
    setDdbHook(ddb, response);

    await ddb.transactWriteItems({
      // @ts-expect-error
      TransactItems: [TableName, TableNameSecond].flatMap((tableName) => [
        { Put: { TableName: tableName, Item: { hashKey: { S: hashKeys[0] } } } },
        { Update: { TableName: tableName, Key: { hashKey: { S: hashKeys[1] } } } },
        { Delete: { TableName: tableName, Key: { hashKey: { S: hashKeys[2] } } } },
        {
          ConditionCheck: {
            TableName: tableName,
            Key: { hashKey: { S: hashKeys[3] } },
            ConditionExpression: 'attribute_exists(hashKey)',
          },
        },
      ]),
    });

    expect(response).eql(
      Array.from({ length: 2 }).map(() => ({
        method: 'transactWriteItems',
        capacityUnits: 6,
        hashKeys,
      })),
    );
  });
});

function setDdbHook(ddb: DynamoDB, response: Response): void {
  // @ts-expect-error
  ddb['hook'] = (method, capacityUnits, hashKeys) => {
    response.push({ method, capacityUnits, hashKeys });
  };
}

type Response = Array<{
  method: Parameters<NonNullable<DynamoDBConfig['hook']>>[0];
  capacityUnits: Parameters<NonNullable<DynamoDBConfig['hook']>>[1];
  hashKeys: Parameters<NonNullable<DynamoDBConfig['hook']>>[2];
}>;
