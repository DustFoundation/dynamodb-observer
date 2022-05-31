import DDB, {
  DynamoDB as OriginDynamoDB,
  DynamoDBClientConfig as OriginDynamoDBClientConfig,
} from '@aws-sdk/client-dynamodb';

const defaultHook: NonNullable<DynamoDBConfig['hook']> = (
  method: keyof OriginDynamoDB,
  capacityUnits: number,
  hashKeys: string[],
) => {
  console.log(`[${method}] [CU: ${capacityUnits}] [Hash Keys: ${hashKeys.join(', ')}]`);
};

export class DynamoDB extends OriginDynamoDB {
  private readonly hook: NonNullable<DynamoDBConfig['hook']>;
  private readonly keys: NonNullable<DynamoDBConfig['keys']>;

  constructor(configuration: DynamoDBConfig) {
    super(configuration);
    this.hook = configuration.hook ?? defaultHook;
    this.keys = configuration.keys ?? {};
  }

  public async getItem(...args: [DDB.GetItemCommandInput, ...any]): Promise<DDB.GetItemCommandOutput> {
    return this.handler('getItem', args);
  }

  public async putItem(...args: [DDB.PutItemCommandInput, ...any]): Promise<DDB.PutItemCommandOutput> {
    if (!args[0].ReturnValues) args[0].ReturnValues = 'ALL_OLD';
    return this.handler('putItem', args);
  }

  public async updateItem(
    ...args: [DDB.UpdateItemCommandInput, ...any]
  ): Promise<DDB.UpdateItemCommandOutput> {
    if (!args[0].ReturnValues) args[0].ReturnValues = 'ALL_NEW';
    return this.handler('updateItem', args);
  }

  public async deleteItem(
    ...args: [DDB.DeleteItemCommandInput, ...any]
  ): Promise<DDB.DeleteItemCommandOutput> {
    if (!args[0].ReturnValues) args[0].ReturnValues = 'ALL_OLD';
    return this.handler('deleteItem', args);
  }

  public async batchGetItem(
    ...args: [DDB.BatchGetItemCommandInput, ...any]
  ): Promise<DDB.BatchGetItemCommandOutput> {
    return this.handler('batchGetItem', args);
  }

  public async batchWriteItem(
    ...args: [DDB.BatchWriteItemCommandInput, ...any]
  ): Promise<DDB.BatchWriteItemCommandOutput> {
    return this.handler('batchWriteItem', args);
  }

  public async query(...args: [DDB.QueryCommandInput, ...any]): Promise<DDB.QueryCommandOutput> {
    return this.handler('query', args);
  }

  public async scan(...args: [DDB.ScanCommandInput, ...any]): Promise<DDB.ScanCommandOutput> {
    return this.handler('query', args);
  }

  public async transactGetItems(
    ...args: [DDB.TransactGetItemsCommandInput, ...any]
  ): Promise<DDB.TransactGetItemsCommandOutput> {
    return this.handler('transactGetItems', args);
  }

  public async transactWriteItems(
    ...args: [DDB.TransactWriteItemsCommandInput, ...any]
  ): Promise<DDB.TransactWriteItemsCommandOutput> {
    return this.handler('transactWriteItems', args);
  }

  public async executeTransaction(
    ...args: [DDB.ExecuteTransactionCommandInput, ...any]
  ): Promise<DDB.ExecuteTransactionCommandOutput> {
    return this.handler('executeTransaction', args);
  }

  public async executeStatement(
    ...args: [DDB.ExecuteStatementCommandInput, ...any]
  ): Promise<DDB.ExecuteStatementCommandOutput> {
    return this.handler('executeStatement', args);
  }

  public async batchExecuteStatement(
    ...args: [DDB.BatchExecuteStatementCommandInput, ...any]
  ): Promise<DDB.BatchExecuteStatementCommandOutput> {
    return this.handler('batchExecuteStatement', args);
  }

  private async handler<T extends BaseRequest>(method: keyof OriginDynamoDB, args: any[]): Promise<T> {
    if (!args[0].ReturnConsumedCapacity) args[0].ReturnConsumedCapacity = 'TOTAL';

    // @ts-expect-error
    const response = (await super[method](...args)) as T;

    try {
      const Cap = response.ConsumedCapacity
        ? Array.isArray(response.ConsumedCapacity)
          ? response.ConsumedCapacity
          : [response.ConsumedCapacity]
        : undefined;
      if (!Cap) return response;

      Cap.forEach(async (cap) => {
        const tableName = cap.TableName;
        const capacityUnits = cap.CapacityUnits;
        const items = response.Item
          ? [response.Item]
          : response.Attributes
          ? [response.Attributes]
          : response.Items
          ? response.Items
          : response.Responses
          ? Array.isArray(response.Responses)
            ? response.Responses.map((r) => r.Item!)
            : Object.values(response.Responses).flatMap((r) => r.map((r) => r))
          : [];

        if (!(tableName && capacityUnits && items.length)) return;

        const key = this.keys[tableName]?.hashKey;
        if (!key) return;

        await Promise.resolve(
          this.hook(method, capacityUnits, [
            ...new Set(items.map((i) => i[key]['S'] || i[key]['N'] || i[key]['B']?.toString() || '')),
          ]),
        ).catch((e) => console.warn('Error when get capacity.', e));
      });
    } catch (e) {
      console.warn('Error when get capacity.', e);
    }

    return response;
  }
}

export interface DynamoDBConfig extends OriginDynamoDBClientConfig {
  hook?: (method: keyof OriginDynamoDB, capacityUnits: number, hashKeys: string[]) => void | Promise<void>;
  keys?: Record<string, { hashKey: string }>;
}

type BaseRequest = {
  ConsumedCapacity?: DDB.ConsumedCapacity | DDB.ConsumedCapacity[];
  Attributes?: Item;
  Item?: Item;
  Items?: Item[];
  Responses?: DDB.ItemResponse[] | Record<string, Item[]>;
  $metadata?: any;
};

type Item = Record<string, DDB.AttributeValue>;
