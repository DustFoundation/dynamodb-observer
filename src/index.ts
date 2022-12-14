import type { DynamoDBClientConfig as OriginDynamoDBClientConfig } from '@aws-sdk/client-dynamodb';
import type DDB from '@aws-sdk/client-dynamodb';
import { DynamoDB as OriginDynamoDB } from '@aws-sdk/client-dynamodb';

const defaultHook: NonNullable<DynamoDBConfig['hook']> = (
  method: keyof OriginDynamoDB,
  capacityUnits: number,
  keys: string[],
) => {
  console.log(`[${method}] [CU: ${capacityUnits}] [Keys: ${keys.join(', ')}]`);
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
    return this.handler('getItem', args, { [args[0].TableName!]: [args[0].Key!] });
  }

  public async putItem(...args: [DDB.PutItemCommandInput, ...any]): Promise<DDB.PutItemCommandOutput> {
    return this.handler('putItem', args, { [args[0].TableName!]: [args[0].Item!] });
  }

  public async updateItem(
    ...args: [DDB.UpdateItemCommandInput, ...any]
  ): Promise<DDB.UpdateItemCommandOutput> {
    return this.handler('updateItem', args, { [args[0].TableName!]: [args[0].Key!] });
  }

  public async deleteItem(
    ...args: [DDB.DeleteItemCommandInput, ...any]
  ): Promise<DDB.DeleteItemCommandOutput> {
    return this.handler('deleteItem', args, { [args[0].TableName!]: [args[0].Key!] });
  }

  public async batchGetItem(
    ...args: [DDB.BatchGetItemCommandInput, ...any]
  ): Promise<DDB.BatchGetItemCommandOutput> {
    return this.handler(
      'batchGetItem',
      args,
      Object.fromEntries(Object.entries(args[0].RequestItems!).map(([key, value]) => [key, value.Keys!])),
    );
  }

  public async batchWriteItem(
    ...args: [DDB.BatchWriteItemCommandInput, ...any]
  ): Promise<DDB.BatchWriteItemCommandOutput> {
    return this.handler(
      'batchWriteItem',
      args,
      Object.fromEntries(
        Object.entries(args[0].RequestItems!).map(([key, value]) => [
          key,
          // eslint-disable-next-line @typescript-eslint/no-non-null-asserted-optional-chain
          value.flatMap((value) => [value.PutRequest?.Item!, value.DeleteRequest?.Key!].filter((_) => _)),
        ]),
      ),
    );
  }

  public async query(...args: [DDB.QueryCommandInput, ...any]): Promise<DDB.QueryCommandOutput> {
    return this.handler('query', args);
  }

  public async scan(...args: [DDB.ScanCommandInput, ...any]): Promise<DDB.ScanCommandOutput> {
    return this.handler('scan', args);
  }

  public async transactGetItems(
    ...args: [DDB.TransactGetItemsCommandInput, ...any]
  ): Promise<DDB.TransactGetItemsCommandOutput> {
    return this.handler(
      'transactGetItems',
      args,
      args[0].TransactItems!.reduce((acc, item) => {
        const TableName = item.Get!.TableName!;
        const Key = item.Get!.Key!;
        if (!acc[TableName]) {
          acc[TableName] = [];
        }
        acc[TableName].push(Key);
        return acc;
      }, {} as Record<string, Item[]>),
    );
  }

  public async transactWriteItems(
    ...args: [DDB.TransactWriteItemsCommandInput, ...any]
  ): Promise<DDB.TransactWriteItemsCommandOutput> {
    return this.handler(
      'transactWriteItems',
      args,
      args[0].TransactItems!.reduce((acc, item) => {
        const obj: any = (item.Put ?? item.Update ?? item.Delete ?? item.ConditionCheck)!;
        const { TableName, Item, Key } = obj;
        if (!acc[TableName]) {
          acc[TableName] = [];
        }
        acc[TableName].push(Item ?? Key);
        return acc;
      }, {} as Record<string, Item[]>),
    );
  }

  private async handler<T extends BaseResponse>(
    method: keyof OriginDynamoDB,
    args: any[],
    reqItems?: Record<string, Item[]>,
  ): Promise<T> {
    if (!args[0].ReturnConsumedCapacity) {
      args[0].ReturnConsumedCapacity = 'TOTAL';
    }

    // @ts-expect-error
    const response = (await super[method](...args)) as T;
    if (response) {
      try {
        const Cap = response.ConsumedCapacity
          ? Array.isArray(response.ConsumedCapacity)
            ? response.ConsumedCapacity
            : [response.ConsumedCapacity]
          : undefined;
        if (!Cap) {
          return response;
        }

        Cap.forEach(async (cap) => {
          const tableName = cap.TableName;
          const capacityUnits = cap.CapacityUnits;
          const items =
            reqItems?.[tableName!] ??
            (response.Item
              ? [response.Item]
              : // eslint-disable-next-line unicorn/prefer-logical-operator-over-ternary
              response.Items
              ? response.Items
              : response.Attributes
              ? [response.Attributes]
              : response.Responses
              ? Array.isArray(response.Responses)
                ? response.Responses.map((r) => r.Item!)
                : Object.values(response.Responses).flatMap((r) => r.map((r) => r))
              : []);

          if (!(tableName && capacityUnits && items.length)) {
            return;
          }

          const key = this.keys[tableName];
          if (!key) {
            return;
          }

          await Promise.resolve(
            this.hook(method, capacityUnits, [
              ...new Set(items.map((i) => i[key]['S'] || i[key]['N'] || i[key]['B']?.toString() || '')),
            ]),
          ).catch((e) => console.warn('Error when get capacity.', e));
        });
      } catch (e) {
        console.warn('Error when get capacity.', e);
      }
    }

    return response;
  }
}

export interface DynamoDBConfig extends OriginDynamoDBClientConfig {
  hook?: (method: keyof OriginDynamoDB, capacityUnits: number, keys: string[]) => void | Promise<void>;
  keys?: Record<string, string>;
}

type BaseResponse = {
  ConsumedCapacity?: DDB.ConsumedCapacity | DDB.ConsumedCapacity[];
  Attributes?: Item;
  Item?: Item;
  Items?: Item[];
  Responses?: DDB.ItemResponse[] | Record<string, Item[]>;
  $metadata?: any;
};

type Item = Record<string, DDB.AttributeValue>;
