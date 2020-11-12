import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Query, Store } from 'imes'

export interface DynamoDBStoreOptions<
  I extends {},
  PK extends keyof I,
  SK extends keyof I | undefined = undefined
> {
  client: DocumentClient
  partitionKey: PK
  sortKey: SK
  tableName: string
}

export type DynamoDBStoreKey<
  I extends {},
  PK extends keyof I,
  SK extends keyof I | undefined = undefined
> = SK extends undefined ? I[PK] : Pick<I, PK | (SK & string)>

export class DynamoDBStore<
  I extends {},
  Q extends Query,
  PK extends keyof I,
  SK extends keyof I | undefined = undefined
> implements Store<I, DynamoDBStoreKey<I, PK, SK>, Q> {
  client: DocumentClient
  partitionKey: PK
  sortKey: SK
  tableName: string

  constructor({
    client,
    partitionKey,
    sortKey,
    tableName,
  }: DynamoDBStoreOptions<I, PK, SK>) {
    this.client = client
    this.partitionKey = partitionKey
    this.sortKey = sortKey
    this.tableName = tableName
  }

  serializeItem(item: I): any {
    return item
  }

  deserializeItem(item: any): I {
    return item
  }

  async get(key: DynamoDBStoreKey<I, PK, SK>) {
    return this.client
      .get({
        TableName: this.tableName,
        Key: this.sortKey ? key : { [this.partitionKey]: key },
      })
      .promise()
      .then(({ Item }) => {
        if (Item) return this.deserializeItem(Item)
      })
  }

  async create(item: I) {
    return this.update(item)
  }

  async update(item: I) {
    return this.client
      .put({
        TableName: this.tableName,
        Item: this.serializeItem(item),
      })
      .promise()
      .then(() => undefined)
  }

  getItemKey(item: I) {
    return (this.sortKey
      ? {
          [this.partitionKey]: item[this.partitionKey],
          [this.sortKey as string]: item[this.sortKey as keyof I],
        }
      : {
          [this.partitionKey]: item[this.partitionKey],
        }) as DynamoDBStoreKey<I, PK, SK>
  }

  cursorToKey(cursor: string): DynamoDBStoreKey<I, PK, SK> {
    try {
      return JSON.parse(Buffer.from(cursor, 'base64').toString('utf8'))
    } catch (error) {
      throw new Error(`Invalid cursor: ${cursor}`)
    }
  }

  keyToCursor(key: any): string {
    return Buffer.from(JSON.stringify(key)).toString('base64')
  }

  async find({ limit, cursor }: Q) {
    const ExclusiveStartKey = cursor ? this.cursorToKey(cursor) : undefined

    const { Items = [], LastEvaluatedKey } = await this.client
      .scan({
        TableName: this.tableName,
        Limit: limit,
        ExclusiveStartKey,
      })
      .promise()

    const items = Items.map(this.deserializeItem)
    const edges = items.map(node => ({
      node,
      cursor: this.keyToCursor(this.getItemKey(node)),
    }))
    cursor = LastEvaluatedKey ? this.keyToCursor(LastEvaluatedKey) : null

    return { cursor, edges, items }
  }

  async clear() {}

  async setup() {}

  async teardown() {}
}
