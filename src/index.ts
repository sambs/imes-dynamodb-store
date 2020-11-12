import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { Query, Store } from 'imes'

export interface DynamoDBStoreOptions<I extends {}, K> {
  client: DocumentClient
  getItemKey: (item: I) => any
  serializeKey: (key: K) => any
  tableName: string
}

export class DynamoDBStore<I extends {}, K, Q extends Query>
  implements Store<I, K, Q> {
  client: DocumentClient
  getItemKey: (item: I) => any
  serializeKey: (key: K) => any
  tableName: string

  constructor({
    client,
    getItemKey,
    serializeKey,
    tableName,
  }: DynamoDBStoreOptions<I, K>) {
    this.client = client
    this.getItemKey = getItemKey
    this.serializeKey = serializeKey
    this.tableName = tableName
  }

  serializeItem(item: I): any {
    return item
  }

  deserializeItem(item: any): I {
    return item
  }

  async get(key: K) {
    return this.client
      .get({
        TableName: this.tableName,
        Key: this.serializeKey(key),
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

  cursorToKey(cursor: string): K {
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
