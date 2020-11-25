import { Query, QueryFilterField, Store } from 'imes'
import { DocumentClient } from 'aws-sdk/clients/dynamodb'

export type DynamoDBFilters<Q extends Query, F = Required<Q['filter']>> = {
  [name in keyof F]: DynamoDBFilterField<F[name]>
}

export type DynamoDBFilterField<F extends QueryFilterField> = {
  [comparator in keyof Required<F>]: DynamoDBFilterClause<
    Exclude<F[comparator], undefined>
  >
}

export type DynamoDBFilterClause<T> = (
  value: T
) => {
  expression: string
  values: DocumentClient.ExpressionAttributeValueMap
  names: DocumentClient.ExpressionAttributeNameMap
}

export interface DynamoDBStoreOptions<
  I extends {},
  Q extends Query,
  PK extends keyof I,
  SK extends keyof I | undefined = undefined
> {
  client: DocumentClient
  filters: DynamoDBFilters<Q>
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
> extends Store<I, DynamoDBStoreKey<I, PK, SK>, Q> {
  client: DocumentClient
  filters: DynamoDBFilters<Q>
  partitionKey: PK
  sortKey: SK
  tableName: string

  constructor({
    client,
    filters,
    partitionKey,
    sortKey,
    tableName,
  }: DynamoDBStoreOptions<I, Q, PK, SK>) {
    super()
    this.client = client
    this.filters = filters
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

  async put(item: I) {
    return this.client
      .put({
        TableName: this.tableName,
        Item: this.serializeItem(item),
      })
      .promise()
      .then(() => undefined)
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

  async find(query: Q) {
    let expressions: Array<string> = []
    let values: DocumentClient.ExpressionAttributeValueMap = {}
    let names: DocumentClient.ExpressionAttributeNameMap = {}

    if (query.filter) {
      for (const field in this.filters) {
        if (query.filter[field]) {
          for (const comparator in this.filters[field]) {
            if (query.filter[field]![comparator] !== undefined) {
              const filterValue = query.filter[field]![comparator]
              const params = this.filters[field][comparator](filterValue)
              expressions.push(params.expression)
              Object.assign(values, params.values)
              Object.assign(names, params.names)
            }
          }
        }
      }
    }

    let params: DocumentClient.ScanInput = {
      TableName: this.tableName,
      Limit: query.limit,
    }

    if (typeof query.cursor == 'string') {
      params.ExclusiveStartKey = this.cursorToKey(query.cursor)
    }

    if (expressions.length) {
      params.FilterExpression = expressions.join(' and ')
    }

    if (Object.keys(values).length) {
      params.ExpressionAttributeValues = values
    }

    if (Object.keys(names).length) {
      params.ExpressionAttributeNames = names
    }

    const { Items = [], LastEvaluatedKey } = await this.client
      .scan(params)
      .promise()

    const cursor = LastEvaluatedKey ? this.keyToCursor(LastEvaluatedKey) : null
    const items = Items.map(this.deserializeItem)
    const edges = items.map(node => ({
      node,
      cursor: this.keyToCursor(this.getItemKey(node)),
    }))

    return { cursor, edges, items }
  }

  async clear() {}

  async setup() {}

  async teardown() {}
}

export * from './filters'
