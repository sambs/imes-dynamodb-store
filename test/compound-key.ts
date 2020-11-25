import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { DynamoDBStore, exactFilters, ordFilters } from '../src'
import { Query, ExactFilter, OrdFilter } from 'imes'

jest.mock('aws-sdk/clients/dynamodb')

const client = {} as jest.Mocked<DocumentClient>

const store = new DynamoDBStore<User, UserQuery, 'teamId', 'id'>({
  client,
  filters: {
    name: exactFilters('name'),
    age: ordFilters('age'),
  },
  partitionKey: 'teamId',
  sortKey: 'id',
  tableName: 'users',
})

const commonQueryParams = {
  TableName: 'users',
}

interface UserData {
  age: number | null
  id: string
  name: string
  teamId: string
}

interface UserMeta {
  createdAt: string
}

type User = UserData & UserMeta

interface UserQuery extends Query {
  filter?: {
    name?: ExactFilter<string>
    age?: OrdFilter<number>
  }
}

const user1: User = {
  age: 47,
  createdAt: 'yesterday',
  id: 'u1',
  name: 'Trevor',
  teamId: 't1',
}

const user2: User = {
  age: 15,
  createdAt: 'today',
  id: 'u2',
  name: 'Whatever',
  teamId: 't2',
}

const user3: User = {
  age: null,
  createdAt: 'now',
  id: 'u3',
  name: 'Eternal',
  teamId: 't1',
}

const cursors = {
  u1: 'eyJ0ZWFtSWQiOiJ0MSIsImlkIjoidTEifQ==',
  u2: 'eyJ0ZWFtSWQiOiJ0MiIsImlkIjoidTIifQ==',
  u3: 'eyJ0ZWFtSWQiOiJ0MSIsImlkIjoidTMifQ==',
}

test('DynamoDBStore#put', async () => {
  client.put = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({}),
  })) as any

  await store.put(user1)

  expect(client.put).toHaveBeenCalledWith({
    ...commonQueryParams,
    Item: user1,
  })
})

test('DynamoDBStore#get', async () => {
  client.get = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Item: user1 }),
  })) as any

  expect(await store.get({ teamId: 't1', id: 'u1' })).toEqual(user1)

  expect(client.get).toHaveBeenCalledWith({
    ...commonQueryParams,
    Key: { teamId: 't1', id: 'u1' },
  })
})

test('DynamoDBStore#get a non existant key', async () => {
  client.get = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({}),
  })) as any

  expect(await store.get({ teamId: 't1', id: 'dne' })).toEqual(undefined)
})

test('DynamoDBStore#find', async () => {
  client.scan = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Items: [user1, user2, user3] }),
  })) as any

  expect(await store.find({})).toEqual({
    cursor: null,
    edges: [
      { cursor: cursors.u1, node: user1 },
      { cursor: cursors.u2, node: user2 },
      { cursor: cursors.u3, node: user3 },
    ],
    items: [user1, user2, user3],
  })

  expect(client.scan).toHaveBeenCalledWith({
    ...commonQueryParams,
  })
})

test('DynamoDBStore#find with limit', async () => {
  client.scan = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({
      Items: [user1, user2],
      LastEvaluatedKey: { teamId: 't2', id: 'u2' },
    }),
  })) as any

  expect(await store.find({ limit: 2 })).toEqual({
    cursor: cursors.u2,
    edges: [
      { cursor: cursors.u1, node: user1 },
      { cursor: cursors.u2, node: user2 },
    ],
    items: [user1, user2],
  })

  expect(client.scan).toHaveBeenCalledWith({
    ...commonQueryParams,
    Limit: 2,
  })
})

test('DynamoDBStore#find with limit and cursor', async () => {
  client.scan = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Items: [user2, user3] }),
  })) as any

  expect(await store.find({ cursor: cursors.u1, limit: 2 })).toEqual({
    cursor: null,
    edges: [
      { cursor: cursors.u2, node: user2 },
      { cursor: cursors.u3, node: user3 },
    ],
    items: [user2, user3],
  })

  expect(client.scan).toHaveBeenCalledWith({
    ...commonQueryParams,
    Limit: 2,
    ExclusiveStartKey: { teamId: 't1', id: 'u1' },
  })
})

test('DynamoDBStore#find with eq filter', async () => {
  client.scan = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Items: [user3] }),
  })) as any

  expect(await store.find({ filter: { name: { eq: 'Eternal' } } })).toEqual({
    cursor: null,
    edges: [{ cursor: cursors.u3, node: user3 }],
    items: [user3],
  })

  expect(client.scan).toHaveBeenCalledWith({
    ...commonQueryParams,
    FilterExpression: '#name = :name_eq',
    ExpressionAttributeNames: {
      '#name': 'name',
    },
    ExpressionAttributeValues: {
      ':name_eq': 'Eternal',
    },
  })
})

test('DynamoDBStore#find with multiple filters', async () => {
  client.scan = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Items: [user1] }),
  })) as any

  expect(await store.find({ filter: { age: { gte: 40, lt: 60 } } })).toEqual({
    cursor: null,
    edges: [{ cursor: cursors.u1, node: user1 }],
    items: [user1],
  })

  expect(client.scan).toHaveBeenCalledWith({
    ...commonQueryParams,
    FilterExpression: '#age < :age_lt and #age >= :age_gte',
    ExpressionAttributeNames: {
      '#age': 'age',
    },
    ExpressionAttributeValues: {
      ':age_gte': 40,
      ':age_lt': 60,
    },
  })
})
