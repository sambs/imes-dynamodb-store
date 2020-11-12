import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { DynamoDBStore } from '../src'
import { Query } from 'imes'

jest.mock('aws-sdk/clients/dynamodb')

const client = {} as jest.Mocked<DocumentClient>

const store = new DynamoDBStore<User, Query, 'teamId', 'id'>({
  client,
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

test('AuroraPostgresStore#create', async () => {
  client.put = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({}),
  })) as any

  await store.create(user1)

  expect(client.put).toHaveBeenCalledWith({
    ...commonQueryParams,
    Item: user1,
  })
})

test('AuroraPostgresStore#update', async () => {
  client.put = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({}),
  })) as any

  await store.update(user1)

  expect(client.put).toHaveBeenCalledWith({
    ...commonQueryParams,
    Item: user1,
  })
})

test('AuroraPostgresStore#get', async () => {
  client.get = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ Item: user1 }),
  })) as any

  expect(await store.get({ teamId: 't1', id: 'u1' })).toEqual(user1)

  expect(client.get).toHaveBeenCalledWith({
    ...commonQueryParams,
    Key: { teamId: 't1', id: 'u1' },
  })
})

test('AuroraPostgresStore#get a non existant key', async () => {
  client.get = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({}),
  })) as any

  expect(await store.get({ teamId: 't1', id: 'dne' })).toEqual(undefined)
})

test('AuroraPostgresStore#find', async () => {
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

test('AuroraPostgresStore#find with limit', async () => {
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

test('AuroraPostgresStore#find with limit and cursor', async () => {
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
