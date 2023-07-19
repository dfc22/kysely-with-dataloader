import { DatabaseConnection, QueryResult } from '../driver/database-connection'
import { OrNode } from '../operation-node/or-node'
import { SelectQueryNode } from '../operation-node/select-query-node'
import { WhereNode } from '../operation-node/where-node'
import { CompiledQuery } from '../query-compiler/compiled-query'
import {
  QueryCompiler,
  RootOperationNode,
} from '../query-compiler/query-compiler'
import { UnknownRow } from '../util/type-utils'
import * as crypto from 'crypto'
import { createId } from '@paralleldrive/cuid2'
import { QueryId } from '../util/query-id'
import { QueryExecutorBase } from './query-executor-base'
import { DialectAdapter } from '../dialect/dialect-adapter'
import { KyselyPlugin } from '../plugin/kysely-plugin'
import { ConnectionProvider } from '../driver/connection-provider'
import {
  BinaryOperator,
  JSComparableOperator,
  JS_COMPARABLE_OPERATORS,
  OperatorNode,
} from '../operation-node/operator-node'
import { AndNode } from '../operation-node/and-node'
import { OperationNode } from '../operation-node/operation-node'
import { BinaryOperationNode } from '../operation-node/binary-operation-node'
import { ReferenceNode } from '../operation-node/reference-node'
import { ColumnNode } from '../operation-node/column-node'
import { ValueNode } from '../operation-node/value-node'

const compareWithJS = <R extends UnknownRow>(
  row: R,
  node: BinaryOperationNode
): boolean => {
  const columnName = ReferenceNode.is(node.leftOperand)
    ? ColumnNode.is(node.leftOperand.column)
      ? node.leftOperand.column.column.name
      : undefined
    : ColumnNode.is(node.leftOperand)
    ? node.leftOperand.column.name
    : undefined

  if (columnName == null) {
    throw new Error('columnName is null')
  }

  if (!(columnName in row)) {
    throw new Error(`Column "${columnName}" does not exist in the row.`)
  }

  const operator = OperatorNode.is(node.operator)
    ? node.operator.operator
    : undefined

  if (operator == null) {
    throw new Error('operator is null')
  }

  if (!(operator in JS_COMPARABLE_OPERATORS)) {
    throw new Error(
      `Operator "${operator}" is not a valid js comparable operator.`
    )
  }

  const value = ValueNode.is(node.rightOperand)
    ? node.rightOperand.value
    : undefined

  if (value == null) {
    throw new Error('value is null')
  }

  const jsComparableOperator = operator as JSComparableOperator

  const rowValue = row[columnName]

  // Perform comparison based on the provided operator.
  switch (jsComparableOperator) {
    case '=':
    case '==':
      return rowValue === value
    case '!=':
    case '<>':
      return rowValue !== value
    case '>':
      if (typeof rowValue === 'number' || rowValue instanceof Date) {
        return rowValue > value
      }
      throw new Error(
        `Unsupported type for jsComparableOperator "${jsComparableOperator}"`
      )
    case '>=':
      if (typeof rowValue === 'number' || rowValue instanceof Date) {
        return rowValue >= value
      }
      throw new Error(
        `Unsupported type for jsComparableOperator "${jsComparableOperator}"`
      )
    case '<':
      if (typeof rowValue === 'number' || rowValue instanceof Date) {
        return rowValue < value
      }
      throw new Error(
        `Unsupported type for jsComparableOperator "${jsComparableOperator}"`
      )
    case '<=':
      if (typeof rowValue === 'number' || rowValue instanceof Date) {
        return rowValue <= value
      }
      throw new Error(
        `Unsupported type for jsComparableOperator "${jsComparableOperator}"`
      )
    case 'in':
      if (!Array.isArray(value)) {
        throw new Error('IN can only be used with array values.')
      }
      return value.includes(rowValue)
    case 'not in':
      if (!Array.isArray(value)) {
        throw new Error('NOT IN can only be used with array values.')
      }
      return !value.includes(rowValue)
    case 'is':
      return rowValue === value
    case 'is not':
      return rowValue !== value
    case 'like':
    case 'not like':
    case 'match':
    case 'ilike':
    case 'not ilike':
      if (typeof rowValue !== 'string' || typeof value !== 'string') {
        throw new Error('LIKE and ILIKE can only be used with string values.')
      }
      const regex = new RegExp(
        `^${value.replace(/%/g, '.*')}$`,
        jsComparableOperator === 'ilike' || jsComparableOperator === 'not ilike'
          ? 'i'
          : ''
      )
      const result = regex.test(rowValue)
      return jsComparableOperator.startsWith('not') ? !result : result
    default:
      throw new Error(
        `Comparison for jsComparableOperator "${jsComparableOperator}" not implemented.`
      )
  }
}

const getQueriedRowsFlags = <R extends UnknownRow>(
  rows: R[],
  hitFlags: boolean[],
  where: AndNode | OrNode | BinaryOperationNode
): boolean[] => {
  if (AndNode.is(where)) {
    if (
      (AndNode.is(where.left) ||
        OrNode.is(where.left) ||
        BinaryOperationNode.is(where.left)) &&
      (AndNode.is(where.right) ||
        OrNode.is(where.right) ||
        BinaryOperationNode.is(where.right))
    ) {
      const leftResult = getQueriedRowsFlags(rows, hitFlags, where.left)
      const rightResult = getQueriedRowsFlags(rows, hitFlags, where.right)

      return leftResult.map((v, i) => v && rightResult[i])
    }
  }

  if (OrNode.is(where)) {
    if (
      (AndNode.is(where.left) ||
        OrNode.is(where.left) ||
        BinaryOperationNode.is(where.left)) &&
      (AndNode.is(where.right) ||
        OrNode.is(where.right) ||
        BinaryOperationNode.is(where.right))
    ) {
      const leftResult = getQueriedRowsFlags(rows, hitFlags, where.left)
      const rightResult = getQueriedRowsFlags(rows, hitFlags, where.right)

      return leftResult.map((v, i) => v || rightResult[i])
    }
  }

  if (BinaryOperationNode.is(where)) {
    const flags = rows.map((v) => compareWithJS(v, where))
    return hitFlags.map((v, i) => v || flags[i])
  }

  throw new Error('where is not AndNode or OrNode or BinaryOperationNode')
}

const getQueriedRows = <R extends UnknownRow>(
  rows: R[],
  where: WhereNode
): R[] => {
  if (
    AndNode.is(where.where) ||
    OrNode.is(where.where) ||
    BinaryOperationNode.is(where.where)
  ) {
    const flags = getQueriedRowsFlags(
      rows,
      rows.map((_) => false),
      where.where
    )

    return rows.filter((_, i) => flags[i])
  }

  throw new Error('where.where is not AndNode or OrNode or BinaryOperationNode')
}

const getHash = (node: OperationNode): string => {
  if (AndNode.is(node)) {
    return `${getHash(node.left)}&${getHash(node.right)}`
  }

  if (OrNode.is(node)) {
    return `${getHash(node.left)}|${getHash(node.right)}`
  }

  return JSON.stringify(node)
}

const isEqualNode = (a: OperationNode, b: OperationNode): boolean => {
  return getHash(a) === getHash(b)
}

const refactorWhere = (node: OperationNode): OperationNode => {
  if (AndNode.is(node)) {
    const left = refactorWhere(node.left)
    const right = refactorWhere(node.right)

    if (isEqualNode(left, right)) {
      return left
    }

    if (OrNode.is(left) && OrNode.is(right)) {
      const leftLeftHash = getHash(left.left)
      const leftRightHash = getHash(left.right)
      const rightLeftHash = getHash(right.left)
      const rightRightHash = getHash(right.right)
      if (leftLeftHash === rightLeftHash) {
        return OrNode.create(left.left, AndNode.create(left.right, right.right))
      }
      if (leftLeftHash === rightRightHash) {
        return OrNode.create(left.left, AndNode.create(left.right, right.left))
      }
      if (leftRightHash === rightLeftHash) {
        return OrNode.create(left.right, AndNode.create(left.left, right.right))
      }
      if (leftRightHash === rightRightHash) {
        return OrNode.create(left.right, AndNode.create(left.left, right.left))
      }
    }
  } else if (OrNode.is(node)) {
    const left = refactorWhere(node.left)
    const right = refactorWhere(node.right)

    if (isEqualNode(left, right)) {
      return left
    }

    if (AndNode.is(left) && AndNode.is(right)) {
      const leftLeftHash = getHash(left.left)
      const leftRightHash = getHash(left.right)
      const rightLeftHash = getHash(right.left)
      const rightRightHash = getHash(right.right)
      if (leftLeftHash === rightLeftHash) {
        return AndNode.create(left.left, OrNode.create(left.right, right.right))
      }
      if (leftLeftHash === rightRightHash) {
        return AndNode.create(left.left, OrNode.create(left.right, right.left))
      }
      if (leftRightHash === rightLeftHash) {
        return AndNode.create(left.right, OrNode.create(left.left, right.right))
      }
      if (leftRightHash === rightRightHash) {
        return AndNode.create(left.right, OrNode.create(left.left, right.left))
      }
    }
  } else if (WhereNode.is(node)) {
    return WhereNode.create(refactorWhere(node.where))
  }

  return node
}

type Job = {
  queryId: QueryId
  where?: WhereNode
}

type Batch = {
  [hash: string]: {
    jobs: Job[]
    result: Promise<QueryResult<UnknownRow>>
  }
}

export class DataloaderQueryExecutor extends QueryExecutorBase {
  static #instance: DataloaderQueryExecutor
  #compiler: QueryCompiler
  #connectionProvider: ConnectionProvider
  #batches: Batch = {}
  #tickActive = false
  #tickId = createId()

  private constructor(
    compiler: QueryCompiler,
    connectionProvider: ConnectionProvider,
    plugins: KyselyPlugin[] = []
  ) {
    super(plugins)

    this.#compiler = compiler
    this.#connectionProvider = connectionProvider
  }

  static create(
    compiler: QueryCompiler,
    connectionProvider: ConnectionProvider,
    plugins: KyselyPlugin[] = []
  ): DataloaderQueryExecutor {
    if (this.#instance == null) {
      this.#instance = new DataloaderQueryExecutor(
        compiler,
        connectionProvider,
        plugins
      )
    }

    return this.#instance
  }

  get adapter(): DialectAdapter {
    throw new Error('this query cannot be compiled to SQL')
  }

  private _getQueryHash(node: SelectQueryNode) {
    const compiledQuery = this.#compiler.compileQuery(node)
    return `${crypto
      .createHash('sha1')
      .update(compiledQuery.sql)
      .digest('hex')}-${this.#tickId}`
  }

  compileQuery(node: RootOperationNode, queryId: QueryId): CompiledQuery {
    if (SelectQueryNode.is(node)) {
      const where = node.where
      const queryHash = this._getQueryHash(node)

      if (this.#batches[queryHash] == null) {
        this.#batches[queryHash] = {
          jobs: [],
          result: new Promise((resolve) =>
            resolve({
              rows: [],
            })
          ),
        }
        this.#batches[queryHash].jobs = []

        const batch = this.#batches[queryHash]

        if (!this.#tickActive) {
          this.#tickActive = true
          process.nextTick(() => {
            this.#tickActive = false

            if (batch.jobs.length > 1) {
              const combinedWhere = batch.jobs
                .map((v) => v.where)
                .reduce((acc, where) =>
                  acc == null
                    ? where
                    : where == null
                    ? undefined
                    : WhereNode.create(OrNode.create(acc.where, where.where))
                )

              const refactoredWhere =
                combinedWhere != null
                  ? (refactorWhere(combinedWhere) as WhereNode)
                  : undefined

              const batchNode = {
                ...node,
                where: refactoredWhere,
              }

              const compiledQuery = this.#compiler.compileQuery(batchNode)

              batch.result = super.executeQuery<UnknownRow>(
                compiledQuery,
                queryId
              )
            }
          })
        }
      }

      this.#batches[queryHash].jobs.push({
        queryId: queryId,
        where,
      })
    }

    return this.#compiler.compileQuery(node)
  }

  async executeQuery<R>(
    compiledQuery: CompiledQuery,
    queryId: QueryId
  ): Promise<QueryResult<R>> {
    const batch = Object.values(this.#batches).find((v) =>
      v.jobs.find((v) => v.queryId.queryId == queryId.queryId)
    )
    const job = batch?.jobs.find((v) => v.queryId.queryId == queryId.queryId)

    if (batch != null && job != null) {
      const result = (await batch.result) as QueryResult<R>
      const where = job.where
      const rows =
        where == null
          ? result.rows
          : (getQueriedRows(result.rows as UnknownRow[], where) as R[])

      return {
        rows,
      }
    }
    return this.executeQuery<R>(compiledQuery, queryId)
  }

  provideConnection<T>(
    consumer: (connection: DatabaseConnection) => Promise<T>
  ): Promise<T> {
    return this.#connectionProvider.provideConnection(consumer)
  }

  withConnectionProvider(): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(
      this.#compiler,
      this.#connectionProvider,
      [...this.plugins]
    )
  }

  withPlugin(plugin: KyselyPlugin): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(
      this.#compiler,
      this.#connectionProvider,
      [...this.plugins, plugin]
    )
  }

  withPlugins(plugins: ReadonlyArray<KyselyPlugin>): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(
      this.#compiler,
      this.#connectionProvider,
      [...this.plugins, ...plugins]
    )
  }

  withPluginAtFront(plugin: KyselyPlugin): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(
      this.#compiler,
      this.#connectionProvider,
      [plugin, ...this.plugins]
    )
  }

  withoutPlugins(): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(
      this.#compiler,
      this.#connectionProvider,
      []
    )
  }
}
