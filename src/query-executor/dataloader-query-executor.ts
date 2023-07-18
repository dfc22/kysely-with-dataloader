import { QueryResult } from '../driver/database-connection'
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
  #compiler: QueryCompiler
  #batches: Batch = {}
  #tickActive = false
  #tickId = createId()

  constructor(compiler: QueryCompiler, plugins: KyselyPlugin[] = []) {
    super(plugins)

    this.#compiler = compiler
  }

  get adapter(): DialectAdapter {
    throw new Error('this query cannot be compiled to SQL')
  }

  private _getQueryHash(node: SelectQueryNode) {
    const compiledQuery = this.#compiler.compileQuery(node)
    return `${crypto
      .createHash('sha256')
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

              const batchNode = {
                ...node,
                where: combinedWhere,
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
      const node = compiledQuery.query as SelectQueryNode
      const where = job.where
      // TODO: 引いてきたクエリ結果を各々のwhere条件で絞り込む

      return result
    }
    return super.executeQuery<R>(compiledQuery, queryId)
  }

  provideConnection<T>(): Promise<T> {
    throw new Error('this query cannot be executed')
  }

  withConnectionProvider(): DataloaderQueryExecutor {
    throw new Error('this query cannot have a connection provider')
  }

  withPlugin(plugin: KyselyPlugin): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(this.#compiler, [
      ...this.plugins,
      plugin,
    ])
  }

  withPlugins(plugins: ReadonlyArray<KyselyPlugin>): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(this.#compiler, [
      ...this.plugins,
      ...plugins,
    ])
  }

  withPluginAtFront(plugin: KyselyPlugin): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(this.#compiler, [
      plugin,
      ...this.plugins,
    ])
  }

  withoutPlugins(): DataloaderQueryExecutor {
    return new DataloaderQueryExecutor(this.#compiler, [])
  }
}
