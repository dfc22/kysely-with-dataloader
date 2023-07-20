import { createId } from '@paralleldrive/cuid2'
import { ColumnNode } from '../../operation-node/column-node'
import { InsertQueryNode } from '../../operation-node/insert-query-node'
import { OperationNodeTransformer } from '../../operation-node/operation-node-transformer'
import { ValueListNode } from '../../operation-node/value-list-node'
import { ValueNode } from '../../operation-node/value-node'
import { ValuesNode } from '../../operation-node/values-node'
import { RootOperationNode } from '../../query-compiler/query-compiler'
import { UnknownRow } from '../../util/type-utils'
import {
  KyselyPlugin,
  PluginTransformQueryArgs,
  PluginTransformResultArgs,
} from '../kysely-plugin'
import { QueryResult } from '../../driver/database-connection'
import { UpdateQueryNode } from '../../operation-node/update-query-node'

class ReturningCuidTransformer extends OperationNodeTransformer {
  #callback: (id: string) => void

  constructor(callback: (id: string) => void) {
    super()
    this.#callback = callback
  }

  override transformInsertQuery(node: InsertQueryNode): InsertQueryNode {
    const values = node.values
    return {
      ...node,
      columns: [...(node.columns ?? []), ColumnNode.create('id')],
      values: values != null ? this.transformNode(values) : undefined,
      returning: undefined,
    }
  }

  override transformValues(node: ValuesNode): ValuesNode {
    return {
      ...node,
      values: node.values.map((v) => {
        const id = createId()
        this.#callback(id)
        return ValueListNode.is(v)
          ? {
              ...v,
              values: [...v.values, ValueNode.create(id)],
            }
          : {
              ...v,
              values: [...v.values, id],
            }
      }),
    }
  }
}

export class ReturningCuidPlugin implements KyselyPlugin {
  #idStore: {
    [key: string]: string[]
  }

  constructor() {
    this.#idStore = {}
  }

  transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
    return new ReturningCuidTransformer((id) => {
      if (!(args.queryId.queryId in this.#idStore)) {
        this.#idStore[args.queryId.queryId] = []
      }
      this.#idStore[args.queryId.queryId].push(id)
    }).transformNode(args.node)
  }

  async transformResult(
    args: PluginTransformResultArgs
  ): Promise<QueryResult<UnknownRow>> {
    if (args.queryId.queryId in this.#idStore) {
      const ids = this.#idStore[args.queryId.queryId]
      return {
        ...args.result,
        rows: ids.map((id) => ({ id })),
      }
    }
    return args.result
  }
}
