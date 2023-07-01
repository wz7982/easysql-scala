package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.SqlQuery

class RemoveUselessJoinTerm extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery =
        query
}