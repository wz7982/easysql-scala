package easysql.optimizer

import easysql.ast.statement.SqlQuery

trait OptimizeRule {
    def optimize(query: SqlQuery): SqlQuery
}