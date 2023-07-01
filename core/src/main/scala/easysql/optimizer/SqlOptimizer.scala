package easysql.optimizer

import easysql.ast.statement.SqlQuery

class SqlOptimizer(private val rules: List[OptimizeRule]) {
    def optimize(query: SqlQuery): SqlQuery = {
        var optimizedQuery = query

        for rule <- rules do {
            optimizedQuery = rule.optimize(optimizedQuery)
        }

        optimizedQuery
    }
}