package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.optimizer.isSpjQuery

class LiftSpjSubQuery extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case s @ SqlSelect(_, _, Some(table), _, _, _, _, _, _) =>
            liftSqjSubQuery(s, table)
        case _ => query
    }

    def liftSqjSubQuery(outerQuery: SqlSelect, table: SqlTable): SqlSelect = table match {
        case t @ SqlSubQueryTable(query: SqlSelect, false, alias) if isSpjQuery(query) => {
            val from = query.from.map {
                case SqlIdentTable(tableName, _) =>
                    SqlIdentTable(tableName, alias)
                case t => t
            }
            SqlSelect(query.param, query.select, from, query.where, Nil, Nil, false, None, None)
        }
        case _ => 
            outerQuery
    }
}