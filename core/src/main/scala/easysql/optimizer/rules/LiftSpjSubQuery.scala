package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.optimizer.isSpjQuery
import easysql.optimizer.replaceTableName

class LiftSpjSubQuery extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case s @ SqlSelect(_, _, Some(table), _, _, _, _, _, _) =>
            liftSqjSubQuery(s, table)
        case _ => query
    }

    // todo
    def liftSqjSubQuery(outerQuery: SqlSelect, table: SqlTable): SqlSelect = table match {
        case t @ SqlSubQueryTable(query @ SqlSelect(_, _, Some(subTable), _, _, _, _, _, _), false, alias) if isSpjQuery(query) => {
            val from = query.from.map {
                case SqlIdentTable(tableName, _) =>
                    SqlIdentTable(tableName, alias)
                case t => t
            }
            val where = alias match {
                case None => query.where
                case Some(name) => query.where.map(replaceTableName(_, name))
            }
            SqlSelect(outerQuery.param, outerQuery.select, from, where, Nil, Nil, false, None, None)
        }
        case _ => 
            outerQuery
    }
}