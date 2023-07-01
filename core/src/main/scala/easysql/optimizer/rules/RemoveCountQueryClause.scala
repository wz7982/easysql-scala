package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.ast.expr.SqlAggFuncExpr
import easysql.ast.table.SqlSubQueryTable

class RemoveCountQueryClause extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case s @ SqlSelect(_, SqlSelectItem(SqlAggFuncExpr("COUNT", _, _, _, _), _) :: Nil, Some(SqlSubQueryTable(subQuery: SqlSelect, false, alias)), _, Nil, _, _, _, _) => {
            val optimizedSubQuery = subQuery match {
                case SqlSelect(_, _, _, _, Nil, _, _, None, _) =>
                    subQuery.copy(groupBy = Nil, orderBy = Nil, having = None, limit = None)
                case _ => subQuery
            }
            
            s.copy(from = Some(SqlSubQueryTable(optimizedSubQuery, false, alias)), groupBy = Nil, orderBy = Nil, having = None, limit = None)
        }
        case s @ SqlSelect(_, _, _, _, Nil, _, _, _, _) => 
            s.copy(groupBy = Nil, orderBy = Nil, having = None, limit = None)
        case s: SqlSelect =>
            s
        case SqlUnion(left, unionType, right) => 
            SqlUnion(optimize(left), unionType, optimize(right))
        case SqlValues(values) => 
            SqlValues(values)
        case SqlWith(withList, recursive, query) =>
            SqlWith(withList.map(i => SqlWithItem(i.name, optimize(i.query), i.columns)), recursive, query.map(optimize))
    }
}