package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.optimizer.conjunctiveTermList
import easysql.optimizer.hasAgg
import easysql.ast.expr.SqlBinaryExpr
import easysql.ast.expr.SqlBinaryOperator

class SimplifyHaving extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case s @ SqlSelect(param, select, from, where, groupBy, orderBy, forUpdate, limit, having) => {
            having match {
                case None => s
                case Some(h) => {
                    val havingList = conjunctiveTermList(h)
                    val aggList = havingList.filter(hasAgg)
                    val otherList = havingList.filter(!hasAgg(_))
                    val newHaving = 
                        if aggList.nonEmpty 
                        then Some(aggList.reduce((l, r) => SqlBinaryExpr(l, SqlBinaryOperator.And, r)))
                        else None
                    val condition =
                        if otherList.nonEmpty
                        then Some(otherList.reduce((l, r) => SqlBinaryExpr(l, SqlBinaryOperator.And, r)))
                        else None
                    val newWhere = {
                        for {
                            w <- where
                            cond <- condition
                        } yield SqlBinaryExpr(w, SqlBinaryOperator.And, cond)
                    }.orElse(condition)
                    
                    SqlSelect(param, select, from, newWhere, groupBy, orderBy, forUpdate, limit, newHaving)
                }
            }
        }
        case SqlUnion(left, unionType, right) => 
            SqlUnion(optimize(left), unionType, optimize(right))
        case SqlValues(values) => 
            SqlValues(values)
        case SqlWith(withList, recursive, query) =>
            SqlWith(withList.map(i => SqlWithItem(i.name, optimize(i.query), i.columns)), recursive, query.map(optimize))
    } 
}