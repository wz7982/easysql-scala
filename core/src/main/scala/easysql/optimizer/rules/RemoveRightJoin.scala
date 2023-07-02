package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.ast.expr.*
import easysql.ast.order.SqlOrderBy

class RemoveRightJoin extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case SqlSelect(param, select, from, where, groupBy, orderBy, forUpdate, limit, having) => {
            val optimizedSelect = select.map(i => SqlSelectItem(removeExprRightJoin(i.expr), i.alias))
            val optimizedFrom = from.map(removeRightJoin)
            val optimizedWhere = where.map(removeExprRightJoin)
            val optimizedGroupBy = groupBy.map(removeExprRightJoin)
            val optimizedOrderBy = orderBy.map(o => SqlOrderBy(removeExprRightJoin(o.expr), o.order))
            val optimizedHaving = having.map(removeExprRightJoin)
            SqlSelect(param, optimizedSelect, optimizedFrom, optimizedWhere, optimizedGroupBy, optimizedOrderBy, forUpdate, limit, optimizedHaving)
        }
        case SqlUnion(left, unionType, right) => 
            SqlUnion(optimize(left), unionType, optimize(right))
        case SqlValues(values) => 
            SqlValues(values)
        case SqlWith(withList, recursive, query) =>
            SqlWith(withList.map(i => SqlWithItem(i.name, optimize(i.query), i.columns)), recursive, query.map(optimize))
    }

    def removeRightJoin(table: SqlTable): SqlTable = table match {
        case SqlJoinTable(left, SqlJoinType.RightJoin, right, on) =>
            SqlJoinTable(removeRightJoin(right), SqlJoinType.LeftJoin, removeRightJoin(left), on)
        case SqlJoinTable(left, joinType, right, on) => 
            SqlJoinTable(removeRightJoin(left), joinType, removeRightJoin(right), on)
        case _ => 
            table
    }

    def removeExprRightJoin(expr: SqlExpr): SqlExpr = expr match {
        case SqlQueryExpr(query) =>
            SqlQueryExpr(optimize(query))
        case SqlBinaryExpr(left, op, right) =>
            SqlBinaryExpr(removeExprRightJoin(left), op, removeExprRightJoin(right))
        case SqlAggFuncExpr(name, args, distinct, attrs, orderBy) => 
            SqlAggFuncExpr(
                name, 
                args.map(removeExprRightJoin(_)), 
                distinct, 
                attrs.map((k, v) => k -> removeExprRightJoin(v)),
                orderBy.map(o => SqlOrderBy(removeExprRightJoin(o.expr) ,o.order))
            )
        case SqlBetweenExpr(expr, start, end, not) => 
            SqlBetweenExpr(removeExprRightJoin(expr), removeExprRightJoin(start), removeExprRightJoin(end), not)
        case SqlInExpr(expr, inExpr, not) =>
            SqlInExpr(removeExprRightJoin(expr), removeExprRightJoin(inExpr), not)
        case SqlOverExpr(agg, partitionBy, orderBy, between) =>
            SqlOverExpr(
                removeExprRightJoin(agg).asInstanceOf[SqlAggFuncExpr], 
                partitionBy.map(removeExprRightJoin(_)),
                orderBy.map(o => SqlOrderBy(removeExprRightJoin(o.expr) ,o.order)),
                between
            )
        case SqlListExpr(items) =>
            SqlListExpr(items.map(removeExprRightJoin(_)))
        case SqlExprFuncExpr(name, args) =>
            SqlExprFuncExpr(name, args.map(removeExprRightJoin(_)))
        case _ => 
            expr
    }
}