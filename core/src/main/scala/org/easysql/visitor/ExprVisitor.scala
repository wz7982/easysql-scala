package org.easysql.visitor

import org.easysql.ast.expr.*
import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.order.SqlOrderBy
import org.easysql.dsl.*
import org.easysql.query.select.SelectQuery

import java.util.Date

def visitExpr(query: Expr[_, _] | Null): SqlExpr = {
    query match {
        case null =>
            SqlNullExpr()
        case ConstExpr(value) =>
            getExpr(value)
        case BinaryExpr(left, op, right) =>
            SqlBinaryExpr(visitExpr(left), op, visitExpr(right))
        case column: ColumnExpr[_] =>
            visitColumnExpr(column)
        case tableColumn: TableColumnExpr[_, _] =>
            SqlPropertyExpr(tableColumn.table, tableColumn.column)
        case primaryKeyColumnExpr: PrimaryKeyColumnExpr[_, _] =>
            SqlPropertyExpr(primaryKeyColumnExpr.table, primaryKeyColumnExpr.column)
        case SubQueryExpr(selectQuery) =>
            SqlSelectQueryExpr(selectQuery.getSelect)
        case NormalFunctionExpr(name, args) =>
            SqlExprFunctionExpr(name, args.map(visitExpr))
        case aggFunctionExpr: AggFunctionExpr[_, _] =>
            visitAggFunctionExpr(aggFunctionExpr)
        case CaseExpr(conditions, default) =>
            SqlCaseExpr(conditions.map(it => SqlCase(visitExpr(it.query), getExpr(it.thenValue))), getExpr(default))
        case InListExpr(query, list, isNot) =>
            SqlInExpr(visitExpr(query), SqlListExpr(list.map(getExpr)), isNot)
        case InSubQueryExpr(query, subQuery, isNot) =>
            SqlInExpr(visitExpr(query), SqlSelectQueryExpr(subQuery.getSelect), isNot)
        case CastExpr(query, castType) =>
            SqlCastExpr(visitExpr(query), castType)
        case BetweenExpr(query, start, end, isNot) =>
            SqlBetweenExpr(visitExpr(query), getExpr(start), getExpr(end), isNot)
        case AllColumnExpr(owner) =>
            SqlAllColumnExpr(owner)
        case OverExpr(function, partitionBy, orderBy) =>
            SqlOverExpr(visitAggFunctionExpr(function), partitionBy.map(visitExpr).toList, orderBy.map(it => SqlOrderBy(visitExpr(it.query), it.order)).toList)
        case SubQueryPredicateExpr(query, predicate) =>
            SqlSubQueryPredicateExpr(SqlSelectQueryExpr(query.getSelect), predicate)
        case ListExpr(list) =>
            SqlListExpr(list.map(getExpr))
        case _ => SqlNullExpr()
    }
}

def visitColumnExpr(column: ColumnExpr[_]): SqlExpr = {
    if (column.column.contains(".")) {
        val split = column.column.split("\\.").asInstanceOf[Array[String]]
        if (split.last.contains("*")) {
            SqlAllColumnExpr(Some(split(0)))
        } else {
            SqlPropertyExpr(split(0), split.last)
        }
    } else {
        if (column.column.contains("*")) {
            SqlAllColumnExpr()
        } else {
            SqlIdentifierExpr(column.column)
        }
    }
}

def visitAggFunctionExpr(aggFunctionExpr: AggFunctionExpr[_, _]): SqlAggFunctionExpr = {
    SqlAggFunctionExpr(aggFunctionExpr.name, aggFunctionExpr.args.map(visitExpr), aggFunctionExpr.distinct, aggFunctionExpr.attributes.map((k, v) => k -> visitExpr(v)), aggFunctionExpr.orderBy.map(it => SqlOrderBy(visitExpr(it.query), it.order)))
}

def getExpr(value: SqlSingleConstType | Null | Expr[_, _] | SelectQuery[_]): SqlExpr = {
    value match {
        case null => SqlNullExpr()
        case string: String => SqlCharExpr(string)
        case number: Int => SqlNumberExpr(number)
        case number: Long => SqlNumberExpr(number)
        case number: Float => SqlNumberExpr(number)
        case number: Double => SqlNumberExpr(number)
        case number: BigDecimal => SqlNumberExpr(number)
        case boolean: Boolean => SqlBooleanExpr(boolean)
        case date: Date => SqlDateExpr(date)
        case expr: Expr[_, _] => visitExpr(expr)
        case selectQuery: SelectQuery[_] => SqlSelectQueryExpr(selectQuery.getSelect)
    }
}
