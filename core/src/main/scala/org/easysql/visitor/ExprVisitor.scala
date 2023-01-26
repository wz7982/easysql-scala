package org.easysql.visitor

import org.easysql.ast.expr.*
import org.easysql.ast.{SqlDataType, SqlNumberType}
import org.easysql.ast.order.SqlOrderBy
import org.easysql.dsl.*
import org.easysql.query.select.SelectQuery

import java.util.Date

def visitExpr(query: Expr[_]): SqlExpr = {
    query match {
        case null =>
            SqlNullExpr()
        case ConstExpr(value) =>
            getExpr(value)
        case BinaryExpr(left, op, right) =>
            SqlBinaryExpr(visitExpr(left), op, visitExpr(right))
        case column: ColumnExpr[_] =>
            visitColumnExpr(column)
        case TableColumnExpr(_, column, _, schema) =>
            SqlPropertyExpr(schema._aliasName.getOrElse(schema._tableName), column)
        case PrimaryKeyColumnExpr(_, column, _, schema, _) =>
            SqlPropertyExpr(schema._aliasName.getOrElse(schema._tableName), column)
        case SubQueryExpr(selectQuery) =>
            SqlSelectQueryExpr(selectQuery.getSelect)
        case NormalFunctionExpr(name, args) =>
            SqlExprFunctionExpr(name, args.map(visitExpr))
        case aggFunctionExpr: AggFunctionExpr[_] =>
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
    }
}

def visitColumnExpr(column: ColumnExpr[_]): SqlExpr = {
    if (column.column.contains(".")) {
        val split = column.column.split("\\.")
        if (split.last.contains("*")) {
            SqlAllColumnExpr(Some(split(0)))
        } else {
            SqlPropertyExpr(split(0), split.last)
        }
    } else {
        if (column.column.contains("*")) {
            SqlAllColumnExpr()
        } else {
            SqlIdentExpr(column.column)
        }
    }
}

def visitAggFunctionExpr(aggFunctionExpr: AggFunctionExpr[_]): SqlAggFunctionExpr = {
    SqlAggFunctionExpr(aggFunctionExpr.name, aggFunctionExpr.args.map(visitExpr), aggFunctionExpr.distinct, aggFunctionExpr.attributes.map((k, v) => k -> visitExpr(v)), aggFunctionExpr.orderBy.map(it => SqlOrderBy(visitExpr(it.query), it.order)))
}

def getExpr(value: SqlDataType | Expr[_] | SelectQuery[_, _]): SqlExpr = {
    value match {
        case null => SqlNullExpr()
        case string: String => SqlCharExpr(string)
        case int: Int => SqlNumberExpr(int)
        case long: Long => SqlNumberExpr(long)
        case float: Float => SqlNumberExpr(float)
        case double: Double => SqlNumberExpr(double)
        case decimal: BigDecimal => SqlNumberExpr(decimal)
        case boolean: Boolean => SqlBooleanExpr(boolean)
        case date: Date => SqlDateExpr(date)
        case expr: Expr[_] => visitExpr(expr)
        case selectQuery: SelectQuery[_, _] => SqlSelectQueryExpr(selectQuery.getSelect)
    }
}
