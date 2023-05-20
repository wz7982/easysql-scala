package easysql.ast.expr

import easysql.ast.order.SqlOrderBy
import easysql.ast.statement.SqlQuery

import java.util.Date

sealed trait SqlExpr

case class SqlBinaryExpr(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr) extends SqlExpr

case class SqlIdentExpr(name: String) extends SqlExpr

case class SqlPropertyExpr(owner: String, name: String) extends SqlExpr

case object SqlNullExpr extends SqlExpr

case class SqlAllColumnExpr(owner: Option[String]) extends SqlExpr

case class SqlNumberExpr(number: Number) extends SqlExpr

case class SqlDateExpr(date: Date) extends SqlExpr

case class SqlCharExpr(text: String) extends SqlExpr

case class SqlBooleanExpr(boolean: Boolean) extends SqlExpr

case class SqlListExpr(items: List[SqlExpr]) extends SqlExpr

case class SqlAggFuncExpr(
    name: String, 
    args: List[SqlExpr],
    distinct: Boolean,
    attrs: Map[String, SqlExpr],
    orderBy: List[SqlOrderBy]
) extends SqlExpr

case class SqlExprFuncExpr(name: String, args: List[SqlExpr]) extends SqlExpr

case class SqlCastExpr(expr: SqlExpr, castType: String) extends SqlExpr

case class SqlQueryExpr(query: SqlQuery) extends SqlExpr

case class SqlInExpr(expr: SqlExpr, inExpr: SqlExpr, not: Boolean) extends SqlExpr

case class SqlBetweenExpr(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean) extends SqlExpr

case class SqlOverExpr(agg: SqlAggFuncExpr, partitionBy: List[SqlExpr], orderBy: List[SqlOrderBy]) extends SqlExpr

case class SqlCaseExpr(caseList: List[SqlCase], default: SqlExpr) extends SqlExpr