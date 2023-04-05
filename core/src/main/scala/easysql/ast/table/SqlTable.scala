package easysql.ast.table

import easysql.ast.expr.SqlExpr
import easysql.ast.statement.*

sealed trait SqlTable

case class SqlIdentTable(tableName: String, alias: Option[String]) extends SqlTable

case class SqlSubQueryTable(query: SqlQuery, lateral: Boolean, alias: Option[String]) extends SqlTable

case class SqlJoinTable(left: SqlTable, joinType: SqlJoinType, right: SqlTable, on: Option[SqlExpr]) extends SqlTable