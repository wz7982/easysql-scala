package easysql.ast.statement

import easysql.ast.expr.SqlExpr

case class SqlSelectItem(expr: SqlExpr, alias: Option[String])