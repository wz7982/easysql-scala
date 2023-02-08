package easysql.ast.order

import easysql.ast.expr.SqlExpr

case class SqlOrderBy(expr: SqlExpr, order: SqlOrderByOption)