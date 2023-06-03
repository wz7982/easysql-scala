package easysql.ast.statement

import easysql.ast.expr.SqlExpr

case class SqlWithItem(name: SqlExpr, query: SqlQuery, columns: List[SqlExpr])