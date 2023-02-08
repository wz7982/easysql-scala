package easysql.ast.statement

import easysql.ast.expr.SqlExpr
import easysql.ast.statement.*

case class SqlWithItem(name: SqlExpr, query: SqlQuery, columns: List[SqlExpr])