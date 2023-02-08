package easysql.ast.table

import easysql.ast.expr.SqlExpr
import easysql.ast.statement.*

enum SqlTable {
    case SqlIdentTable(tableName: String, alias: Option[String])
    case SqlSubQueryTable(query: SqlQuery, lateral: Boolean, alias: Option[String])
    case SqlJoinTable(left: SqlTable, joinType: SqlJoinType, right: SqlTable, on: Option[SqlExpr])
}