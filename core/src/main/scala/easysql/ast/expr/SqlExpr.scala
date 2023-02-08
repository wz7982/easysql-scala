package easysql.ast.expr

import easysql.ast.order.SqlOrderBy
import easysql.ast.statement.SqlQuery

import java.util.Date

enum SqlExpr {
    case SqlBinaryExpr(left: SqlExpr, op: SqlBinaryOperator, right: SqlExpr)
    case SqlIdentExpr(name: String)
    case SqlPropertyExpr(owner: String, name: String)
    case SqlNullExpr
    case SqlAllColumnExpr(owner: Option[String])
    case SqlNumberExpr(number: Number)
    case SqlDateExpr(date: Date)
    case SqlCharExpr(text: String)
    case SqlBooleanExpr(boolean: Boolean)
    case SqlListExpr(items: List[SqlExpr])
    case SqlAggFuncExpr(name: String, args: List[SqlExpr], distinct: Boolean, attrs: Map[String, SqlExpr], orderBy: List[SqlOrderBy])
    case SqlExprFuncExpr(name: String, args: List[SqlExpr])
    case SqlCastExpr(expr: SqlExpr, castType: String)
    case SqlQueryExpr(query: SqlQuery)
    case SqlInExpr(expr: SqlExpr, inExpr: SqlExpr, not: Boolean)
    case SqlBetweenExpr(expr: SqlExpr, start: SqlExpr, end: SqlExpr, not: Boolean)
    case SqlOverExpr(agg: SqlAggFuncExpr, partitionBy: List[SqlExpr], orderBy: List[SqlOrderBy])
    case SqlCaseExpr(caseList: List[SqlCase], default: SqlExpr)
}