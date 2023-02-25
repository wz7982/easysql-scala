package easysql.util

import easysql.database.DB
import easysql.printer.*
import easysql.ast.statement.*
import easysql.ast.expr.SqlExpr
import easysql.ast.expr.SqlExpr.*
import easysql.ast.expr.SqlCase
import easysql.ast.SqlDataType
import easysql.ast.order.SqlOrderBy
import easysql.dsl.*

import java.util.Date

def fetchPrinter(db: DB): SqlPrinter = db match {
    case DB.MYSQL => MysqlPrinter()
    case DB.PGSQL => PgsqlPrinter()
    case DB.SQLSERVER => SqlserverPrinter()
    case DB.SQLITE => SqlitePrinter()
    case DB.ORACLE => OraclePrinter()
}

def queryToString(query: SqlQuery, db: DB): String = {
    val printer = fetchPrinter(db)
    printer.printQuery(query)
    printer.sql
}

def statementToString(statement: SqlStatement, db: DB): String = {
    val printer = fetchPrinter(db)
    printer.printStatement(statement)
    printer.sql
}

def camelListToSnakeList(s: List[Char]): List[Char] = s match {
    case x :: y :: t if y.isUpper => x.toLower :: '_' :: camelListToSnakeList(y :: t)
    case h :: t => h.toLower :: camelListToSnakeList(t)
    case Nil => Nil
}

def camelToSnake(s: String): String = 
    camelListToSnakeList(s.toList).mkString

def exprToSqlExpr(expr: Expr[_]): SqlExpr = expr match {
    case NullExpr => 
        SqlNullExpr
    case LiteralExpr(value) => 
        valueToSqlExpr(value)
    case BinaryExpr(left, op, right) => 
        SqlBinaryExpr(exprToSqlExpr(left), op, exprToSqlExpr(right))
    case IdentExpr(column) => 
        parseIdent(column)
    case ColumnExpr(tableName, columnName, _) =>
        SqlPropertyExpr(tableName, columnName)
    case PrimaryKeyExpr(tableName, columnName, _, _) =>
        SqlPropertyExpr(tableName, columnName)
    case SubQueryExpr(query) => 
        SqlQueryExpr(query.getAst)
    case FuncExpr(name, args) => 
        SqlExprFuncExpr(name, args.map(exprToSqlExpr))
    case agg: AggExpr[_] =>
        aggExprToSqlExpr(agg)
    case CaseExpr(branches, default) =>
        SqlCaseExpr(branches.map(b => SqlCase(exprToSqlExpr(b.expr), exprToSqlExpr(b.thenValue))), exprToSqlExpr(default))
    case InExpr(expr, inExpr, not) =>
        SqlInExpr(exprToSqlExpr(expr), exprToSqlExpr(inExpr), not)
    case CastExpr(expr, castType) =>
        SqlCastExpr(exprToSqlExpr(expr), castType)
    case BetweenExpr(expr, start, end, not) =>
        SqlBetweenExpr(exprToSqlExpr(expr), exprToSqlExpr(start), exprToSqlExpr(end), not)
    case AllColumnExpr(owner) =>
        SqlAllColumnExpr(owner)
    case OverExpr(func, partitionBy, orderBy) =>
        SqlOverExpr(aggExprToSqlExpr(func), partitionBy.map(exprToSqlExpr), orderBy.map(o => SqlOrderBy(exprToSqlExpr(o.expr), o.order)))
    case ListExpr(list) =>
        SqlListExpr(list.map(exprToSqlExpr))
}

def parseIdent(column: String): SqlExpr = {
    import scala.language.unsafeNulls

    if (column.contains(".")) {
        val split = column.split("\\.")
        if (split.last.contains("*")) {
            SqlAllColumnExpr(Some(split(0)))
        } else {
            SqlPropertyExpr(split(0), split.last)
        }
    } else {
        if (column.contains("*")) {
            SqlAllColumnExpr(None)
        } else {
            SqlIdentExpr(column)
        }
    }
}

def aggExprToSqlExpr(agg: AggExpr[_]): SqlAggFuncExpr = agg match {
    case AggExpr(name, args, distinct, attrs, orderBy) =>
        SqlAggFuncExpr(name, args.map(exprToSqlExpr), distinct, attrs.mapValues(exprToSqlExpr).toMap, orderBy.map(o => SqlOrderBy(exprToSqlExpr(o.expr), o.order)))
}

def valueToSqlExpr(value: SqlDataType): SqlExpr = value match {
    case s: String => SqlCharExpr(s)
    case i: Int => SqlNumberExpr(i)
    case l: Long => SqlNumberExpr(l)
    case f: Float => SqlNumberExpr(f)
    case d: Double => SqlNumberExpr(d)
    case d: BigDecimal => SqlNumberExpr(d)
    case b: Boolean => SqlBooleanExpr(b)
    case d: Date => SqlDateExpr(d)
}