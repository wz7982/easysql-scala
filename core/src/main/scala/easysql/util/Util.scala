package easysql.util

import easysql.ast.SqlDataType
import easysql.ast.expr.*
import easysql.ast.order.SqlOrderBy
import easysql.ast.statement.*
import easysql.database.DB
import easysql.dsl.*
import easysql.printer.*

import java.util.Date
import easysql.parser.SqlParser

def fetchPrinter(db: DB, prepare: Boolean): SqlPrinter = db match {
    case DB.MYSQL => new MysqlPrinter(prepare)
    case DB.PGSQL => new PgsqlPrinter(prepare)
    case DB.SQLSERVER => new SqlserverPrinter(prepare)
    case DB.SQLITE => new SqlitePrinter(prepare)
    case DB.ORACLE => new OraclePrinter(prepare)
}

def queryToString(query: SqlQuery, db: DB, prepare: Boolean): (String, Array[Any]) = {
    val printer = fetchPrinter(db, prepare)
    printer.printQuery(query)
    printer.sql -> printer.args.toArray
}

def statementToString(statement: SqlStatement, db: DB, prepare: Boolean): (String, Array[Any]) = {
    val printer = fetchPrinter(db, prepare)
    printer.printStatement(statement)
    printer.sql -> printer.args.toArray
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
    case OverExpr(func, partitionBy, orderBy, between) =>
        SqlOverExpr(aggExprToSqlExpr(func), partitionBy.map(exprToSqlExpr), orderBy.map(o => SqlOrderBy(exprToSqlExpr(o.expr), o.order)), between)
    case ListExpr(list) =>
        SqlListExpr(list.map(exprToSqlExpr))
    case DynamicExpr(expr) => 
        expr
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
        SqlAggFuncExpr(
            name,
            args.map(exprToSqlExpr),
            distinct,
            attrs.map((k, v) => k -> exprToSqlExpr(v)),
            orderBy.map(o => SqlOrderBy(exprToSqlExpr(o.expr), o.order))
        )
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