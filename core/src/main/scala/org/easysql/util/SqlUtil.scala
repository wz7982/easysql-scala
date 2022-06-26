package org.easysql.util

import org.easysql.ast.statement.SqlStatement
import org.easysql.database.DB
import org.easysql.dsl.{Expr, ListExpr, const}
import org.easysql.visitor.outputVisitor.*

import java.sql.SQLException
import java.util.Date

def getOutPutVisitor(db: DB): SqlVisitor = {
    db match {
        case DB.MYSQL => MysqlVisitor()
        case DB.PGSQL => PgsqlVisitor()
        case DB.SQLSERVER => SqlserverVisitor()
        case DB.SQLITE => SqliteVisitor()
        case DB.ORACLE => OracleVisitor()
    }
}

def toSqlString(sqlStatement: SqlStatement, db: DB): String = {
    val visitor = getOutPutVisitor(db)
    visitor.visitSqlStatement(sqlStatement)
    visitor.sql()
}

def anyToExpr(value: Any): Expr[_, _] = {
    value match {
        case null => const(null)
        case s: String => const(s)
        case i: Int => const(i)
        case l: Long => const(l)
        case d: Double => const(d)
        case f: Float => const(f)
        case b: Boolean => const(b)
        case d: Date => const(d)
        case dc: BigDecimal => const(dc)
        case o: Option[_] => {
            if (o.isEmpty) {
                const(null)
            } else {
                o.get match {
                    case s: String => const(s)
                    case i: Int => const(i)
                    case l: Long => const(l)
                    case d: Double => const(d)
                    case f: Float => const(f)
                    case b: Boolean => const(b)
                    case d: Date => const(d)
                    case _ => throw SQLException("实体类中存在无法转换为sql表达式的属性类型")
                }
            }
        }
        case list: List[_] => ListExpr(list.map(it => anyToExpr(it)))
        case _ => throw SQLException("实体类中存在无法转换为sql表达式的属性类型")
    }
}