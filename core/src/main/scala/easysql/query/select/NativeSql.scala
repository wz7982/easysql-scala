package easysql.query.select

import easysql.query.ToSql
import easysql.database.DB
import easysql.util.*
import easysql.ast.SqlDataType

class NativeSql(val sql: String, val args: Array[SqlDataType]) {
    override def toString(): String = sql
}

object NativeSql {
    given nativeSqlToSql: ToSql[NativeSql] with {
        extension (n: NativeSql) {
            def sql(db: DB): String = {
                var sql = n.sql

                val exprs = n.args.map { arg =>
                    val printer = fetchPrinter(db, false)
                    val expr = valueToSqlExpr(arg)
                    printer.printExpr(expr)
                    printer.sql
                }
                exprs.foreach { e =>
                    sql = sql.replaceFirst("\\?", e).nn
                }

                sql
            }

            def preparedSql(db: DB): (String, Array[Any]) =
                n.sql -> n.args.map(a => a.asInstanceOf[Any])
        }
    }
}