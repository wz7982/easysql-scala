package easysql.printer

import easysql.ast.expr.SqlListExpr
import easysql.ast.limit.SqlLimit
import easysql.ast.statement.*
import easysql.ast.expr.SqlIntervalExpr

class MysqlPrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
    override val quote = "`"

    override def printLimit(limit: SqlLimit): Unit = {
        if prepare then {
            sqlBuilder.append("LIMIT ?, ?")
            args.append(limit.offset)
            args.append(limit.limit)
        } else {
            sqlBuilder.append(s"LIMIT ${limit.offset}, ${limit.limit}")
        }
    }

    override def printIntervalExpr(expr: SqlIntervalExpr): Unit = {
        sqlBuilder.append("INTERVAL")
        sqlBuilder.append(" '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
        sqlBuilder.append(expr.unit.get.unit)
    }

    override def printUpsert(upsert: SqlUpsert): Unit = {
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table.get)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES")
        sqlBuilder.append(" (")
        printList(upsert.value)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON DUPLICATE KEY UPDATE ")

        printList(upsert.updateList) { u =>
            printExpr(u)
            sqlBuilder.append(" = VALUES(")
            printExpr(u)
            sqlBuilder.append(")")
        }
    }

    override def printValues(values: SqlValues): Unit = {
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlListExpr(_))) { v =>
            sqlBuilder.append("ROW")
            printExpr(v)
        }
    }
}