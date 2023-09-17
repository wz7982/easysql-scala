package easysql.printer

import easysql.ast.statement.SqlUpsert
import easysql.ast.expr.SqlIntervalExpr

class PgsqlPrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
    override def printlnIntervalExpr(expr: SqlIntervalExpr): Unit = {
        sqlBuilder.append("INTERVAL")
        sqlBuilder.append(" '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append("' ")
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

        sqlBuilder.append(" DO UPDATE SET ")

        printList(upsert.updateList) { u =>
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append("EXCLUDED.")
            printExpr(u)
        }
    }
}