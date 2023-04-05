package easysql.printer

import easysql.ast.statement.SqlUpsert

class PgsqlPrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
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