package easysql.printer

import easysql.ast.limit.SqlLimit
import easysql.ast.statement.SqlStatement.SqlUpsert

class SqlitePrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
    override def printLimit(limit: SqlLimit): Unit = {
        if prepare then {
            sqlBuilder.append("LIMIT ?, ?")
            args.append(limit.offset)
            args.append(limit.limit)
        } else {
            sqlBuilder.append(s"LIMIT ${limit.offset}, ${limit.limit}")
        }
    }

    override def printUpsert(upsert: SqlUpsert): Unit = {
        sqlBuilder.append("INSERT OR REPLACE INTO ")
        printTable(upsert.table.get)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES")
        sqlBuilder.append(" (")
        printList(upsert.value)(printExpr)
        sqlBuilder.append(")")
    }
}