package easysql.printer

import easysql.ast.limit.SqlLimit
import easysql.ast.statement.SqlStatement.SqlUpsert

class OraclePrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
    override def printLimit(limit: SqlLimit): Unit = {
        if prepare then {
            sqlBuilder.append("OFFSET ? ROWS FETCH FIRST ? ROWS ONLY")
            args.append(limit.offset)
            args.append(limit.limit)
        } else {
            sqlBuilder.append(s"OFFSET ${limit.offset} ROWS FETCH FIRST ${limit.limit} ROWS ONLY")
        }
    }

    override def printWithRecursive: Unit = {}

    override def printUpsert(upsert: SqlUpsert): Unit = {
        sqlBuilder.append("MERGE INTO ")
        printTable(upsert.table.get)
        sqlBuilder.append(s" ${quote}t1$quote")

        sqlBuilder.append(" USING (")
        sqlBuilder.append("SELECT ")
        for (index <- upsert.columns.indices) {
            printExpr(upsert.value(index))
            sqlBuilder.append(" AS ")
            printExpr(upsert.columns(index))
            if (index < upsert.columns.size - 1) {
                sqlBuilder.append(",")
                sqlBuilder.append(" ")
            }
        }
        sqlBuilder.append(s" FROM ${quote}dual$quote) ${quote}t2$quote")

        sqlBuilder.append("\nON (")
        for (index <- upsert.pkList.indices) {
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(upsert.pkList(index))
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${quote}t2$quote.")
            printExpr(upsert.pkList(index))
            if (index < upsert.pkList.size - 1) {
                sqlBuilder.append(" AND ")
            }
        }
        sqlBuilder.append(")")

        sqlBuilder.append("\nWHEN MATCHED THEN UPDATE SET ")
        printList(upsert.updateList) { u =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${quote}t2$quote.")
            printExpr(u)
        }

        sqlBuilder.append("\nWHEN NOT MATCHED THEN INSERT")
        sqlBuilder.append(" (")
        printList(upsert.columns) { c =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(c)
        }
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES")
        sqlBuilder.append(" (")
        printList(upsert.value)(printExpr)
        sqlBuilder.append(")")
    }
}