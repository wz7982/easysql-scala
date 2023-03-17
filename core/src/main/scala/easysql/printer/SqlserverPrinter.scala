package easysql.printer

import easysql.ast.limit.SqlLimit
import easysql.ast.statement.SqlQuery.*
import easysql.ast.statement.SqlStatement.*

class SqlserverPrinter(override val prepare: Boolean) extends SqlPrinter(prepare) {
    override def printLimit(limit: SqlLimit): Unit = {
        if prepare then {
            sqlBuilder.append("OFFSET ? ROWS FETCH NEXT ? ROWS ONLY")
            args.append(limit.offset)
            args.append(limit.limit)
        } else {
            sqlBuilder.append(s"OFFSET ${limit.offset} ROWS FETCH NEXT ${limit.limit} ROWS ONLY")
        }
    }

    override def printForUpdate: Unit = {
        sqlBuilder.append("WITH (UPDLOCK)")
    }

    override def printSelect(select: SqlSelect): Unit = {
        sqlBuilder.append("SELECT ")

        if (select.select.isEmpty) {
            sqlBuilder.append("*")
        } else {
            if (select.distinct) {
                sqlBuilder.append("DISTINCT ")
            }

            printList(select.select)(printSelectItem)
        }

        select.from.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("FROM ")
            printTable(it)
        }

        if (select.forUpdate) {
            sqlBuilder.append(" ")
            printForUpdate
        }

        select.where.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("WHERE ")
            printExpr(it)
        }

        if (select.groupBy.nonEmpty) {
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("GROUP BY ")
            printList(select.groupBy)(printExpr)
        }

        select.having.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("HAVING ")
            printExpr(it)
        }

        if (select.orderBy.nonEmpty) {
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("ORDER BY ")
            printList(select.orderBy)(printOrderBy)
        }

        select.limit.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            printLimit(it)
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
        sqlBuilder.append(s") ${quote}t2$quote")

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
        printList(upsert.updateList) { it =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(it)
            sqlBuilder.append(" = ")
            sqlBuilder.append(s"${quote}t2$quote.")
            printExpr(it)
        }

        sqlBuilder.append("\nWHEN NOT MATCHED THEN INSERT")
        sqlBuilder.append(" (")
        printList(upsert.columns) { it =>
            sqlBuilder.append(s"${quote}t1$quote.")
            printExpr(it)
        }
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES")
        sqlBuilder.append(" (")
        printList(upsert.value)(printExpr)
        sqlBuilder.append(")")
    }
}