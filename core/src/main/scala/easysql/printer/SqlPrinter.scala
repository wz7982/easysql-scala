package easysql.printer

import easysql.ast.expr.*
import easysql.ast.expr.SqlBinaryOperator.*
import easysql.ast.expr.SqlOverBetween.*
import easysql.ast.expr.SqlOverBetweenType.*
import easysql.ast.limit.SqlLimit
import easysql.ast.order.*
import easysql.ast.statement.*
import easysql.ast.table.*

import java.text.SimpleDateFormat
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SqlPrinter(val prepare: Boolean) {
    val sqlBuilder: StringBuilder = StringBuilder()

    val quote = "\""

    var spaceNum = 0

    val args: ArrayBuffer[Any] = ArrayBuffer()

    def sql: String = sqlBuilder.toString()

    def printStatement(statement: SqlStatement): Unit = {
        statement match {
            case update: SqlUpdate => printUpdate(update)
            case insert: SqlInsert => printInsert(insert)
            case delete: SqlDelete => printDelete(delete)
            case upsert: SqlUpsert => printUpsert(upsert)
            case truncate: SqlTruncate => printTruncate(truncate)
            case cte: SqlWith => printWith(cte)
        }
    }

    def printUpdate(update: SqlUpdate): Unit = {
        sqlBuilder.append("UPDATE ")
        printTable(update.table.get)
        sqlBuilder.append(" SET ")

        for (i <- update.setList.indices) {
            printExpr(update.setList(i)._1)
            sqlBuilder.append(" = ")
            printExpr(update.setList(i)._2)

            if (i < update.setList.size - 1) {
                sqlBuilder.append(", ")
            }
        }

        update.where.foreach { it =>
            sqlBuilder.append(" WHERE ")
            printExpr(it)
        }
    }

    def printInsert(insert: SqlInsert): Unit = {
        sqlBuilder.append("INSERT INTO ")
        printTable(insert.table.get)
        if (insert.columns.nonEmpty) {
            sqlBuilder.append(" (")
            printList(insert.columns)(printExpr)
            sqlBuilder.append(")")
        }

        if (insert.query.isDefined) {
            sqlBuilder.append("\n")
            printQuery(insert.query.get)
        } else {
            sqlBuilder.append(" VALUES ")
            printList(insert.values.map(SqlListExpr(_)))(printExpr)
        }
    }

    def printDelete(delete: SqlDelete): Unit = {
        sqlBuilder.append("DELETE FROM ")
        printTable(delete.table.get)

        delete.where.foreach { it =>
            sqlBuilder.append(" WHERE ")
            printExpr(it)
        }
    }

    def printUpsert(upsert: SqlUpsert): Unit

    def printTruncate(truncate: SqlTruncate): Unit = {
        sqlBuilder.append("TRUNCATE ")
        printTable(truncate.table.get)
    }

    def printQuery(query: SqlQuery): Unit = {
        query match {
            case select: SqlSelect => printSelect(select)
            case SqlUnion(left, unionType, right) => {
                sqlBuilder.append("(")
                printQuery(left)
                sqlBuilder.append(")")
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(unionType.unionType)
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append("(")
                printQuery(right)
                sqlBuilder.append(")")
            }
            case values: SqlValues => printValues(values)
        }
    }

    def printSelect(select: SqlSelect): Unit = {
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

        if (select.forUpdate) {
            sqlBuilder.append(" ")
            printForUpdate()
        }
    }

    def printValues(values: SqlValues): Unit = {
        sqlBuilder.append("VALUES ")
        printList(values.values.map(SqlListExpr(_)))(printExpr)
    }

    def printWithRecursive(): Unit = {
        sqlBuilder.append("RECURSIVE ")
    }

    def printWith(cte: SqlWith): Unit = {
        sqlBuilder.append("WITH ")
        if (cte.recursive) {
            printWithRecursive()
        }

        def printWithItem(sqlWithItem: SqlWithItem): Unit = {
            spaceNum += 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            printExpr(sqlWithItem.name)
            if (sqlWithItem.columns.nonEmpty) {
                sqlBuilder.append("(")
                printList(sqlWithItem.columns)(printExpr)
                sqlBuilder.append(")")
            }
            sqlBuilder.append(" AS ")
            sqlBuilder.append("(")
            spaceNum += 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            printQuery(sqlWithItem.query)
            spaceNum -= 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            spaceNum -= 4
            sqlBuilder.append(")")
        }

        printList(cte.withList)(printWithItem)
        sqlBuilder.append("\n")
        printQuery(cte.query.get)
    }

    def printLimit(limit: SqlLimit): Unit = {
        if prepare then {
            sqlBuilder.append("LIMIT ? OFFSET ?")
            args.append(limit.limit)
            args.append(limit.offset)
        } else {
            sqlBuilder.append(s"LIMIT ${limit.limit} OFFSET ${limit.offset}")
        }
    }

    def printOrderBy(orderBy: SqlOrderBy): Unit = {
        printExpr(orderBy.expr)
        sqlBuilder.append(s" ${orderBy.order.order}")
    }

    def printExpr(expr: SqlExpr): Unit = {
        expr match {
            case binary: SqlBinaryExpr => printBinaryExpr(binary)

            case SqlCharExpr(text) => {
                if prepare then {
                    sqlBuilder.append("?")
                    args.append(text)
                } else {
                    sqlBuilder.append(s"'${text.replace("'", "''")}'")
                }
            }
                
            case SqlNumberExpr(number) => {
                if prepare then {
                    sqlBuilder.append("?")
                    args.append(number)
                } else {
                    sqlBuilder.append(number.toString)
                }
            }
                
            case SqlBooleanExpr(bool) => {
                if prepare then {
                    sqlBuilder.append("?")
                    args.append(bool)
                } else {
                    sqlBuilder.append(bool.toString.toUpperCase.nn)
                }
            }

            case SqlDateExpr(date) => {
                if prepare then {
                    sqlBuilder.append("?")
                    args.append(date)
                } else {
                    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    sqlBuilder.append(s"'${format.format(date)}'")
                }
            }

            case SqlIdentExpr(name) => sqlBuilder.append(s"$quote${name}$quote")

            case SqlPropertyExpr(owner, name) => sqlBuilder.append(s"$quote${owner}$quote.$quote${name}$quote")

            case SqlNullExpr => sqlBuilder.append("NULL")

            case SqlAllColumnExpr(owner) => {
                owner.foreach(o => sqlBuilder.append(s"$quote$o$quote."))
                sqlBuilder.append("*")
            }

            case SqlListExpr(items) => {
                sqlBuilder.append("(")
                printList(items)(printExpr)
                sqlBuilder.append(")")
            }

            case SqlInExpr(expr, inExpr, not) => {
                printExpr(expr)
                if (not) {
                    sqlBuilder.append(" NOT")
                }
                sqlBuilder.append(" IN ")
                printExpr(inExpr)
            }

            case SqlBetweenExpr(expr, start, end, not) => {
                printExpr(expr)
                if (not) {
                    sqlBuilder.append(" NOT")
                }
                sqlBuilder.append(" BETWEEN ")
                printExpr(start)
                sqlBuilder.append(" AND ")
                printExpr(end)
            }

            case SqlCastExpr(expr, castType) => {
                sqlBuilder.append("CAST(")
                printExpr(expr)
                sqlBuilder.append(s" AS ${castType})")
            }

            case SqlExprFuncExpr(name, args) => {
                sqlBuilder.append(name)
                sqlBuilder.append("(")
                printList(args)(printExpr)
                sqlBuilder.append(")")
            }

            case agg: SqlAggFuncExpr => printAggFuncExpr(agg)

            case SqlQueryExpr(query) => {
                sqlBuilder.append("(")
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                printQuery(query)
                spaceNum -= 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(")")
            }

            case SqlOverExpr(agg, partitionBy, orderBy, between) => {
                printAggFuncExpr(agg)
                sqlBuilder.append(" OVER (")
                if (partitionBy.nonEmpty) {
                    sqlBuilder.append("PARTITION BY ")
                    printList(partitionBy)(printExpr)
                }
                if (orderBy.nonEmpty) {
                    if (partitionBy.nonEmpty) {
                        sqlBuilder.append(" ")
                    }
                    sqlBuilder.append("ORDER BY ")
                    printList(orderBy)(printOrderBy)
                }
                between match {
                    case Some(b) => {
                        def printOverBetweenType(b: SqlOverBetweenType): Unit =
                            b match {
                                case Following(n) => {
                                    printExpr(n)
                                    sqlBuilder.append(" " + b.show)
                                }
                                case Preceding(n) => {
                                    printExpr(n)
                                    sqlBuilder.append(" " + b.show)
                                }
                                case _ => sqlBuilder.append(b.show)
                            }

                        b match {
                            case rows: Rows => sqlBuilder.append(" ROWS BETWEEN ")
                            case range: Range => sqlBuilder.append(" RANGE BETWEEN ")
                        }
                        printOverBetweenType(b.start)
                        sqlBuilder.append(" AND ")
                        printOverBetweenType(b.end)
                    }
                    case None =>
                }
                sqlBuilder.append(")")
            }

            case SqlCaseExpr(caseList, default) => {
                sqlBuilder.append("CASE")
                caseList.foreach { branch =>
                    sqlBuilder.append(" WHEN ")
                    printExpr(branch.expr)
                    sqlBuilder.append(" THEN ")
                    printExpr(branch.thenExpr)
                }
                sqlBuilder.append(" ELSE ")
                printExpr(default)
                sqlBuilder.append(" END")
            }
        }
    }

    def printBinaryExpr(expr: SqlBinaryExpr): Unit = {
        def hasBrackets(parent: SqlBinaryExpr, child: SqlExpr): Boolean = {
            parent.op match {
                case And => {
                    child match {
                        case SqlBinaryExpr(_, op, _) => op match {
                            case Or | Xor => true
                            case _ => false
                        }
                        case _ => false
                    }
                }
                case Xor => {
                    child match {
                        case SqlBinaryExpr(_, op, _) => op match {
                            case Or => true
                            case _ => false
                        }
                        case _ => false
                    }
                }
                case Mul | Div | Mod => {
                    child match {
                        case SqlBinaryExpr(_, op, _) => op match {
                            case Add | Sub => true
                            case _ => false
                        }
                        case _ => false
                    }   
                } 
                case _ => false
            }
        }

        val leftBrackets = hasBrackets(expr, expr.left)
        if (leftBrackets) {
            sqlBuilder.append("(")
            printExpr(expr.left)
            sqlBuilder.append(")")
        } else {
            printExpr(expr.left)
        }
        sqlBuilder.append(s" ${expr.op.operator} ")
        val rightBrackets = hasBrackets(expr, expr.right)
        if (rightBrackets) {
            sqlBuilder.append("(")
            printExpr(expr.right)
            sqlBuilder.append(")")
        } else {
            printExpr(expr.right)
        }
    }

    def printAggFuncExpr(agg: SqlAggFuncExpr): Unit = {
        sqlBuilder.append(agg.name)
        sqlBuilder.append("(")
        if (agg.distinct) {
            sqlBuilder.append("DISTINCT ")
        }
        if (agg.name.toUpperCase() == "COUNT" && agg.args.isEmpty) {
            sqlBuilder.append("*")
        }
        printList(agg.args)(printExpr)
        if (agg.orderBy.nonEmpty) {
            sqlBuilder.append(" ORDER BY ")
            printList(agg.orderBy)(printOrderBy)
        }
        agg.attrs.foreach { attr =>
            sqlBuilder.append(s" ${attr._1} ")
            printExpr(attr._2)
        }
        sqlBuilder.append(")")
    }

    def printTable(table: SqlTable): Unit = {
        table match {
            case SqlIdentTable(tableName, alias) => {
                sqlBuilder.append(s"$quote$tableName$quote")
                alias.foreach { a =>
                    sqlBuilder.append(s" AS $quote$a$quote")
                }
            }
            case SqlSubQueryTable(query, lateral, alias) => {
                if (lateral) {
                    sqlBuilder.append("LATERAL ")
                }

                sqlBuilder.append("(")
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                printQuery(query)
                spaceNum -= 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(")")

                alias.foreach { a =>
                    sqlBuilder.append(s" AS $quote$a$quote")
                }
            }
            case SqlJoinTable(left, joinType, right, on) => {
                printTable(left)
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(s"${joinType.joinType} ")
                right match {
                    case _: SqlJoinTable => sqlBuilder.append("(")
                    case _ =>
                }
                printTable(right)
                right match {
                    case _: SqlJoinTable => sqlBuilder.append(")")
                    case _ =>
                }

                on.foreach { it =>
                    sqlBuilder.append(" ON ")
                    printExpr(it)
                }
                spaceNum -= 4
            }
        }
    }

    def printSelectItem(selectItem: SqlSelectItem): Unit = {
        printExpr(selectItem.expr)
        selectItem.alias.foreach(a => sqlBuilder.append(s" AS $quote$a$quote"))
    }

    def printForUpdate(): Unit = sqlBuilder.append("FOR UPDATE")

    def printSpace(num: Int): Unit = {
        if (num > 0) {
            for (_ <- 1 to num) {
                sqlBuilder.append(" ")
            }
        }
    }

    def printList[T](list: List[T])(printer: T => Unit): Unit = {
        for (i <- list.indices) {
            printer(list(i))
            if (i < list.size - 1) {
                sqlBuilder.append(", ")
            }
        }
    }
}