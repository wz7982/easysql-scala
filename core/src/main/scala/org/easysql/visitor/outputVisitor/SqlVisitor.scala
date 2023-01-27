package org.easysql.visitor.outputVisitor

import org.easysql.ast.SqlNode
import org.easysql.ast.expr.*
import org.easysql.ast.limit.SqlLimit
import org.easysql.ast.order.SqlOrderBy
import org.easysql.ast.statement.*
import org.easysql.ast.statement.delete.SqlDelete
import org.easysql.ast.statement.insert.SqlInsert
import org.easysql.ast.statement.select.*
import org.easysql.ast.statement.truncate.SqlTruncate
import org.easysql.ast.statement.update.SqlUpdate
import org.easysql.ast.statement.upsert.SqlUpsert
import org.easysql.ast.table.*

import java.sql.SQLException
import scala.collection.mutable

abstract class SqlVisitor {
    val sqlBuilder: mutable.StringBuilder = mutable.StringBuilder()

    val quote = "\""

    protected var spaceNum = 0

    def sql(): String = sqlBuilder.toString()

    def visitSqlStatement(sqlNode: SqlStatement): Unit = {
        sqlNode match {
            case query: SqlSelectQuery => visitSqlSelectQuery(query)
            case update: SqlUpdate => visitSqlUpdate(update)
            case insert: SqlInsert => visitSqlInsert(insert)
            case delete: SqlDelete => visitSqlDelete(delete)
            case truncate: SqlTruncate => visitSqlTruncate(truncate)
            case upsert: SqlUpsert => visitSqlUpsert(upsert)
            case _ =>
        }
    }

    def visitSqlSelectQuery(query: SqlSelectQuery): Unit = {
        query match {
            case select: SqlSelect => visitSqlSelect(select)
            case SqlUnionSelect(left, unionType, right) =>
                left match {
                    case _: SqlUnionSelect =>
                    case _ => sqlBuilder.append("(")
                }
                visitSqlSelectQuery(left)
                left match {
                    case _: SqlUnionSelect =>
                    case _ => sqlBuilder.append(")")
                }
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(unionType.unionType)
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                right match {
                    case _: SqlUnionSelect =>
                    case _ => sqlBuilder.append("(")
                }
                visitSqlSelectQuery(right)
                right match {
                    case _: SqlUnionSelect =>
                    case _ => sqlBuilder.append(")")
                }
            case values: SqlValuesSelect => visitSqlValuesSelect(values)
            case withSelect: SqlWithSelect => visitSqlWithSelect(withSelect)
            case _ =>
        }
    }

    def visitSqlSelect(select: SqlSelect): Unit = {
        sqlBuilder.append("SELECT ")

        if (select.selectList.isEmpty) {
            sqlBuilder.append("*")
        } else {
            if (select.distinct) {
                sqlBuilder.append("DISTINCT ")
            }

            printList(select.selectList.toList, visitSqlSelectItem)
        }

        select.from.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("FROM ")
            visitSqlTable(it)
        }

        select.where.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("WHERE ")
            visitSqlExpr(it)
        }

        if (select.groupBy.nonEmpty) {
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("GROUP BY ")
            printList(select.groupBy.toList, visitSqlExpr)
        }

        select.having.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("HAVING ")
            visitSqlExpr(it)
        }

        if (select.orderBy.nonEmpty) {
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            sqlBuilder.append("ORDER BY ")
            printList(select.orderBy.toList, visitSqlOrderBy)
        }

        select.limit.foreach { it =>
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            visitSqlLimit(it)
        }

        if (select.forUpdate) {
            sqlBuilder.append(" ")
            visitSqlForUpdate()
        }
    }

    def visitSqlValuesSelect(sqlValuesSelect: SqlValuesSelect): Unit = {
        sqlBuilder.append("VALUES ")
        printList(sqlValuesSelect.values.toList.map { it =>
            SqlListExpr(it)
        }, visitSqlExpr)
    }

    def printWithRecursive(): Unit = {
        sqlBuilder.append("RECURSIVE ")
    }

    def visitSqlWithSelect(sqlWithSelect: SqlWithSelect): Unit = {
        sqlBuilder.append("WITH ")
        if (sqlWithSelect.recursive) {
            printWithRecursive()
        }

        def visitSqlWithItem(sqlWithItem: SqlWithItem): Unit = {
            spaceNum += 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            visitSqlExpr(sqlWithItem.name)
            if (sqlWithItem.columns.nonEmpty) {
                sqlBuilder.append("(")
                printList(sqlWithItem.columns, visitSqlExpr)
                sqlBuilder.append(")")
            }
            sqlBuilder.append(" AS ")
            sqlBuilder.append("(")
            spaceNum += 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            visitSqlSelectQuery(sqlWithItem.query)
            spaceNum -= 4
            sqlBuilder.append("\n")
            printSpace(spaceNum)
            spaceNum -= 4
        }

        printList(sqlWithSelect.withList.toList, visitSqlWithItem)
        sqlBuilder.append("\n")
        visitSqlSelectQuery(sqlWithSelect.query.get)
    }

    def visitSqlDelete(sqlDelete: SqlDelete): Unit = {
        sqlBuilder.append("DELETE FROM ")
        visitSqlTable(sqlDelete.table.get)

        sqlDelete.where.foreach { it =>
            sqlBuilder.append(" WHERE ")
            visitSqlExpr(it)
        }
    }

    def visitSqlUpdate(sqlUpdate: SqlUpdate): Unit = {
        sqlBuilder.append("UPDATE ")
        visitSqlTable(sqlUpdate.table.get)
        sqlBuilder.append(" SET ")

        for (i <- sqlUpdate.setList.indices) {
            visitSqlExpr(sqlUpdate.setList(i)._1)
            sqlBuilder.append(" = ")
            visitSqlExpr(sqlUpdate.setList(i)._2)

            if (i < sqlUpdate.setList.size - 1) {
                sqlBuilder.append(", ")
            }
        }

        sqlUpdate.where.foreach { it =>
            sqlBuilder.append(" WHERE ")
            visitSqlExpr(it)
        }
    }

    def visitSqlInsert(sqlInsert: SqlInsert): Unit = {
        sqlBuilder.append("INSERT INTO ")
        visitSqlTable(sqlInsert.table.get)
        if (sqlInsert.columns.nonEmpty) {
            sqlBuilder.append(" (")
            printList(sqlInsert.columns.toList, visitSqlExpr)
            sqlBuilder.append(")")
        }

        if (sqlInsert.query.isDefined) {
            sqlBuilder.append("\n")
            visitSqlSelectQuery(sqlInsert.query.get)
        } else {
            sqlBuilder.append(" VALUES ")
            printList(sqlInsert.values.map(SqlListExpr(_)).toList, visitSqlExpr)
        }
    }

    def visitSqlUpsert(sqlUpsert: SqlUpsert): Unit = {}

    def visitSqlTruncate(sqlTruncate: SqlTruncate): Unit = {
        sqlBuilder.append("TRUNCATE ")
        visitSqlTable(sqlTruncate.table.get)
    }

    def visitSqlLimit(sqlLimit: SqlLimit): Unit = {
        sqlBuilder.append(s"LIMIT ${sqlLimit.limit} OFFSET ${sqlLimit.offset}")
    }

    def visitSqlOrderBy(sqlOrderBy: SqlOrderBy): Unit = {
        visitSqlExpr(sqlOrderBy.expr)
        sqlBuilder.append(s" ${sqlOrderBy.order.order}")
    }

    def visitSqlForUpdate(): Unit = sqlBuilder.append("FOR UPDATE")

    def visitSqlBinaryExpr(sqlExpr: SqlBinaryExpr): Unit = {
        def needParentheses(parent: SqlBinaryExpr, child: SqlExpr): Boolean = {
            if (parent.operator == SqlBinaryOperator.AND) {
                child match {
                    case expr: SqlBinaryExpr =>
                        if (expr.operator == SqlBinaryOperator.OR || expr.operator == SqlBinaryOperator.XOR) {
                            return true
                        }

                    case _ =>
                }
            }

            if (parent.operator == SqlBinaryOperator.XOR) {
                child match {
                    case expr: SqlBinaryExpr => 
                        if (expr.operator == SqlBinaryOperator.OR) {
                            return true
                        }
                    case _ =>
                }
            }

            if (parent.operator == SqlBinaryOperator.MUL || parent.operator == SqlBinaryOperator.DIV || parent.operator == SqlBinaryOperator.MOD) {
                child match {
                    case expr: SqlBinaryExpr =>
                        if (expr.operator == SqlBinaryOperator.ADD || expr.operator == SqlBinaryOperator.SUB) {
                            return true
                        }

                    case _ =>
                }
            }
            false
        }

        val visitLeft = needParentheses(sqlExpr, sqlExpr.left)
        if (visitLeft) {
            sqlBuilder.append("(")
            visitSqlExpr(sqlExpr.left)
            sqlBuilder.append(")")
        } else {
            visitSqlExpr(sqlExpr.left)
        }

        sqlExpr.right match {
            case SqlNullExpr() => {
                if (sqlExpr.operator == SqlBinaryOperator.EQ) {
                    sqlBuilder.append(" IS ")
                } else if (sqlExpr.operator == SqlBinaryOperator.NE) {
                    sqlBuilder.append(" IS NOT ")
                } else {
                    sqlBuilder.append(s" ${sqlExpr.operator.operator} ")
                }
            }
            case _ => sqlBuilder.append(s" ${sqlExpr.operator.operator} ")
        }

        val visitRight = needParentheses(sqlExpr, sqlExpr.right)
        if (visitRight) {
            sqlBuilder.append("(")
            visitSqlExpr(sqlExpr.right)
            sqlBuilder.append(")")
        } else {
            visitSqlExpr(sqlExpr.right)
        }
    }

    def visitSqlExpr(sqlExpr: SqlExpr): Unit = {
        sqlExpr match {
            case expr: SqlBinaryExpr => visitSqlBinaryExpr(expr)

            case expr: SqlCharExpr => sqlBuilder.append(expr.toString)

            case expr: SqlNumberExpr => sqlBuilder.append(expr.number.toString)

            case expr: SqlBooleanExpr => sqlBuilder.append(expr.boolean.toString)

            case expr: SqlDateExpr => sqlBuilder.append(expr.toString)

            case expr: SqlIdentExpr => sqlBuilder.append(s"$quote${expr.name}$quote")

            case expr: SqlPropertyExpr => sqlBuilder.append(s"$quote${expr.owner}$quote.$quote${expr.name}$quote")

            case _: SqlNullExpr => sqlBuilder.append("NULL")

            case expr: SqlAllColumnExpr =>
                expr.owner.foreach(it => sqlBuilder.append(s"$quote$it$quote."))
                sqlBuilder.append("*")

            case expr: SqlListExpr[_] =>
                sqlBuilder.append("(")
                printList(expr.items, visitSqlExpr)
                sqlBuilder.append(")")

            case expr: SqlInExpr =>
                visitSqlExpr(expr.expr)
                if (expr.isNot) {
                    sqlBuilder.append(" NOT")
                }
                sqlBuilder.append(" IN ")
                visitSqlExpr(expr.inExpr)

            case expr: SqlBetweenExpr[_] =>
                visitSqlExpr(expr.expr)
                if (expr.isNot) {
                    sqlBuilder.append(" NOT")
                }
                sqlBuilder.append(" BETWEEN ")
                visitSqlExpr(expr.start)
                sqlBuilder.append(" AND ")
                visitSqlExpr(expr.end)

            case expr: SqlCastExpr =>
                sqlBuilder.append("CAST(")
                visitSqlExpr(expr.expr)
                sqlBuilder.append(s" AS ${expr.castType})")

            case expr: SqlExprFunctionExpr =>
                sqlBuilder.append(expr.name)
                sqlBuilder.append("(")
                printList(expr.args, visitSqlExpr)
                sqlBuilder.append(")")

            case expr: SqlSelectQueryExpr =>
                sqlBuilder.append("(")
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                visitSqlSelectQuery(expr.query)
                spaceNum -= 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(")")

            case expr: SqlAggFunctionExpr => visitSqlAggFunctionExpr(expr)

            case expr: SqlOverExpr =>
                visitSqlAggFunctionExpr(expr.agg)
                sqlBuilder.append(" OVER (")
                if (expr.partitionBy.nonEmpty) {
                    sqlBuilder.append("PARTITION BY ")
                    printList(expr.partitionBy, visitSqlExpr)
                }
                if (expr.orderBy.nonEmpty) {
                    if (expr.partitionBy.nonEmpty) {
                        sqlBuilder.append(" ")
                    }
                    sqlBuilder.append("ORDER BY ")
                    printList(expr.orderBy, visitSqlOrderBy)
                }
                sqlBuilder.append(")")

            case expr: SqlCaseExpr =>
                sqlBuilder.append("CASE")
                expr.caseList.foreach { it =>
                    sqlBuilder.append(" WHEN ")
                    visitSqlExpr(it.expr)
                    sqlBuilder.append(" THEN ")
                    visitSqlExpr(it.thenExpr)
                }
                sqlBuilder.append(" ELSE ")
                visitSqlExpr(expr.default)
                sqlBuilder.append(" END")

            case expr: SqlSubQueryPredicateExpr =>
                sqlBuilder.append(expr.predicate.predicate)
                sqlBuilder.append(" ")
                visitSqlExpr(expr.select)

            case _ =>
        }
    }

    def visitSqlAggFunctionExpr(sqlAggFunctionExpr: SqlAggFunctionExpr): Unit = {
        sqlBuilder.append(sqlAggFunctionExpr.name)
        sqlBuilder.append("(")
        if (sqlAggFunctionExpr.distinct) {
            sqlBuilder.append("DISTINCT ")
        }
        if (sqlAggFunctionExpr.name.toUpperCase() == "COUNT" && sqlAggFunctionExpr.args.isEmpty) {
            sqlBuilder.append("*")
        }
        printList(sqlAggFunctionExpr.args, visitSqlExpr)
        if (sqlAggFunctionExpr.orderBy.nonEmpty) {
            sqlBuilder.append(" ORDER BY ")
            printList(sqlAggFunctionExpr.orderBy, visitSqlOrderBy)
        }
        sqlAggFunctionExpr.attributes.foreach { it =>
            sqlBuilder.append(s" ${it._1} ")
            visitSqlExpr(it._2)
        }
        sqlBuilder.append(")")
    }

    def visitSqlTable(sqlTable: SqlTable): Unit = {
        sqlTable match {
            case table: SqlIdentTable => sqlBuilder.append(s"$quote${table.tableName}$quote")

            case table: SqlSubQueryTable =>
                if (table.isLateral) {
                    sqlBuilder.append("LATERAL ")
                }

                sqlBuilder.append("(")
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                visitSqlSelectQuery(table.select)
                spaceNum -= 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(")")

            case table: SqlJoinTable =>
                visitSqlTable(table.left)
                spaceNum += 4
                sqlBuilder.append("\n")
                printSpace(spaceNum)
                sqlBuilder.append(s"${table.joinType.joinType} ")
                table.right match {
                    case _: SqlJoinTable => sqlBuilder.append("(")
                    case _ =>
                }
                visitSqlTable(table.right)
                table.right match {
                    case _: SqlJoinTable => sqlBuilder.append(")")
                    case _ =>
                }

                table.on.foreach { it =>
                    sqlBuilder.append(" ON ")
                    visitSqlExpr(it)
                }
                spaceNum -= 4

            case _ =>
        }

        sqlTable.alias.foreach { it =>
            sqlBuilder.append(s" $quote$it$quote")
            if (sqlTable.columnAliasNames.nonEmpty) {
                sqlBuilder.append("(")
                printList(sqlTable.columnAliasNames.map { alias => SqlIdentExpr(alias) }.toList
                    , { column => visitSqlExpr(column) })
                sqlBuilder.append(")")
            }
        }
    }

    def visitSqlSelectItem(sqlSelectItem: SqlSelectItem): Unit = {
        visitSqlExpr(sqlSelectItem.expr)
        sqlSelectItem.alias.foreach(it => sqlBuilder.append(s" AS $quote$it$quote"))
    }

    def printSpace(num: Int): Unit = {
        if (num > 0) {
            for (_ <- 1 to num) {
                sqlBuilder.append(" ")
            }
        }
    }

    protected inline def printList[T <: SqlNode](list: List[T], handle: T => Unit): Unit = {
        for (i <- 0 until list.size) {
            handle(list(i))
            if (i < list.size - 1) {
                sqlBuilder.append(", ")
            }
        }
    }
}
