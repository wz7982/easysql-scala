package easysql.printer

import easysql.ast.expr.*
import easysql.ast.limit.SqlLimit
import easysql.ast.order.SqlOrderByOption
import easysql.ast.statement.{SqlSelect, SqlSelectItem}
import easysql.ast.table.SqlIdentTable

import scala.collection.mutable

class MongoPrinter {
    val dslBuilder: mutable.StringBuilder = mutable.StringBuilder()

    var spaceNum = 0

    def printSelect(s: SqlSelect): Unit = {
        s.from match {
            case Some(SqlIdentTable(t, None)) => dslBuilder.append(s"db.$t.find(")
            case _ =>
        }
        s.where match {
            case Some(expr: SqlBinaryExpr) => {
                printBinaryExpr(expr)
                dslBuilder.append(", ")
            }
            case Some(expr: SqlInExpr) => {
                printInExpr(expr)
                dslBuilder.append(", ")
            }
            case _ => dslBuilder.append("{}, ")
        }
        if (s.select.isEmpty) {
            dslBuilder.append("{}")
        } else {
            dslBuilder.append("{")
            val selectItems = s.select.filter { item =>
                item.expr match {
                    case _: SqlIdentExpr => true
                    case _: SqlPropertyExpr => true
                    case _ => false
                }
            }.map { item =>
                item.expr match {
                    case SqlIdentExpr(name) => s"\"$name\": 1"
                    case SqlPropertyExpr(_, name) => s"\"$name\": 1"
                    case _ => ""
                }
            }.reduce((i, acc) => i + ", " + acc)
            dslBuilder.append(s"$selectItems}")
        }
        dslBuilder.append(")")
        if (s.orderBy.nonEmpty) {
            dslBuilder.append(".sort({")
            for (i <- s.orderBy.indices) {
                printExpr(s.orderBy(i).expr)
                dslBuilder.append(": ")
                if (s.orderBy(i).order == SqlOrderByOption.Asc) {
                    dslBuilder.append("1")
                } else {
                    dslBuilder.append("-1")
                }
                if (i < s.orderBy.size - 1) {
                    dslBuilder.append(", ")
                }
            }
            dslBuilder.append("})")
        }
        s.limit.foreach { l =>
            dslBuilder.append(s".limit(${l.limit}).skip(${l.offset})")
        }
    }

    def printExpr(e: SqlExpr): Unit = {
        e match {
            case SqlIdentExpr(name) => dslBuilder.append(s"\"$name\"")
            case SqlPropertyExpr(_, name) => dslBuilder.append(s"\"$name\"")
            case SqlNumberExpr(n) => dslBuilder.append(n.toString)
            case SqlCharExpr(c) => dslBuilder.append(s"\"$c\"")
            case SqlBooleanExpr(b) => dslBuilder.append(b.toString)
            case d: SqlDateExpr => dslBuilder.append(d.toString.replaceAll("'", "\""))
            case b: SqlBinaryExpr => printBinaryExpr(b)
            case l: SqlListExpr => {
                dslBuilder.append("[")
                for (i <- l.items.indices) {
                    printExpr(l.items(i))
                    if (i < l.items.size - 1) {
                        dslBuilder.append(", ")
                    }
                }
                dslBuilder.append("]")
            }
            case i: SqlInExpr => printInExpr(i)
            case _ =>
        }
    }

    def printInExpr(e: SqlInExpr): Unit = {
        val operator = if e.not then "$nin" else "$in"
        dslBuilder.append("{")
        printExpr(e.expr)
        dslBuilder.append(s": {$operator: ")
        printExpr(e.inExpr)
        dslBuilder.append("}}")
    }

    def printBinaryExpr(e: SqlBinaryExpr): Unit = {
        import easysql.ast.expr.SqlBinaryOperator.*

        e.op match {
            case And => {
                dslBuilder.append("{$and: [")
                printExpr(e.left)
                dslBuilder.append(", ")
                printExpr(e.right)
                dslBuilder.append("]}")
            }
            case Or => {
                dslBuilder.append("{$or: [")
                printExpr(e.left)
                dslBuilder.append(", ")
                printExpr(e.right)
                dslBuilder.append("]}")
            }
            case Eq => {
                dslBuilder.append("{")
                printExpr(e.left)
                dslBuilder.append(": ")
                printExpr(e.right)
                dslBuilder.append("}")
            }
            case Ne | Gt | Ge | Lt | Le => {
                val operator = e.op match {
                    case Ne => "$ne"
                    case Gt => "$gt"
                    case Ge => "$gte"
                    case Lt => "$lt"
                    case Le => "$lte"
                    case _ => ""
                }
                dslBuilder.append("{")
                printExpr(e.left)
                dslBuilder.append(s": {$operator: ")
                printExpr(e.right)
                dslBuilder.append("}}")
            }
            case _ =>
        }
    }
}