package easysql.query.select

import easysql.ast.SqlDataType
import easysql.ast.statement.SqlValues
import easysql.dsl.*
import easysql.util.exprToSqlExpr

class Values[T <: Tuple](val ast: SqlValues) {
    def getAst: SqlValues = 
        ast

    def addRow(row: T): Values[T] = {
        val add = row.toList.map {
            case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
            case _ => exprToSqlExpr(NullExpr)
        }
        new Values(ast.copy(values = ast.values ++ (add :: Nil)))
    }

    def addRows(rows: List[T]): Values[T] = {
        val add = rows.map { row =>
            row.toList.map {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
                case _ => exprToSqlExpr(NullExpr)
            }
        }
        new Values(ast.copy(values = ast.values ++ add))
    }
}

object Values {
    def apply[T <: Tuple](row: T): Values[T] = 
        new Values[T](SqlValues(Nil)).addRow(row)
        
    def apply[T <: Tuple](rows: List[T]): Values[T] = 
        new Values[T](SqlValues(Nil)).addRows(rows)
}