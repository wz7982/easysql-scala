package easysql.query.select

import easysql.ast.statement.SqlQuery
import easysql.ast.SqlDataType
import easysql.dsl.*
import easysql.util.exprToSqlExpr

class Values[T <: Tuple](
    private[easysql] override val ast: SqlQuery.SqlValues
) extends Query[T, EmptyTuple](ast, Map()) {
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
        new Values[T](SqlQuery.SqlValues(Nil)).addRow(row)
        
    def apply[T <: Tuple](rows: List[T]): Values[T] = 
        new Values[T](SqlQuery.SqlValues(Nil)).addRows(rows)
}