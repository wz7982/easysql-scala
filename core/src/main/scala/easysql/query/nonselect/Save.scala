package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.expr.SqlExpr.*
import easysql.ast.table.SqlTable.SqlIdentTable
import easysql.ast.SqlDataType
import easysql.dsl.*
import easysql.macros.*
import easysql.util.exprToSqlExpr

class Save(
    private[easysql] override val ast: SqlStatement.SqlUpsert
) extends NonSelect(ast) {
    inline def save[T <: Product](entity: T): Save = {
        val (tableName, pkList, colList) = updateMetaData[T]

        val table = Some(new SqlIdentTable(tableName, None))

        val pkInfo = pkList.map { (pkName, fun) =>
            SqlIdentExpr(pkName) -> exprToSqlExpr(LiteralExpr(fun.apply(entity)))
        }

        val updateInfo = colList.map { (colName, fun) =>
            val col = SqlIdentExpr(colName)
            val value = fun.apply(entity) match {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
                case o: Option[SqlDataType] => exprToSqlExpr(o.map(LiteralExpr(_)).getOrElse(NullExpr))
            }
            col -> value
        }

        val pkColumns = pkInfo.map(_._1)
        val updateColumns = updateInfo.map(_._1)
        val columns = pkColumns ++ updateColumns
        val values = pkInfo.map(_._2) ++ updateInfo.map(_._2)

        new Save(SqlStatement.SqlUpsert(table, columns, values, pkColumns, updateColumns))
    }
}

object Save {
    def apply(): Save = 
        new Save(SqlStatement.SqlUpsert(None, Nil, Nil, Nil, Nil))
}