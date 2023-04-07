package easysql.query.nonselect

import easysql.ast.SqlDataType
import easysql.ast.expr.*
import easysql.ast.statement.{SqlStatement, SqlUpsert}
import easysql.ast.table.SqlIdentTable
import easysql.database.DB
import easysql.dsl.*
import easysql.macros.*
import easysql.query.ToSql
import easysql.util.*

class Save(private val ast: SqlUpsert) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    inline def save[T <: Product](entity: T): Save = {
        val (pkList, colList) = updateMetaData[T](entity)

        val tableName = fetchTableName[T]
        val table = Some(SqlIdentTable(tableName, None))

        val pkInfo = pkList.map { (pkName, pkValue) =>
            SqlIdentExpr(pkName) -> exprToSqlExpr(LiteralExpr(pkValue))
        }

        val updateInfo = colList.map { (colName, colValue) =>
            val col = SqlIdentExpr(colName)
            val value = colValue match {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
                case o: Option[SqlDataType] => exprToSqlExpr(o.map(LiteralExpr(_)).getOrElse(NullExpr))
            }
            col -> value
        }

        val pkColumns = pkInfo.map(_._1)
        val updateColumns = updateInfo.map(_._1)
        val columns = pkColumns ++ updateColumns
        val values = pkInfo.map(_._2) ++ updateInfo.map(_._2)

        new Save(SqlUpsert(table, columns, values, pkColumns, updateColumns))
    }
}

object Save {
    def apply(): Save = 
        new Save(SqlUpsert(None, Nil, Nil, Nil, Nil))
}