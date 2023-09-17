package easysql.query.nonselect

import easysql.ast.SqlDataType
import easysql.ast.expr.*
import easysql.ast.statement.{SqlStatement, SqlUpdate}
import easysql.ast.table.SqlIdentTable
import easysql.dsl.*
import easysql.macros.*
import easysql.util.*

class Update(private val ast: SqlUpdate) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    inline def update[T <: Product](entity: T, skipNone: Boolean): Update = {
        val (pkList, colList) = updateMetaData[T](entity)

        val tableName = fetchTableName[T]
        val table = Some(SqlIdentTable(tableName, None))

        val where = pkList map { (pkName, pkValue) =>
            SqlBinaryExpr(SqlIdentExpr(pkName), SqlBinaryOperator.Eq, exprToSqlExpr(LiteralExpr(pkValue)))
        } reduce { (x, y) =>
            SqlBinaryExpr(x, SqlBinaryOperator.And, y)
        }

        val updateInfo = colList.map { (colName, colValue) =>
            val col = SqlIdentExpr(colName)

            val value = colValue match {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
                case o: Option[SqlDataType] => exprToSqlExpr(o.map(LiteralExpr(_)).getOrElse(NullExpr))
            }
            col -> value
        }

        val updateList = if !skipNone then updateInfo else updateInfo.filter { u =>
            u._2 match {
                case SqlNullExpr => false
                case _ => true
            }
        }

        if (updateList.isEmpty) {
            throw Exception("no fields need to be updated in the entity class")
        }

        new Update(SqlUpdate(table, updateList, Some(where)))
    }

    def update(table: TableSchema[?]): Update =
        new Update(ast.copy(table = Some(SqlIdentTable(table.__tableName, None))))

    infix def set(items: (ColumnExpr[?, ?], Expr[?])*): Update = {
        val updateList = items map { (column, value) =>
            val columnExpr = SqlIdentExpr(column.columnName)
            val valueExpr = exprToSqlExpr(value)
            columnExpr -> valueExpr
        }

        new Update(ast.copy(setList = updateList.toList))
    }

    infix def where(expr: Expr[Boolean]): Update =
        new Update(ast.addCondition(exprToSqlExpr(expr)))

    infix def where(test: () => Boolean, expr: Expr[Boolean]): Update =
        if test() then where(expr) else this

    infix def where(test: Boolean, expr: Expr[Boolean]): Update =
        if test then where(expr) else this
}

object Update {
    def apply(): Update = 
        new Update(SqlUpdate(None, Nil, None))
}