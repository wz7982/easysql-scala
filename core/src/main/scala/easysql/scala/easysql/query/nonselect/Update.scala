package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.expr.SqlExpr.*
import easysql.ast.expr.SqlBinaryOperator
import easysql.ast.table.SqlTable.SqlIdentTable
import easysql.ast.SqlDataType
import easysql.dsl.*
import easysql.macros.*
import easysql.util.exprToSqlExpr

class Update(
    private[easysql] override val ast: SqlStatement.SqlUpdate
) extends NonSelect(ast) {
    inline def update[T <: Product](entity: T, skipNone: Boolean): Update = {
        val (tableName, pkList, colList) = updateMetaData[T]

        val table = Some(new SqlIdentTable(tableName, None))

        val where = pkList map { (pkName, fun) =>
            SqlBinaryExpr(SqlIdentExpr(pkName), SqlBinaryOperator.EQ, exprToSqlExpr(LiteralExpr(fun.apply(entity))))
        } reduce { (x, y) =>
            SqlBinaryExpr(x, SqlBinaryOperator.AND, y)
        }

        val updateInfo = colList.map { (colName, fun) =>
            val col = SqlIdentExpr(colName)

            val value = fun.apply(entity) match {
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

        new Update(SqlStatement.SqlUpdate(table, updateInfo, Some(where)))
    }

    def update(table: TableSchema[_]): Update =
        new Update(ast.copy(table = Some(SqlIdentTable(table.__tableName, None))))

    infix def set(items: (ColumnExpr[_, _] | IdentExpr[_], Expr[_])*): Update = {
        val updateList = items map { (column, value) =>
            val columnExpr = column match {
                case c: ColumnExpr[_, _] => SqlIdentExpr(c.columnName)
                case IdentExpr(column) => SqlIdentExpr(column)
            }
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
    def apply(): Update = new Update(SqlStatement.SqlUpdate(None, Nil, None))
}