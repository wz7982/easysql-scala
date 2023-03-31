package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.table.SqlTable.*
import easysql.ast.SqlDataType
import easysql.ast.expr.SqlBinaryOperator
import easysql.ast.expr.SqlExpr.*
import easysql.query.ToSql
import easysql.dsl.*
import easysql.macros.*
import easysql.util.*
import easysql.database.DB

class Delete(private val ast: SqlStatement.SqlDelete) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    infix def deleteFrom(table: TableSchema[_]): Delete =
        new Delete(ast.copy(table = Some(SqlIdentTable(table.__tableName, None))))

    inline def delete[T <: Product](pk: SqlDataType | Tuple): Delete = {
        val (tableName, cols) = fetchPk[T, pk.type]
        val table = Some(SqlIdentTable(tableName, None))

        val conditions = inline pk match {
            case t: Tuple => t.toArray.toList.zip(cols).map { (p, c) =>
                p match {
                    case d: SqlDataType => exprToSqlExpr(BinaryExpr(IdentExpr(c), SqlBinaryOperator.Eq, LiteralExpr(d)))
                }
            }
            case d: SqlDataType => List(exprToSqlExpr(BinaryExpr(IdentExpr(cols.head), SqlBinaryOperator.Eq, LiteralExpr(d))))
        }

        val where = conditions.reduce((x, y) => SqlBinaryExpr(x, SqlBinaryOperator.And, y))
        
        new Delete(ast.copy(table = Some(SqlIdentTable(tableName, None)), where = Some(where)))
    }

    infix def where(expr: Expr[Boolean]): Delete =
        new Delete(ast.addCondition(exprToSqlExpr(expr)))

    infix def where(test: () => Boolean, expr: Expr[Boolean]): Delete =
        if test() then where(expr) else this

    infix def where(test: Boolean, expr: Expr[Boolean]): Delete =
        if test then where(expr) else this
}

object Delete {
    def apply(): Delete = 
        new Delete(SqlStatement.SqlDelete(None, None))
}