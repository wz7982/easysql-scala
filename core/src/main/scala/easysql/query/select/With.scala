package easysql.query.select

import easysql.ast.statement.*
import easysql.ast.expr.SqlExpr.*
import easysql.query.ToSql
import easysql.database.DB
import easysql.util.statementToString

class With[T <: Tuple](
    private val ast: SqlStatement.SqlWith
) {
    def addTable(query: AliasQuery[_, _]*): With[T] = {
        val withInfo = query.toList map { q =>
            SqlWithItem(SqlIdentExpr(q.__queryName), q.__ast, q.__selectItems.values.map(SqlIdentExpr(_)).toList)
        }
        new With(ast.copy(withList = ast.withList ++ withInfo))
    }

    def recursive: With[T] =
        new With(ast.copy(recursive = true))

    def query[QT <: Tuple, A <: Tuple, Q[_, _]](query: Q[QT, A])(using q: Query[QT, A, Q]): With[QT] =
        new With(ast.copy(query = Some(q.ast(query))))
}

object With {
    def apply(): With[EmptyTuple] = 
        new With[EmptyTuple](SqlStatement.SqlWith(Nil, false, None))

    given withToSql[T <: Tuple]: ToSql[With[T]] with {
        extension (q: With[T]) def sql(db: DB) =
            statementToString(q.ast, db)
    }
}