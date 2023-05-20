package easysql.query.select

import easysql.ast.expr.*
import easysql.ast.statement.*
import easysql.database.DB
import easysql.query.ToSql
import easysql.util.statementToString

class With[T <: Tuple](
    private val ast: SqlWith
) {
    def getAst: SqlWith = 
        ast

    def addTable(query: AliasQuery[_, _]*): With[T] = {
        val withInfo = query.toList map { q =>
            SqlWithItem(SqlIdentExpr(q.__queryName), q.__ast, q.__selectItems.values.map(SqlIdentExpr(_)).toList)
        }
        new With(ast.copy(withList = ast.withList ++ withInfo))
    }

    def recursive: With[T] =
        new With(ast.copy(recursive = true))

    def query[QT <: Tuple, A <: Tuple](query: InWithQuery ?=> Query[QT, A]): With[QT] = {
        given InWithQuery = In
        new With(ast.copy(query = Some(query.getAst)))
    }       
}

object With {
    def apply(): With[EmptyTuple] = 
        new With[EmptyTuple](SqlWith(Nil, false, None))

    given withToSql[T <: Tuple]: ToSql[With[T]] with {
        extension (q: With[T]) {
            def sql(db: DB): String =
                statementToString(q.ast, db, false)._1
            
            def preparedSql(db: DB): (String, Array[Any]) =
                statementToString(q.ast, db, true)
        }
    }
}