package easysql.query.select

import easysql.ast.statement.SqlQuery
import easysql.ast.table.SqlTable
import easysql.ast.statement.SqlUnionType
import easysql.dsl.NonEmpty
import easysql.dsl.TableSchema
import easysql.query.ToSql
import easysql.database.DB
import easysql.util.queryToString

class Union[T <: Tuple, A <: Tuple](
    private val selectItems: Map[String, String], 
    private[select] val left: SqlQuery, 
    private[select] val op: SqlUnionType, 
    private[select] val right: SqlQuery
) {
    def unsafeAs(name: String): AliasQuery[T, A] =
        AliasQuery(this.selectItems, SqlQuery.SqlUnion(left, op, right), name)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasQuery[T, A] =
        AliasQuery(this.selectItems, SqlQuery.SqlUnion(left, op, right), name)
}

object Union {
    given unionQuery[T <: Tuple, A <: Tuple]: Query[T, A, Union] with {
        def ast(query: Union[T, A]): SqlQuery =
            SqlQuery.SqlUnion(query.left, query.op, query.right)
    
        def selectItems(query: Union[T, A]): Map[String, String] = 
            query.selectItems
    }

    given unionToSql: ToSql[Union[_, _]] with {
        extension (x: Union[_, _]) def sql(db: DB): String =
            queryToString(SqlQuery.SqlUnion(x.left, x.op, x.right), db)
    }
}