package easysql.query.select

import easysql.ast.statement.SqlQuery
import easysql.ast.table.SqlTable
import easysql.ast.statement.SqlUnionType
import easysql.dsl.NonEmpty
import easysql.dsl.TableSchema

class Union[T <: Tuple, A <: Tuple](
    private[select] override val selectItems: Map[String, String], 
    private val left: Query[_, _], 
    private val op: SqlUnionType, 
    private val right: Query[_, _]
) extends Query[T, A](SqlQuery.SqlUnion(left.ast, op, right.ast), selectItems) {
    def unsafeAs(name: String): AliasQuery[T, A] =
        AliasQuery(this, name)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasQuery[T, A] =
        AliasQuery(this, name)
}