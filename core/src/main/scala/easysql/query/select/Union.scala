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
) extends Query[T, A] {
    override def getAst: SqlQuery = 
        SqlQuery.SqlUnion(left, op, right)

    override def getSelectItems: Map[String, String] = 
        selectItems
}