package easysql.query.select

import easysql.ast.statement.{SqlQuery, SqlUnion, SqlUnionType}

class Union[T <: Tuple, A <: Tuple](
    private val selectItems: Map[String, String], 
    private[select] val left: SqlQuery, 
    private[select] val op: SqlUnionType, 
    private[select] val right: SqlQuery
) extends Query[T, A] {
    override def getAst: SqlQuery = 
        SqlUnion(left, op, right)

    override def getSelectItems: Map[String, String] = 
        selectItems
}