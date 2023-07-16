package easysql.ast.statement

import easysql.ast.expr.*
import easysql.ast.limit.SqlLimit
import easysql.ast.order.SqlOrderBy
import easysql.ast.table.*

sealed trait SqlStatement

case class SqlDelete(table: Option[SqlIdentTable], where: Option[SqlExpr]) extends SqlStatement {
    def addCondition(condition: SqlExpr): SqlDelete =
        this.copy(where = this.where.map(SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))
}

case class SqlInsert(
    table: Option[SqlIdentTable],
    columns: List[SqlExpr],
    values: List[List[SqlExpr]],
    query: Option[SqlQuery]
) extends SqlStatement

case class SqlTruncate(table: Option[SqlIdentTable]) extends SqlStatement

case class SqlUpdate(
    table: Option[SqlIdentTable],
    setList: List[(SqlExpr, SqlExpr)],
    where: Option[SqlExpr]
) extends SqlStatement {
    def addCondition(condition: SqlExpr): SqlUpdate =
        this.copy(where = this.where.map(SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))
}

case class SqlUpsert(
    table: Option[SqlIdentTable],
    columns: List[SqlExpr],
    value: List[SqlExpr],
    pkList: List[SqlExpr],
    updateList: List[SqlExpr]
) extends SqlStatement

sealed trait SqlQuery

case class SqlSelect(
    param: Option[String],
    select: List[SqlSelectItem],
    from: List[SqlTable],
    where: Option[SqlExpr],
    groupBy: List[SqlExpr],
    orderBy: List[SqlOrderBy],
    forUpdate: Boolean,
    limit: Option[SqlLimit],
    having: Option[SqlExpr]
) extends SqlQuery {
    def addSelectItem(item: SqlSelectItem): SqlSelect =
        this.copy(select = this.select.appended(item))

    def addCondition(condition: SqlExpr): SqlSelect =
        this.copy(where = this.where.map(SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    def addHaving(condition: SqlExpr): SqlSelect =
        this.copy(having = this.having.map(SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    def combine(that: SqlSelect): SqlSelect = {
        this.copy(
            select = this.select ++ that.select,

            from = this.from ++ that.from,

            where = (this.where, that.where) match {
                case (Some(w), Some(tw)) => Some(SqlBinaryExpr(w, SqlBinaryOperator.And, tw))
                case (None, tw) => tw
                case (w, None) => w
            },

            groupBy = this.groupBy ++ that.groupBy,

            orderBy = this.orderBy ++ that.orderBy,

            having = (this.having, that.having) match {
                case (Some(h), Some(th)) => Some(SqlBinaryExpr(h, SqlBinaryOperator.And, th))
                case (None, th) => th
                case (h, None) => h
            }
        )
    }
}

case class SqlUnion(left: SqlQuery, unionType: SqlUnionType, right: SqlQuery) extends SqlQuery

case class SqlValues(values: List[List[SqlExpr]]) extends SqlQuery

case class SqlWith(withList: List[SqlWithItem], recursive: Boolean, query: Option[SqlQuery]) extends SqlQuery