package easysql.ast.statement

import easysql.ast.table.*
import easysql.ast.expr.*
import easysql.ast.table.SqlTable
import easysql.ast.limit.SqlLimit
import easysql.ast.order.SqlOrderBy

enum SqlStatement {
    case SqlDelete(table: Option[SqlTable.SqlIdentTable], where: Option[SqlExpr])
    case SqlInsert(table: Option[SqlTable.SqlIdentTable], columns: List[SqlExpr], values: List[List[SqlExpr]], query: Option[SqlQuery])
    case SqlTruncate(table: Option[SqlTable.SqlIdentTable])
    case SqlUpdate(table: Option[SqlTable.SqlIdentTable], setList: List[(SqlExpr, SqlExpr)], where: Option[SqlExpr])
    case SqlUpsert(
        table: Option[SqlTable.SqlIdentTable], 
        columns: List[SqlExpr], 
        value: List[SqlExpr], 
        pkList: List[SqlExpr], 
        updateList: List[SqlExpr]
    )
    case SqlWith(withList: List[SqlWithItem], recursive: Boolean, query: Option[SqlQuery])
}

object SqlStatement {
    extension (d: SqlDelete) def addCondition(condition: SqlExpr): SqlDelete =
        d.copy(where = d.where.map(SqlExpr.SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

    extension (u: SqlUpdate) def addCondition(condition: SqlExpr): SqlUpdate =
        u.copy(where = u.where.map(SqlExpr.SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))
}

enum SqlQuery {
    case SqlSelect(
        distinct: Boolean, 
        select: List[SqlSelectItem], 
        from: Option[SqlTable], 
        where: Option[SqlExpr],
        groupBy: List[SqlExpr],
        orderBy: List[SqlOrderBy],
        forUpdate: Boolean,
        limit: Option[SqlLimit],
        having: Option[SqlExpr]
    )
    case SqlUnion(left: SqlQuery, unionType: SqlUnionType, right: SqlQuery)
    case SqlValues(values: List[List[SqlExpr]])
}

object SqlQuery {
    extension (s: SqlSelect) {
        def addSelectItem(item: SqlSelectItem): SqlSelect = 
            s.copy(select = s.select.appended(item))

        def addCondition(condition: SqlExpr): SqlSelect =
            s.copy(where = s.where.map(SqlExpr.SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def addHaving(condition: SqlExpr): SqlSelect =
            s.copy(having = s.having.map(SqlExpr.SqlBinaryExpr(_, SqlBinaryOperator.And, condition)).orElse(Some(condition)))

        def combine(that: SqlSelect): SqlSelect = {
            s.copy(
                select = s.select ++ that.select,

                from = for {
                    f <- s.from
                    tf <- that.from
                } yield SqlTable.SqlJoinTable(f, SqlJoinType.InnerJoin, tf, None),

                where = (s.where, that.where) match {
                    case (Some(w), Some(tw)) => Some(SqlExpr.SqlBinaryExpr(w, SqlBinaryOperator.And, tw))
                    case (None, tw) => tw
                    case (w, None) => w
                },
                
                groupBy = s.groupBy ++ that.groupBy,

                orderBy = s.orderBy ++ that.orderBy,

                having = (s.having, that.having) match {
                    case (Some(h), Some(th)) => Some(SqlExpr.SqlBinaryExpr(h, SqlBinaryOperator.And, th))
                    case (None, th) => th
                    case (h, None) => h
                }
            )
        }
    }
}