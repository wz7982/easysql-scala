package easysql.query.select

import easysql.ast.table.SqlTable
import easysql.ast.table.SqlTable.*
import easysql.ast.statement.SqlQuery
import easysql.ast.statement.*
import easysql.ast.SqlDataType
import easysql.ast.order.SqlOrderBy
import easysql.ast.limit.SqlLimit
import easysql.dsl.*
import easysql.util.*
import easysql.query.ToCountSql
import easysql.query.ToPageSql
import easysql.database.DB

import scala.Tuple.Concat
import easysql.ast.table.SqlJoinType

class Select[T <: Tuple, A <: Tuple](
    private[easysql] override val ast: SqlQuery.SqlSelect, 
    private[select] override val selectItems: Map[String, String], 
    private val joinLeft: Option[SqlTable]
) extends Query[T, A](ast, selectItems) {
    infix def from(table: TableSchema[_]): Select[T, A] = {
        val fromTable = SqlTable.SqlIdentTable(table.__tableName, table.__aliasName)
        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    infix def from(table: AliasQuery[_, _]): Select[T, A] = {
        val fromTable = SqlTable.SqlSubQueryTable(table.__query.ast, false, Some(table.__queryName))
        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    infix def fromLateral(table: AliasQuery[_, _]): Select[T, A] = {
        val fromTable = SqlTable.SqlSubQueryTable(table.__query.ast, true, Some(table.__queryName))
        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    infix def select[U <: Tuple](items: U): Select[Concat[T, InverseMap[U]], Concat[A, AliasNames[U]]] = {
        val selectInfo = items.toArray.map {
            case expr: Expr[_] => expr match {
                case ColumnExpr(_, columnName, identName) => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map(identName -> columnName)
                case PrimaryKeyExpr(_, columnName, identName, _) => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map(identName -> columnName)
                case _ => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map()
            }
            case AliasExpr(expr, name) => List(SqlSelectItem(exprToSqlExpr(expr), Some(name))) -> Map(name -> name)
            case table: TableSchema[_] => table.__cols.map(c => SqlSelectItem(exprToSqlExpr(c), None)) -> Map()
        }.toList

        val sqlSelectItems = for {
            info <- selectInfo
            item <- info._1
        } yield item

        val selectItems = for {
            info <- selectInfo
            item <- info._2
        } yield item

        new Select(ast.copy(select = ast.select ++ sqlSelectItems), this.selectItems ++ selectItems, joinLeft)
    }

    infix def select[I <: SqlDataType, E <: Expr[I]](item: E): Select[Concat[T, Tuple1[I]], Concat[A, AliasNames[Tuple1[E]]]] = {
        val selectInfo = item match {
            case ColumnExpr(_, columnName, identName) => SqlSelectItem(exprToSqlExpr(item), None) -> Map(identName -> columnName)
            case PrimaryKeyExpr(_, columnName, identName, _) => SqlSelectItem(exprToSqlExpr(item), None) -> Map(identName -> columnName)
            case _ => SqlSelectItem(exprToSqlExpr(item), None) -> Map()
        }
        
        new Select(ast.copy(select = ast.select ++ List(selectInfo._1)), this.selectItems ++ selectInfo._2, joinLeft)
    }

    infix def select[I <: SqlDataType, N <: String](item: AliasExpr[I, N]): Select[Concat[T, Tuple1[I]], Concat[A, Tuple1[N]]] = {
        val sqlSelectItem = List(SqlSelectItem(exprToSqlExpr(item.expr), Some(item.name)))
        val selectItem = Map(item.name -> item.name)

        new Select(ast.copy(select = ast.select ++ sqlSelectItem), this.selectItems ++ selectItem, joinLeft)
    }

    infix def select[P <: Product](table: TableSchema[P]): Select[Concat[T, Tuple1[P]], A] = {
        val sqlSelectItems = table.__cols.map { c =>
            SqlSelectItem(exprToSqlExpr(c), None)
        }

        new Select(ast.copy(select = ast.select ++ sqlSelectItems), selectItems, joinLeft)
    }

    infix def dynamicsSelect(columns: Expr[_] | AliasExpr[_, _]*): Select[EmptyTuple, EmptyTuple] = {
        val sqlSelectItems = columns.map {
            case e: Expr[_] => SqlSelectItem(exprToSqlExpr(e), None)
            case a: AliasExpr[_, _] => SqlSelectItem(exprToSqlExpr(a.expr), Some(a.name))
        }

        new Select(ast.copy(select = ast.select ++ sqlSelectItems), selectItems, joinLeft)
    }

    def distinct: Select[T, A] =
        new Select(ast.copy(distinct = true), selectItems, joinLeft)

    infix def where(expr: Expr[Boolean]): Select[T, A] =
        new Select(ast.addCondition(exprToSqlExpr(expr)), selectItems, joinLeft)

    def where(test: () => Boolean, expr: Expr[Boolean]): Select[T, A] = 
        if test() then where(expr) else this

    def where(test: Boolean, expr: Expr[Boolean]): Select[T, A] =
        if test then where(expr) else this

    infix def having(expr: Expr[Boolean]): Select[T, A] =
        new Select(ast.addHaving(exprToSqlExpr(expr)), selectItems, joinLeft)

    infix def orderBy(order: OrderBy*): Select[T, A] = {
        val sqlOrderBy = order.map(o => SqlOrderBy(exprToSqlExpr(o.expr), o.order))
        
        new Select(ast.copy(orderBy = ast.orderBy ++ sqlOrderBy), selectItems, joinLeft)
    }

    infix def limit(count: Int): Select[T, A] = {
        val sqlLimit = ast.limit.map(l => SqlLimit(count, l.offset)).orElse(Some(SqlLimit(count, 0)))

        new Select(ast.copy(limit = sqlLimit), selectItems, joinLeft)
    }

    infix def offset(count: Int): Select[T, A] = {
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, count)).orElse(Some(SqlLimit(1, count)))

        new Select(ast.copy(limit = sqlLimit), selectItems, joinLeft)
    }

    infix def groupBy(group: Expr[_]*): Select[T, A] = {
        val sqlGroupBy = group.map(exprToSqlExpr)

        new Select(ast.copy(groupBy = ast.groupBy ++ sqlGroupBy), selectItems, joinLeft)
    }

    private def joinClause(table: TableSchema[_], joinType: SqlJoinType): Select[T, A] = {
        val joinTable = SqlIdentTable(table.__tableName, table.__aliasName)

        val fromTable = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable, None)
        }

        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    private def joinClause(table: AliasQuery[_, _], joinType: SqlJoinType, lateral: Boolean): Select[T, A] = {
        val joinTable = SqlSubQueryTable(table.__query.ast, lateral, Some(table.__queryName))

        val fromTable = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable, None)
        }

        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    private def joinClause(table: JoinTable, joinType: SqlJoinType): Select[T, A] = {
        def unapplyTable(t: AnyTable): SqlTable = t match {
            case ts: TableSchema[_] => 
                SqlIdentTable(ts.__tableName, ts.__aliasName)
            case jt: JoinTable => 
                SqlJoinTable(unapplyTable(jt.left), jt.joinType, unapplyTable(jt.right), jt.onCondition.map(exprToSqlExpr))
        }

        val joinTable = 
            SqlJoinTable(unapplyTable(table.left), table.joinType, unapplyTable(table.right), table.onCondition.map(exprToSqlExpr))

        val fromTable = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable, None)
        }

        new Select(ast.copy(from = Some(fromTable)), selectItems, Some(fromTable))
    }

    infix def on(expr: Expr[Boolean]): Select[T, A] = {
        val from = ast.from.map { f =>
            f match {
                case jt: SqlJoinTable => jt.copy(on = Some(exprToSqlExpr(expr)))
                case _ => f
            }
        }

        new Select(ast.copy(from = from), selectItems, from)
    }

    infix def join(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.JOIN)
    }

    infix def joinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.JOIN, true)

    infix def leftJoin(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.LEFT_JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.LEFT_JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.LEFT_JOIN)
    }

    infix def leftJoinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.LEFT_JOIN, true)

    infix def rightJoin(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.RIGHT_JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.RIGHT_JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.RIGHT_JOIN)
    }

    infix def rightJoinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.RIGHT_JOIN, true)

    infix def innerJoin(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.INNER_JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.INNER_JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.INNER_JOIN)
    }

    infix def innerJoinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.INNER_JOIN, true)

    infix def crossJoin(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.CROSS_JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.CROSS_JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.CROSS_JOIN)
    }

    infix def crossJoinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.CROSS_JOIN, true)

    infix def fullJoin(table: TableSchema[_] | AliasQuery[_, _] | JoinTable): Select[T, A] = table match {
        case t: TableSchema[_] => joinClause(t, SqlJoinType.FULL_JOIN)
        case t: AliasQuery[_, _] => joinClause(t, SqlJoinType.FULL_JOIN, false)
        case t: JoinTable => joinClause(t, SqlJoinType.FULL_JOIN)
    }

    infix def fullJoinLateral(table: AliasQuery[_, _]): Select[T, A] =
        joinClause(table, SqlJoinType.FULL_JOIN, true)

    def forUpdate: Select[T, A] =
        new Select(ast.copy(forUpdate = true), selectItems, joinLeft)

    def unsafeAs(name: String): AliasQuery[T, A] =
        AliasQuery(this, name)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasQuery[T, A] =
        AliasQuery(this, name)
}

object Select {
    def apply(): Select[EmptyTuple, EmptyTuple] =
        new Select(SqlQuery.SqlSelect(false, Nil, None, None, Nil, Nil, false, None, None), Map(), None)

    given selectToCountSql[T <: Tuple, A <: Tuple]: ToCountSql[Select[T, A]] with {
        extension (s: Select[T, A]) def countSql(db: DB): String = {
            val astCopy = 
                s.ast.copy(select = SqlSelectItem(exprToSqlExpr(count()), Some("count")) :: Nil, limit = None, orderBy = Nil)

            queryToString(astCopy, db)
        }
    }

    given selectToPageSql[T <: Tuple, A <: Tuple]: ToPageSql[Select[T, A]] with {
        extension (s: Select[T, A]) def pageSql(pageSize: Int, pageNumber: Int)(db: DB): String = {
            val offset = if pageNumber <= 1 then 0 else pageSize * (pageNumber - 1)
            val limit = SqlLimit(pageSize, offset)
            val astCopy = s.ast.copy(limit = Some(limit))

            queryToString(astCopy, db)
        }
    }
}