package easysql.query.select

import easysql.ast.SqlDataType
import easysql.ast.limit.SqlLimit
import easysql.ast.order.SqlOrderBy
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.database.DB
import easysql.dsl.*
import easysql.query.{ToCountSql, ToPageSql}
import easysql.util.*

import scala.Tuple.Concat

class Select[T <: Tuple, A <: Tuple](
    private[easysql] val ast: SqlSelect,
    private val selectItems: Map[String, String], 
    private val joinLeft: Option[SqlTable]
) extends Query[T, A] {
    override def getAst: SqlQuery = 
        ast

    override def getSelectItems: Map[String, String] = 
        selectItems

    infix def from(table: TableSchema[?]): Select[T, A] = {
        val fromTable = SqlIdentTable(table.__tableName, table.__aliasName)
        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    infix def from(table: AliasQuery[?, ?])(using inWithQuery: InWithQuery = NotIn): Select[T, A] = {
        val fromTable = 
            if inWithQuery == In
            then SqlIdentTable(table.__queryName, None)
            else SqlSubQueryTable(table.__ast, false, Some(table.__queryName))
        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    infix def fromLateral(table: AliasQuery[?, ?]): Select[T, A] = {
        val fromTable = SqlSubQueryTable(table.__ast, true, Some(table.__queryName))
        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    infix def select[U <: Tuple](items: U): Select[Concat[T, InverseMap[U]], Concat[A, AliasNames[U]]] = {
        val selectInfo = items.toArray.map {
            case expr: Expr[?] => expr match {
                case ColumnExpr(_, columnName, identName) => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map(identName -> columnName)
                case PrimaryKeyExpr(_, columnName, identName, _) => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map(identName -> columnName)
                case _ => List(SqlSelectItem(exprToSqlExpr(expr), None)) -> Map()
            }
            case AliasExpr(expr, name) => List(SqlSelectItem(exprToSqlExpr(expr), Some(name))) -> Map(name -> name)
            case table: TableSchema[?] => table.__cols.map(c => SqlSelectItem(exprToSqlExpr(c), None)) -> Map()
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

    infix def dynamicsSelect(columns: Expr[?] | AliasExpr[?, ?]*): Select[EmptyTuple, EmptyTuple] = {
        val sqlSelectItems = columns.map {
            case e: Expr[?] => SqlSelectItem(exprToSqlExpr(e), None)
            case a: AliasExpr[?, ?] => SqlSelectItem(exprToSqlExpr(a.expr), Some(a.name))
        }

        new Select(ast.copy(select = ast.select ++ sqlSelectItems), selectItems, joinLeft)
    }

    def distinct: Select[T, A] =
        new Select(ast.copy(param = Some("DISTINCT")), selectItems, joinLeft)

    def param(p: String): Select[T, A] =
        new Select(ast.copy(param = Some(p)), selectItems, joinLeft)

    infix def where(expr: Expr[Boolean]): Select[T, A] =
        new Select(ast.addCondition(exprToSqlExpr(expr)), selectItems, joinLeft)

    def where(test: => Boolean, expr: Expr[Boolean]): Select[T, A] =
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

    infix def groupBy(group: Expr[?]*): Select[T, A] = {
        val sqlGroupBy = group.map(exprToSqlExpr)

        new Select(ast.copy(groupBy = ast.groupBy ++ sqlGroupBy), selectItems, joinLeft)
    }

    private def joinClause(table: TableSchema[?], joinType: SqlJoinType): Select[T, A] = {
        val joinTable = SqlIdentTable(table.__tableName, table.__aliasName)

        val fromTable = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable, None)
        }

        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    private def joinClause(table: AliasQuery[?, ?], joinType: SqlJoinType, lateral: Boolean)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = {
        val joinTable = 
            if inWithQuery == In && !lateral
            then SqlIdentTable(table.__queryName, None)
            else SqlSubQueryTable(table.__ast, lateral, Some(table.__queryName))

        val fromTable = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable, None)
        }

        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    private def joinClause(table: JoinTable, joinType: SqlJoinType): Select[T, A] = {
        def unapplyTable(t: AnyTable): SqlTable = t match {
            case ts: TableSchema[?] => 
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

        new Select(ast.copy(from = List(fromTable)), selectItems, Some(fromTable))
    }

    infix def on(expr: Expr[Boolean]): Select[T, A] = {
        val from = ast.from.map {
            case jt: SqlJoinTable => jt.copy(on = Some(exprToSqlExpr(expr)))
            case f => f
        }

        new Select(ast.copy(from = from), selectItems, from.headOption)
    }

    infix def join(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.Join)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.Join, false)
        case t: JoinTable => joinClause(t, SqlJoinType.Join)
    }

    infix def joinLateral(table: AliasQuery[?, ?]): Select[T, A] =
        joinClause(table, SqlJoinType.Join, true)

    infix def leftJoin(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.LeftJoin)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.LeftJoin, false)
        case t: JoinTable => joinClause(t, SqlJoinType.LeftJoin)
    }

    infix def leftJoinLateral(table: AliasQuery[?, ?]): Select[T, A] =
        joinClause(table, SqlJoinType.LeftJoin, true)

    infix def rightJoin(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.RightJoin)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.RightJoin, false)
        case t: JoinTable => joinClause(t, SqlJoinType.RightJoin)
    }

    infix def rightJoinLateral(table: AliasQuery[?, ?]): Select[T, A] =
        joinClause(table, SqlJoinType.RightJoin, true)

    infix def innerJoin(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.InnerJoin)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.InnerJoin, false)
        case t: JoinTable => joinClause(t, SqlJoinType.InnerJoin)
    }

    infix def innerJoinLateral(table: AliasQuery[?, ?]): Select[T, A] =
        joinClause(table, SqlJoinType.InnerJoin, true)

    infix def crossJoin(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.CrossJoin)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.CrossJoin, false)
        case t: JoinTable => joinClause(t, SqlJoinType.CrossJoin)
    }

    infix def crossJoinLateral(table: AliasQuery[?, ?]): Select[T, A] =
        joinClause(table, SqlJoinType.CrossJoin, true)

    infix def fullJoin(table: TableSchema[?] | AliasQuery[?, ?] | JoinTable)(using inWithQuery: InWithQuery = NotIn): Select[T, A] = table match {
        case t: TableSchema[?] => joinClause(t, SqlJoinType.FullJoin)
        case t: AliasQuery[?, ?] => joinClause(t, SqlJoinType.FullJoin, false)
        case t: JoinTable => joinClause(t, SqlJoinType.FullJoin)
    }

    infix def fullJoinLateral(table: AliasQuery[?, ?])(using inWithQuery: InWithQuery = NotIn): Select[T, A] =
        joinClause(table, SqlJoinType.FullJoin, true)

    def forUpdate: Select[T, A] =
        new Select(ast.copy(forUpdate = true), selectItems, joinLeft)
}

object Select {
    def apply(): Select[EmptyTuple, EmptyTuple] =
        new Select(SqlSelect(None, Nil, Nil, None, Nil, Nil, false, None, None), Map(), None)

    given selectToCountSql: ToCountSql[Select[?, ?]] with {
        extension (x: Select[?, ?]) {
            def countSql(db: DB): String = {
                val astCopy = 
                    x.ast.copy(select = SqlSelectItem(exprToSqlExpr(count()), Some("count")) :: Nil, limit = None, orderBy = Nil)

                queryToString(astCopy, db, false)._1
            }

            def preparedCountSql(db: DB): (String, Array[Any]) = {
                val astCopy = 
                    x.ast.copy(select = SqlSelectItem(exprToSqlExpr(count()), Some("count")) :: Nil, limit = None, orderBy = Nil)

                queryToString(astCopy, db, true)
            }
        }
    }

    given selectToPageSql: ToPageSql[Select[?, ?]] with {
        extension (x: Select[?, ?]) {
            def pageSql(pageSize: Int, pageNumber: Int)(db: DB): String = {
                val offset = if pageNumber <= 1 then 0 else pageSize * (pageNumber - 1)
                val limit = SqlLimit(pageSize, offset)
                val astCopy = x.ast.copy(limit = Some(limit))

                queryToString(astCopy, db, false)._1
            }

            def preparedPageSql(pageSize: Int, pageNumber: Int)(db: DB): (String, Array[Any]) = {
                val offset = if pageNumber <= 1 then 0 else pageSize * (pageNumber - 1)
                val limit = SqlLimit(pageSize, offset)
                val astCopy = x.ast.copy(limit = Some(limit))

                queryToString(astCopy, db, true)
            }
        }
    }
}

sealed trait InWithQuery

case object In extends InWithQuery

case object NotIn extends InWithQuery