package easysql.query.select

import easysql.ast.expr.*
import easysql.ast.limit.SqlLimit
import easysql.ast.order.SqlOrderBy
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.ast.{SqlDataType, SqlNumberType}
import easysql.database.DB
import easysql.dsl.*
import easysql.query.ToSql
import easysql.util.*

import scala.annotation.targetName

class MonadicQuery[T <: Tuple, From](
    private val query: SqlSelect,
    private val from: From
) {
    def filter(f: From => Expr[Boolean]): MonadicQuery[T, From] = {
        val expr = f(from)

        val where = exprToSqlExpr(expr)
        
        new MonadicQuery(query.addCondition(where), from)
    }

    def withFilter(f: From => Expr[Boolean]): MonadicQuery[T, From] =
        filter(f)

    def map[MT <: Tuple](f: From => MT): MonadicQuery[InverseMap[MT], None.type] = {
        val selectList = f(from)

        val selectInfo = selectList.toArray.map {
            case expr: Expr[_] => expr match {
                case ColumnExpr(_, columnName, identName) => List(SqlSelectItem(exprToSqlExpr(expr), None))
                case PrimaryKeyExpr(_, columnName, identName, _) => List(SqlSelectItem(exprToSqlExpr(expr), None))
                case _ => List(SqlSelectItem(exprToSqlExpr(expr), None))
            }
            case AliasExpr(expr, name) => List(SqlSelectItem(exprToSqlExpr(expr), Some(name)))
            case table: TableSchema[_] => table.__cols.map(c => SqlSelectItem(exprToSqlExpr(c), None))
        }.toList

        val sqlSelectItems = for {
            info <- selectInfo
            item <- info
        } yield item

        new MonadicQuery(query.copy(select = sqlSelectItems), None)
    }

    @targetName("mapExpr")
    def map[I <: SqlDataType](f: From => Expr[I]): MonadicQuery[Tuple1[I], None.type] = {
        val expr = f(from)

        val sqlSelectItem = SqlSelectItem(exprToSqlExpr(expr), None)

        new MonadicQuery(query.copy(select = sqlSelectItem :: Nil), None)
    }

    @targetName("mapAliasExpr")
    def map[I <: SqlDataType, N <: String](f: From => AliasExpr[I, N]): MonadicQuery[Tuple1[I], None.type] = {
        val expr = f(from)

        val sqlSelectItem = SqlSelectItem(exprToSqlExpr(expr.expr), Some(expr.name))

        new MonadicQuery(query.copy(select = sqlSelectItem :: Nil), None)
    }

    def flatMap[MT <: Tuple](f: From => MonadicQuery[MT, _]): MonadicQuery[MT, None.type] = {
        val thatQuery = f(from)

        new MonadicQuery(this.query.combine(thatQuery.query).copy(select = thatQuery.query.select), None)
    }

    def drop(n: Int): MonadicQuery[T, From] = {
        val sqlLimit = query.limit.map(l => SqlLimit(l.limit, n)).orElse(Some(SqlLimit(1, n)))

        new MonadicQuery(this.query.copy(limit = sqlLimit), from)
    }

    def take(n: Int): MonadicQuery[T, From] = {
        val sqlLimit = query.limit.map(l => SqlLimit(n, l.offset)).orElse(Some(SqlLimit(n, 0)))

        new MonadicQuery(this.query.copy(limit = sqlLimit), from)
    }

    def groupBy(f: From => Expr[_]): MonadicQuery[T, From] = {
        val expr = f(from)
        val sqlExpr = exprToSqlExpr(expr)

        new MonadicQuery(this.query.copy(groupBy = this.query.groupBy ++ (sqlExpr :: Nil)), from)
    }

    def sortBy(f: From => OrderBy): MonadicQuery[T, From] = {
        val order = f(from)
        val sqlExpr = exprToSqlExpr(order._1)

        new MonadicQuery(this.query.copy(orderBy = this.query.orderBy ++ (SqlOrderBy(sqlExpr, order._2) :: Nil)), from)
    }

    private def join[E <: Product](
        table: TableSchema[E], 
        joinType: SqlJoinType, 
        on: MonadicJoin[From, TableSchema[E]] => Expr[Boolean]
    ): MonadicQuery[Tuple.Concat[T, Tuple1[E]], MonadicJoin[From, TableSchema[E]]] = {
        val from = {
            this.from match {
                case t: TableSchema[_] => (t, table)
                case t: Tuple => t ++ Tuple1(table)
            }
        }.asInstanceOf[MonadicJoin[From, TableSchema[E]]]

        val onExpr = on(from)
        val sqlOnExpr = exprToSqlExpr(onExpr)

        val fromTable = this.query.from.map { f => 
            SqlJoinTable(f, joinType, SqlIdentTable(table.__tableName, table.__aliasName), Some(sqlOnExpr))
        }
        val sqlSelectItems = table.__cols.map { c =>
            SqlSelectItem(exprToSqlExpr(c), None)
        }

        new MonadicQuery(this.query.copy(select = this.query.select ++ sqlSelectItems, from = fromTable), from)
    }

    def innerJoin[E <: Product](
        table: TableSchema[E]
    )(
        on: MonadicJoin[From, TableSchema[E]] => Expr[Boolean]
    ): MonadicQuery[Tuple.Concat[T, Tuple1[E]], MonadicJoin[From, TableSchema[E]]] =
        join(table, SqlJoinType.InnerJoin, on)

    def leftJoin[E <: Product](
        table: TableSchema[E]
    )(
        on: MonadicJoin[From, TableSchema[E]] => Expr[Boolean]
    ): MonadicQuery[Tuple.Concat[T, Tuple1[E]], MonadicJoin[From, TableSchema[E]]] =
        join(table, SqlJoinType.LeftJoin, on)

    def rightJoin[E <: Product](
        table: TableSchema[E]
    )(
        on: MonadicJoin[From, TableSchema[E]] => Expr[Boolean]
    ): MonadicQuery[Tuple.Concat[T, Tuple1[E]], MonadicJoin[From, TableSchema[E]]] =
        join(table, SqlJoinType.RightJoin, on)

    def fullJoin[E <: Product](
        table: TableSchema[E]
    )(
        on: MonadicJoin[From, TableSchema[E]] => Expr[Boolean]
    ): MonadicQuery[Tuple.Concat[T, Tuple1[E]], MonadicJoin[From, TableSchema[E]]] =
        join(table, SqlJoinType.FullJoin, on)

    def having(f: From => Expr[Boolean]): MonadicQuery[T, From] = {
        val expr = f(from)

        val having = exprToSqlExpr(expr)
        
        new MonadicQuery(query.addHaving(having), from)
    }

    def size: MonadicQuery[Tuple1[Long], None.type] = {
        val expr = SqlExprFuncExpr("COUNT", Nil)
        val selectItem = SqlSelectItem(expr, None)

        new MonadicQuery(query.copy(select = selectItem :: Nil), None)
    }

    def max[I <: SqlDataType](f: From => Expr[I]): MonadicQuery[Tuple1[I], None.type] = {
        val expr = SqlExprFuncExpr("MAX", exprToSqlExpr(f(from)) :: Nil)
        val selectItem = SqlSelectItem(expr, None)

        new MonadicQuery(query.copy(select = selectItem :: Nil), None)
    }

    def min[I <: SqlDataType](f: From => Expr[I]): MonadicQuery[Tuple1[I], None.type] = {
        val expr = SqlExprFuncExpr("MIN", exprToSqlExpr(f(from)) :: Nil)
        val selectItem = SqlSelectItem(expr, None)

        new MonadicQuery(query.copy(select = selectItem :: Nil), None)
    }

    def sum[I <: SqlNumberType](f: From => Expr[I]): MonadicQuery[Tuple1[BigDecimal], None.type] = {
        val expr = SqlExprFuncExpr("SUM", exprToSqlExpr(f(from)) :: Nil)
        val selectItem = SqlSelectItem(expr, None)

        new MonadicQuery(query.copy(select = selectItem :: Nil), None)
    }

    def avg[I <: SqlNumberType](f: From => Expr[I]): MonadicQuery[Tuple1[BigDecimal], None.type] = {
        val expr = SqlExprFuncExpr("AVG", exprToSqlExpr(f(from)) :: Nil)
        val selectItem = SqlSelectItem(expr, None)

        new MonadicQuery(query.copy(select = selectItem :: Nil), None)
    }
}

object MonadicQuery {
    def apply[E <: Product](table: TableSchema[E]): MonadicQuery[Tuple1[E], table.type] = {
        val fromTable = SqlIdentTable(table.__tableName, table.__aliasName)
        val sqlSelectItems = table.__cols.map { c =>
            SqlSelectItem(exprToSqlExpr(c), None)
        }
        val query: SqlSelect = SqlSelect(false, sqlSelectItems, Some(fromTable), None, Nil, Nil, false, None, None)
        
        new MonadicQuery(query, table)
    }

    given monadicQueryToSql: ToSql[MonadicQuery[_, _]] with {
        extension (x: MonadicQuery[_, _]) {
            def sql(db: DB): String =
                queryToString(x.query, db, false)._1

            def preparedSql(db: DB): (String, Array[Any]) =
                queryToString(x.query, db, true)
        }
    }

    extension [T <: SqlDataType] (q: MonadicQuery[Tuple1[T], _]) {
        def exists: MonadicQuery[Tuple1[Boolean], None.type] = {
            val expr = SqlExprFuncExpr("EXISTS", SqlQueryExpr(q.query) :: Nil)
            val selectItem = SqlSelectItem(expr, None)
            val newQuery: SqlSelect = SqlSelect(false, selectItem :: Nil, None, None, Nil, Nil, false, None, None)

            new MonadicQuery(newQuery, None)
        }
    }
}