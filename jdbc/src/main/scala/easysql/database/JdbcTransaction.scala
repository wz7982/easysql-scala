package easysql.database

import easysql.query.nonselect.*
import easysql.query.select.*
import easysql.jdbc.*
import easysql.dsl.*
import easysql.ast.SqlDataType
import easysql.database.DBOperator.dbMonadId
import easysql.query.ToSql

import java.sql.Connection

class JdbcTransaction(val db: DB, val conn: Connection)

object JdbcTransaction {
    import easysql.database.DBOperator.dbMonadId

    given jdbcTransaction: DBOperator[JdbcTransaction, Id] with {
        def db(x: JdbcTransaction): DB = 
            x.db

        def runSql(x: JdbcTransaction, sql: String, args: Array[Any]): Id[Int] =
            Id(jdbcExec(x.conn, sql, args))

        def runSqlAndReturnKey(x: JdbcTransaction, sql: String, args: Array[Any]): Id[List[Long]] =
            Id(jdbcExecReturnKey(x.conn, sql, args))

        def querySql(x: JdbcTransaction, sql: String, args: Array[Any]): Id[List[Array[Any]]] =
            Id(jdbcQueryToArray(x.conn, sql, args))

        def querySqlCount(x: JdbcTransaction, sql: String, args: Array[Any]): Id[Long] =
            Id(jdbcQueryToArray(x.conn, sql, args).head.head.toString.toLong)
    }
}

def run[T <: NonSelect](query: T)(using logger: Logger, t: JdbcTransaction): Int = 
    summon[DBOperator[JdbcTransaction, Id]].runMonad(t, query).get

def runAndReturnKey(query: Insert[_, _])(using logger: Logger, t: JdbcTransaction): List[Long] = 
    summon[DBOperator[JdbcTransaction, Id]].runAndReturnKeyMonad(t, query).get

inline def query[T <: Tuple](query: Query[T, _])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad(t, query).get

inline def query[T <: Tuple](query: MonadicQuery[T, _])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad(t, query).get

inline def query[T <: Tuple](query: With[T])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad(t, query).get

inline def query[T <: Tuple](query: NativeSql)(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad[T](t, query).get

inline def querySkipNoneRows[T](query: Query[Tuple1[T], _])(using logger: Logger, t: JdbcTransaction): List[T] = 
    summon[DBOperator[JdbcTransaction, Id]].querySkipNoneRowsMonad(t, query).get

inline def querySkipNoneRows[T](query: MonadicQuery[Tuple1[T], _])(using logger: Logger, t: JdbcTransaction): List[T] = 
    summon[DBOperator[JdbcTransaction, Id]].querySkipNoneRowsMonad(t, query).get

inline def querySkipNoneRows[T](query: With[Tuple1[T]])(using logger: Logger, t: JdbcTransaction): List[T] = 
    summon[DBOperator[JdbcTransaction, Id]].querySkipNoneRowsMonad(t, query).get

inline def querySkipNoneRows[T](query: NativeSql)(using logger: Logger, t: JdbcTransaction): List[T] = 
    summon[DBOperator[JdbcTransaction, Id]].querySkipNoneRowsMonad[T](t, query).get

inline def find[T <: Tuple](query: Select[T, _])(using logger: Logger, t: JdbcTransaction): Option[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].findMonad(t, query).get

inline def page[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger, t: JdbcTransaction): Page[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].pageMonad(t, query)(pageSize, pageNum, queryCount).get

def fetchCount(query: Select[_, _])(using logger: Logger, t: JdbcTransaction): Long = 
    summon[DBOperator[JdbcTransaction, Id]].fetchCountMonad(t, query).get