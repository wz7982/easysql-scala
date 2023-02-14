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

        def runSql(x: JdbcTransaction, sql: String): Id[Int] =
            Id(jdbcExec(x.conn, sql))

        def runSqlAndReturnKey(x: JdbcTransaction, sql: String): Id[List[Long]] =
            Id(jdbcExecReturnKey(x.conn, sql))

        def querySql(x: JdbcTransaction, sql: String): Id[List[Array[Any]]] =
            Id(jdbcQueryToArray(x.conn, sql))

        def querySqlToMap(x: JdbcTransaction, sql: String): Id[List[Map[String, Any]]] =
            Id(jdbcQuery(x.conn, sql))

        def querySqlCount(x: JdbcTransaction, sql: String): Id[Long] =
            Id(jdbcQuery(x.conn, sql).head.head._2.toString().toLong)
    }
}

def run[T : NonSelect : ToSql](query: T)(using logger: Logger, t: JdbcTransaction): Int = 
    summon[DBOperator[JdbcTransaction, Id]].runMonad(t, query).get

def runAndReturnKey(query: Insert[_, _])(using logger: Logger, t: JdbcTransaction): List[Long] = 
    summon[DBOperator[JdbcTransaction, Id]].runAndReturnKeyMonad(t, query).get

def query(sql: String)(using logger: Logger, t: JdbcTransaction): List[Map[String, Any]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad(t, sql).get

inline def query[T <: Tuple](query: Select[T, _])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].queryMonad(t, query).get

inline def querySkipNoneRows[T <: Tuple](query: Select[Tuple1[T], _])(using logger: Logger, t: JdbcTransaction): List[T] = 
    summon[DBOperator[JdbcTransaction, Id]].querySkipNoneRowsMonad(t, query).get

inline def find[T <: Tuple](query: Select[T, _])(using logger: Logger, t: JdbcTransaction): Option[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].findMonad(t, query).get

inline def page[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger, t: JdbcTransaction): Page[ResultType[T]] = 
    summon[DBOperator[JdbcTransaction, Id]].pageMonad(t, query)(pageSize, pageNum, queryCount).get

def fetchCount(query: Select[_, _])(using logger: Logger, t: JdbcTransaction): Long = 
    summon[DBOperator[JdbcTransaction, Id]].fetchCountMonad(t, query).get