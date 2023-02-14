package easysql.database

import easysql.query.nonselect.*
import easysql.query.select.*
import easysql.jdbc.*
import easysql.dsl.*
import easysql.ast.SqlDataType
import easysql.database.DBOperator.dbMonadId
import easysql.query.ToSql

import java.sql.Connection

class JdbcTransaction(override val db: DB, conn: Connection) extends DBOperator[Id](db) {
    private[database] override def runSql(sql: String): Id[Int] =
        Id(jdbcExec(conn, sql))

    private[database] override def runSqlAndReturnKey(sql: String): Id[List[Long]] =
        Id(jdbcExecReturnKey(conn, sql))

    private[database] override def querySql(sql: String): Id[List[Array[Any]]] =
        Id(jdbcQueryToArray(conn, sql))

    private[database] override def querySqlToMap(sql: String): Id[List[Map[String, Any]]] =
        Id(jdbcQuery(conn, sql))

    private[database] override def querySqlCount(sql: String): Id[Long] =
        Id(jdbcQueryToArray(conn, sql).head.head.toString().toLong)

    def run[T : NonSelect : ToSql](query: T)(using logger: Logger): Int =
        runMonad(query).get

    def runAndReturnKey(query: Insert[_, _])(using logger: Logger): List[Long] =
        runAndReturnKeyMonad(query).get

    def query(sql: String)(using logger: Logger): List[Map[String, Any]] =
        queryMonad(sql).get

    inline def query[T <: Tuple](query: Select[T, _])(using logger: Logger): List[ResultType[T]] =
        queryMonad(query).get

    inline def querySkipNoneRows[T <: Tuple](query: Select[Tuple1[T], _])(using logger: Logger): List[T] =
        querySkipNoneRowsMonad(query).get

    inline def find[T <: Tuple](query: Select[T, _])(using logger: Logger): Option[ResultType[T]] =
        findMonad(query).get
    
    inline def page[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNumber: Int, queryCount: Boolean)(using logger: Logger): Page[ResultType[T]] =
        pageMonad(query)(pageSize, pageNumber, queryCount).get

    def fetchCount(query: Select[_, _])(using logger: Logger): Long =
        fetchCountMonad(query).get
}

def run[T : NonSelect : ToSql](query: T)(using logger: Logger, t: JdbcTransaction): Int = 
    t.run(query)

def runAndReturnKey(query: Insert[_, _])(using logger: Logger, t: JdbcTransaction): List[Long] = 
    t.runAndReturnKey(query)

def query(sql: String)(using logger: Logger, t: JdbcTransaction): List[Map[String, Any]] = 
    t.query(sql)

inline def query[T <: Tuple](query: Select[T, _])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = 
    t.query(query)

inline def querySkipNoneRows[T <: Tuple](query: Select[Tuple1[T], _])(using logger: Logger, t: JdbcTransaction): List[T] = 
    t.querySkipNoneRows(query)

inline def find[T <: Tuple](query: Select[T, _])(using logger: Logger, t: JdbcTransaction): Option[ResultType[T]] = 
    t.find(query)

inline def page[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger, t: JdbcTransaction): Page[ResultType[T]] = 
    t.page(query)(pageSize, pageNum, queryCount)

def fetchCount(query: Select[_, _])(using logger: Logger, t: JdbcTransaction): Long = 
    t.fetchCount(query)