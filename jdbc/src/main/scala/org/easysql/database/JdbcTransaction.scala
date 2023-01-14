package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.insert.Insert
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.jdbc.*
import org.easysql.dsl.*
import org.easysql.ast.SqlDataType
import org.easysql.database.DBOperator.dbMonadId

import java.sql.Connection

class JdbcTransaction(override val db: DB, conn: Connection) extends DBTransaction[Id](db) {
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

    inline def run(query: ReviseQuery)(using logger: Logger): Int = 
        runMonad(query).get

    inline def runAndReturnKey(query: Insert[_, _])(using logger: Logger): List[Long] = 
        runAndReturnKeyMonad(query).get

    inline def query(sql: String)(using logger: Logger): List[Map[String, Any]] = 
        queryMonad(sql).get

    inline def query[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): List[ResultType[T]] = 
        queryMonad(query).get

    inline def find[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): Option[ResultType[T]] =
        findMonad(query).get

    inline def page[T <: Tuple](query: SelectQuery[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger): Page[ResultType[T]] =
        pageMonad(query)(pageSize, pageNum, queryCount).get

    inline def fetchCount(query: SelectQuery[_, _])(using logger: Logger): Long = 
        fetchCountMonad(query).get
}

inline def run(query: ReviseQuery)(using logger: Logger, t: JdbcTransaction): Int = t.run(query)

inline def runAndReturnKey(query: Insert[_, _])(using logger: Logger, t: JdbcTransaction): List[Long] = t.runAndReturnKey(query)

inline def query(sql: String)(using logger: Logger, t: JdbcTransaction): List[Map[String, Any]] = t.query(sql)

inline def query[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger, t: JdbcTransaction): List[ResultType[T]] = t.query(query)

inline def find[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger, t: JdbcTransaction): Option[ResultType[T]] = t.find(query)

inline def page[T <: Tuple](query: SelectQuery[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger, t: JdbcTransaction): Page[ResultType[T]] = t.page(query)(pageSize, pageNum, queryCount)

inline def fetchCount(query: SelectQuery[_, _])(using logger: Logger, t: JdbcTransaction): Long = t.fetchCount(query)