package easysql.database

import easysql.dsl.*
import easysql.jdbc.*
import easysql.query.select.*
import easysql.query.nonselect.*
import easysql.macros.*
import easysql.query.ToSql

import javax.sql.DataSource
import java.sql.Connection

class JdbcConnection(val db: DB, val dataSource: DataSource) {
    def getConnection: Connection = 
        dataSource.getConnection.nn

    def exec[T](handler: Connection => T): T = {
        val conn = getConnection
        val result = handler(conn)
        conn.close()
        result
    }

    def transactionIsolation[T](isolation: Int)(query: JdbcTransaction ?=> T): T = {
        val conn = getConnection
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
       
        try {
            given t: JdbcTransaction = new JdbcTransaction(db, conn)
            val result = query
            conn.commit()
            result
        } catch {
            case e: Exception => {
                conn.rollback()
                throw e
            }
        } finally {
            conn.setAutoCommit(true)
            conn.close()
        }
    }
   
    def transaction[T](query: JdbcTransaction ?=> T): T = {
        val conn = getConnection
        conn.setAutoCommit(false)
       
        try {
            given t: JdbcTransaction = new JdbcTransaction(db, conn)
            val result = query
            conn.commit()
            result
        } catch {
            case e: Exception => {
                conn.rollback()
                throw e
            }
        } finally {
            conn.setAutoCommit(true)
            conn.close()
        }
    }
}

object JdbcConnection {
    import easysql.database.DBOperator.dbMonadId

    given jdbcConnection: DBOperator[JdbcConnection, Id] with {
        def db(x: JdbcConnection): DB = 
            x.db

        def runSql(x: JdbcConnection, sql: String): Id[Int] =
            Id(x.exec(jdbcExec(_, sql)))

        def runSqlAndReturnKey(x: JdbcConnection, sql: String): Id[List[Long]] =
            Id(x.exec(jdbcExecReturnKey(_, sql)))

        def querySql(x: JdbcConnection, sql: String): Id[List[Array[Any]]] =
            Id(x.exec(jdbcQueryToArray(_, sql)))

        def querySqlToMap(x: JdbcConnection, sql: String): Id[List[Map[String, Any]]] =
            Id(x.exec(jdbcQuery(_, sql)))

        def querySqlCount(x: JdbcConnection, sql: String): Id[Long] =
            Id(x.exec(jdbcQuery(_, sql).head.head._2.toString().toLong))

        extension (x: JdbcConnection) {
            def run[T <: NonSelect : ToSql](query: T)(using logger: Logger): Int =
                runMonad(x, query).get

            def runAndReturnKey(query: Insert[_, _])(using logger: Logger): List[Long] =
                runAndReturnKeyMonad(x, query).get

            def query(sql: String)(using logger: Logger): List[Map[String, Any]] =
                queryMonad(x, sql).get

            inline def query[T <: Tuple](query: Query[T, _])(using logger: Logger): List[ResultType[T]] =
                queryMonad(x, query).get

            inline def query[T <: Tuple](query: With[T])(using logger: Logger): List[ResultType[T]] =
                queryMonad(x, query).get

            inline def querySkipNoneRows[T](query: Query[Tuple1[T], _])(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def querySkipNoneRows[T](query: With[Tuple1[T]])(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def find[T <: Tuple](query: Select[T, _])(using logger: Logger): Option[ResultType[T]] =
                findMonad(x, query).get
            
            inline def page[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNumber: Int, queryCount: Boolean)(using logger: Logger): Page[ResultType[T]] =
                pageMonad(x, query)(pageSize, pageNumber, queryCount).get

            def fetchCount(query: Select[_, _])(using logger: Logger): Long =
                fetchCountMonad(x, query).get
        }
    }
}