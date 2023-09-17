package easysql.database

import easysql.dsl.*
import easysql.jdbc.*
import easysql.query.nonselect.*
import easysql.query.select.*

import java.sql.Connection
import javax.sql.DataSource

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

        def runSql(x: JdbcConnection, sql: String, args: Array[Any]): Id[Int] =
            Id(x.exec(jdbcExec(_, sql, args)))

        def runSqlAndReturnKey(x: JdbcConnection, sql: String, args: Array[Any]): Id[List[Long]] =
            Id(x.exec(jdbcExecReturnKey(_, sql, args)))

        def querySql(x: JdbcConnection, sql: String, args: Array[Any]): Id[List[Array[Any]]] =
            Id(x.exec(jdbcQueryToArray(_, sql, args)))

        def querySqlCount(x: JdbcConnection, sql: String, args: Array[Any]): Id[Long] =
            Id(x.exec(jdbcQueryToArray(_, sql, args).head.head.toString.toLong))

        extension (x: JdbcConnection) {
            def run[T <: NonSelect](query: T)(using logger: Logger): Int =
                runMonad(x, query).get

            def runAndReturnKey(query: Insert[?, ?])(using logger: Logger): List[Long] =
                runAndReturnKeyMonad(x, query).get

            inline def query[T <: Tuple](query: Query[T, ?])(using logger: Logger): List[ResultType[T]] =
                queryMonad(x, query).get

            inline def query[T <: Tuple](query: MonadicQuery[T, ?])(using logger: Logger): List[ResultType[T]] =
                queryMonad(x, query).get

            inline def query[T <: Tuple](query: With[T])(using logger: Logger): List[ResultType[T]] =
                queryMonad(x, query).get

            inline def query[T](query: NativeSql)(using logger: Logger): List[NativeSqlResultType[T]] =
                queryMonad[T](x, query).get

            inline def querySkipNoneRows[T](query: Query[Tuple1[T], ?])(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def querySkipNoneRows[T](query: MonadicQuery[Tuple1[T], ?])(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def querySkipNoneRows[T](query: With[Tuple1[T]])(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def querySkipNoneRows[T](query: NativeSql)(using logger: Logger): List[T] =
                querySkipNoneRowsMonad(x, query).get

            inline def find[T <: Tuple](query: Select[T, ?])(using logger: Logger): Option[ResultType[T]] =
                findMonad(x, query).get
            
            inline def page[T <: Tuple](query: Select[T, ?])(pageSize: Int, pageNumber: Int, queryCount: Boolean)(using logger: Logger): Page[ResultType[T]] =
                pageMonad(x, query)(pageSize, pageNumber, queryCount).get

            def fetchCount(query: Select[?, ?])(using logger: Logger): Long =
                fetchCountMonad(x, query).get
        }
    }
}