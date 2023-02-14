package easysql.database

import easysql.database.DBOperator.dbMonadId
import easysql.dsl.*
import easysql.jdbc.*
import easysql.query.select.*
import easysql.query.nonselect.*
import easysql.macros.*
import easysql.query.ToSql

import javax.sql.DataSource
import java.sql.Connection

class JdbcConnection(override val db: DB, dataSource: DataSource) extends DBOperator[Id](db) {
    private[database] override def runSql(sql: String): Id[Int] = 
        Id(exec(jdbcExec(_, sql)))

    private[database] override def runSqlAndReturnKey(sql: String): Id[List[Long]] = 
        Id(exec(jdbcExecReturnKey(_, sql)))

    private[database] override def querySql(sql: String): Id[List[Array[Any]]] = 
        Id(exec(jdbcQueryToArray(_, sql)))

    private[database] override def querySqlToMap(sql: String): Id[List[Map[String, Any]]] = 
        Id(exec(jdbcQuery(_, sql)))

    private[database] override def querySqlCount(sql: String): Id[Long] = 
        Id(exec(jdbcQuery(_, sql).head.head._2.toString().toLong))

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

    def getConnection: Connection = 
        dataSource.getConnection.nn

    private def exec[T](handler: Connection => T): T = {
        val conn = getConnection
        val result = handler(conn)
        conn.close()
        result
    }
}