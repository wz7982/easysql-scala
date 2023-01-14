package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.delete.Delete
import org.easysql.query.insert.Insert
import org.easysql.query.save.Save
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.query.update.Update
import org.easysql.jdbc.*
import org.easysql.dsl.*
import org.easysql.ast.SqlDataType
import org.easysql.database.DBOperator.dbMonadId

import java.sql.Connection
import javax.sql.DataSource

class JdbcConnection(override val db: DB, dataSource: DataSource) extends DBConnection[Id](db) {
    def getDB: DB = db
   
    private[database] override def runSql(sql: String): Id[Int] = 
        Id(exec(jdbcExec(_, sql)))

    private[database] override def runSqlAndReturnKey(sql: String): Id[List[Long]] = 
        Id(exec(jdbcExecReturnKey(_, sql)))

    private[database] override def querySql(sql: String): Id[List[Array[Any]]] = 
        Id(exec(jdbcQueryToArray(_, sql)))

    private[database] override def querySqlToMap(sql: String): Id[List[Map[String, Any]]] = 
        Id(exec(jdbcQuery(_, sql)))

    private[database] override def querySqlCount(sql: String): Id[Long] = 
        Id(exec(jdbcQueryToArray(_, sql).head.head.toString().toLong))

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

    def getConnection: Connection = dataSource.getConnection.nn

    private def exec[T](handler: Connection => T): T = {
        val conn = getConnection
        val result = handler(conn)
        conn.close()
        result
    }
}
