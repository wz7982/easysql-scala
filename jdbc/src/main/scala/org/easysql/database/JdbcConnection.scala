package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.delete.Delete
import org.easysql.query.insert.Insert
import org.easysql.query.save.Save
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.query.update.Update
import org.easysql.jdbc.*
import org.easysql.macros.bindEntityMacro
import org.easysql.dsl.MapUnionNull

import java.sql.Connection
import javax.sql.DataSource

class JdbcConnection(db: DB, dataSource: DataSource) extends DBConnection(db) {
    def getDB: DB = db
    
    override def run(query: ReviseQuery): Int = exec(conn => jdbcExec(conn, query.sql(db)))

    override def runAndReturnKey(query: Insert[_, _]): List[Long] = exec(conn => jdbcExecReturnKey(conn, query.sql(db)))

    override def queryMap(query: SelectQuery[_]): List[Map[String, Any]] = exec(conn => jdbcQuery(conn, query.sql(db)).map(_.toMap))

    override def queryTuple[T <: Tuple](query: SelectQuery[T]): List[MapUnionNull[T]] = exec(conn => jdbcQuery(conn, query.sql(db)).map(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]]))

    inline def queryEntity[T <: TableEntity[_]](query: SelectQuery[_]): List[T] = exec(conn => jdbcQuery(conn, query.sql(db)).map(it => bindEntityMacro[T](it.toMap)))

    override def findMap(query: Select[_, _]): Option[Map[String, Any]] = exec(conn => jdbcQuery(conn, query.sql(db)).headOption.map(_.toMap))

    override def findTuple[T <: Tuple](query: Select[T, _]): Option[MapUnionNull[T]] = exec(conn => jdbcQuery(conn, query.sql(db)).headOption.map(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]]))

    inline def findEntity[T <: TableEntity[_]](query: Select[_, _]): Option[T] = exec(conn => jdbcQuery(conn, query.sql(db)).headOption.map(it => bindEntityMacro[T](it.toMap)))

    override def pageMap(query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[Map[String, Any]] =
        page(query)(pageSize, pageNum, needCount)(it => it.toMap)

    override def pageTuple[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[MapUnionNull[T]] =
        page(query)(pageSize, pageNum, needCount)(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]])

    inline def pageEntity[T <: TableEntity[_]](query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[T] =
        page(query)(pageSize, pageNum, needCount)(it => bindEntityMacro[T](it.toMap))

    override def fetchCount(query: Select[_, _]): Int = exec(conn => jdbcQueryCount(conn, query.fetchCountSql(db)))

    def transaction(isolation: Int)(query: JdbcTransaction => Unit): Unit = {
        val conn = getConnection
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
        
        try {
            query(new JdbcTransaction(db, conn))
            conn.commit()
        } catch {
            case e: Exception => {
                e.printStackTrace()
                conn.rollback()
            }
        } finally {
            conn.setAutoCommit(true)
            conn.close()
        }
    }
    
    def transaction(query: JdbcTransaction => Unit): Unit = {
        val conn = getConnection
        conn.setAutoCommit(false)
        
        try {
            query(new JdbcTransaction(db, conn))
            conn.commit()
        } catch {
            case e: Exception => {
                e.printStackTrace()
                conn.rollback()
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

    private def page[T](query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean)(bind: List[(String, Any)] => T): Page[T] = {
        val data = if (pageSize == 0) {
            List[T]()
        } else {
            exec(conn => jdbcQuery(conn, query.pageSql(pageNum, pageNum)(db)).map(bind))
        }

        val count = if (needCount) {
            fetchCount(query)
        } else {
            0
        }

        val totalPage = if (count == 0 || pageSize == 0) {
            0
        } else {
            if (count % pageSize == 0) {
                count / pageSize
            } else {
                count / pageSize + 1
            }
        }

        new Page[T](totalPage, count, data)
    }
}
