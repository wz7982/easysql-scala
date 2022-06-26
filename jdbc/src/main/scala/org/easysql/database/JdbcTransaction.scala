package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.insert.Insert
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.jdbc.*
import org.easysql.macros.bindEntityMacro
import org.easysql.dsl.MapUnionNull

import java.sql.Connection

class JdbcTransaction(db: DB, conn: Connection) extends DBTransaction(db) {
    override def run(query: ReviseQuery): Int = jdbcExec(conn, query.sql(db))

    override def runAndReturnKey(query: Insert[_, _]): List[Long] = jdbcExecReturnKey(conn, query.sql(db))

    override def queryMap(query: SelectQuery[_]): List[Map[String, Any]] = jdbcQuery(conn, query.sql(db)).map(_.toMap)

    override def queryTuple[T <: Tuple](query: SelectQuery[T]): List[MapUnionNull[T]] = jdbcQuery(conn, query.sql(db)).map(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]])

    inline def queryEntity[T <: TableEntity[_]](query: SelectQuery[_]): List[T] = jdbcQuery(conn, query.sql(db)).map(it => bindEntityMacro[T](it.toMap))

    override def findMap(query: Select[_, _]): Option[Map[String, Any]] = jdbcQuery(conn, query.sql(db)).headOption.map(_.toMap)

    override def findTuple[T <: Tuple](query: Select[T, _]): Option[MapUnionNull[T]] = jdbcQuery(conn, query.sql(db)).headOption.map(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]])

    inline def findEntity[T <: TableEntity[_]](query: Select[_, _]): Option[T] = jdbcQuery(conn, query.sql(db)).headOption.map(it => bindEntityMacro[T](it.toMap))

    override def pageMap(query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[Map[String, Any]] =
        page(query)(pageSize, pageNum, needCount)(it => it.toMap)

    override def pageTuple[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[MapUnionNull[T]] =
        page(query)(pageSize, pageNum, needCount)(it => Tuple.fromArray(it.map(_._2).toArray).asInstanceOf[MapUnionNull[T]])

    inline def pageEntity[T <: TableEntity[_]](query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean = true): Page[T] =
        page(query)(pageSize, pageNum, needCount)(it => bindEntityMacro[T](it.toMap))

    override def fetchCount(query: Select[_, _]): Int = jdbcQueryCount(conn, query.fetchCountSql(db))

    private def page[T](query: Select[_, _])(pageSize: Int, pageNum: Int, needCount: Boolean)(bind: List[(String, Any)] => T): Page[T] = {
        val data = if (pageSize == 0) {
            List[T]()
        } else {
            jdbcQuery(conn, query.pageSql(pageNum, pageNum)(db)).map(bind)
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
