package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.insert.Insert
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.dsl.*
import org.easysql.dsl.AllColumn.`*`
import org.easysql.ast.SqlDataType
import org.easysql.macros.bindSelect

import reflect.Selectable.reflectiveSelectable

abstract class DBOperater(val db: DB) {
    private[database] def runSql(sql: String): Int

    private[database] def runSqlAndReturnKey(sql: String): List[Long]

    private[database] def querySql(sql: String): List[Array[Any]]

    private[database] def querySqlToMap(sql: String): List[Map[String, Any]]

    private[database] def querySqlCount(sql: String): Long

    inline def run(query: ReviseQuery)(using logger: Logger): Int = {
        val sql = query.sql(db)
        logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")

        runSql(sql)
    }

    inline def runAndReturnKey(query: Insert[_, _])(using logger: Logger): List[Long] = {
        val sql = query.sql(db)
        logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")

        runSqlAndReturnKey(sql)
    }

    inline def query(sql: String)(using logger: Logger): List[Map[String, Any]] = {
        logger.info(s"execute sql: $sql")

        querySqlToMap(sql)
    }

    inline def query[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): List[ResultType[T]] = {
        import scala.compiletime.erasedValue

        val sql = query.sql(db)
        logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")

        inline erasedValue[T] match {
            case _: Tuple1[t] => 
                querySql(sql).map(i => bindSelect[Option[t]].apply(i)).filter(_.nonEmpty).map(_.get.asInstanceOf[ResultType[T]])
            case _ => 
                querySql(sql).map(i => bindSelect[ResultType[T]].apply(i))
        }
    }

    inline def find[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): Option[ResultType[T]] = {
        import scala.compiletime.erasedValue

        val sql = inline query match {
            case s: Select[?, ?] => s.limit(1).sql(db)
            case _ => query.sql(db)
        }
        logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")

        inline erasedValue[T] match {
            case _: Tuple1[t] => 
                querySql(sql).map(i => bindSelect[Option[t]].apply(i)).filter(_.nonEmpty).map(_.get.asInstanceOf[ResultType[T]]).headOption
            case _ => 
                querySql(sql).headOption.map(i => bindSelect[ResultType[T]].apply(i))
        }
    }
    
    inline def page[T <: Tuple](query: SelectQuery[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger): Page[ResultType[T]] = {
        val data = if (pageSize == 0) {
            List[ResultType[T]]()
        } else {
            val sql = inline query match {
                case s: Select[?, ?] => s.pageSql(pageSize, pageNum)(db)
                case _ => select(*).from(query.as("_q1")).pageSql(pageSize, pageNum)(db)
            }
            logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")

            querySql(sql).map(i => bindSelect[ResultType[T]].apply(i))
        }

        val count = if (queryCount) {
            fetchCount(query)(using logger)
        } else {
            0l
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

        new Page[ResultType[T]](totalPage, count, data)
    }

    inline def fetchCount(query: SelectQuery[_, _])(using logger: Logger): Long = {
        val sql = inline query match {
            case s: Select[?, ?] => s.countSql(db)
            case _ => select(*).from(query.as("_q1")).countSql(db)
        }
        logger.info(s"execute sql: ${sql.replaceAll("\n", " ")}")
        
        querySqlCount(sql)
    }
}

type Logger = { def info(text: String): Unit }