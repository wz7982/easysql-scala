package org.easysql.database

import org.easysql.query.ReviseQuery
import org.easysql.query.insert.Insert
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.dsl.*
import org.easysql.dsl.AllColumn.`*`
import org.easysql.ast.SqlDataType
import org.easysql.macros.bindSelect

import scala.concurrent.Future

abstract class DBOperator[F[_]](val db: DB)(using m: DBMonad[F]) {
    private[database] def runSql(sql: String): F[Int]

    private[database] def runSqlAndReturnKey(sql: String): F[List[Long]]

    private[database] def querySql(sql: String): F[List[Array[Any]]]

    private[database] def querySqlToMap(sql: String): F[List[Map[String, Any]]]

    private[database] def querySqlCount(sql: String): F[Long]

    inline def runMonad(query: ReviseQuery)(using logger: Logger): F[Int] = {
        val sql = query.sql(db)
        logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")

        runSql(sql)
    }

    inline def runAndReturnKeyMonad(query: Insert[_, _])(using logger: Logger): F[List[Long]] = {
        val sql = query.sql(db)
        logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")

        runSqlAndReturnKey(sql)
    }

    inline def queryMonad(sql: String)(using logger: Logger): F[List[Map[String, Any]]] = {
        logger.apply(s"execute sql: $sql")

        querySqlToMap(sql)
    }

    inline def queryMonad[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): F[List[ResultType[T]]] = {
        import scala.compiletime.erasedValue

        val sql = query.sql(db)
        logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")
        
        querySql(sql).map(datum => datum.map(i => bindSelect[ResultType[T]](0, i)))
    }

    inline def findMonad[T <: Tuple](query: SelectQuery[T, _])(using logger: Logger): F[Option[ResultType[T]]] = {
        import scala.compiletime.erasedValue

        val sql = inline query match {
            case s: Select[_, _] => s.limit(1).sql(db)
            case _ => query.sql(db)
        }
        logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")

        querySql(sql).map(datum => datum.headOption.map(i => bindSelect[ResultType[T]](0, i)))
    }
    
    inline def pageMonad[T <: Tuple](query: SelectQuery[T, _])(pageSize: Int, pageNum: Int, queryCount: Boolean)(using logger: Logger): F[Page[ResultType[T]]] = {
        val data = if (pageSize == 0) {
            m.pure(Nil)
        } else {
            val sql = inline query match {
                case s: Select[_, _] => s.pageSql(pageSize, pageNum)(db)
                case _ => select(*).from(query.as("_q1")).pageSql(pageSize, pageNum)(db)
            }
            logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")

            querySql(sql).map(datum => datum.map(i => bindSelect[ResultType[T]](0, i)))
        }

        val count = if (queryCount) {
            fetchCountMonad(query)(using logger)
        } else {
            m.pure(0L)
        }

        val totalPage = for {
            c <- count
        } yield {
            if (c == 0 || pageSize == 0) {
                0
            } else {
                if (c % pageSize == 0) {
                    c / pageSize 
                } else {
                    c / pageSize + 1
                }
            }
        }

        for {
            t <- totalPage
            c <- count
            d <- data
        } yield new Page[ResultType[T]](t, c, d)
    }

    inline def fetchCountMonad(query: SelectQuery[_, _])(using logger: Logger): F[Long] = {
        val sql = inline query match {
            case s: Select[_, _] => s.countSql(db)
            case _ => select(*).from(query.as("_q1")).countSql(db)
        }
        logger.apply(s"execute sql: ${sql.replaceAll("\n", " ")}")
        
        querySqlCount(sql)
    }
}

object DBOperator {
    import scala.concurrent.ExecutionContext

    given dbMonadId: DBMonad[Id] with {
        def pure[T](x: T): Id[T] = Id(x)

        extension [T] (x: Id[T]) {
            def map[R](f: T => R): Id[R] = x.map(f)

            def flatMap[R](f: T => Id[R]): Id[R] = x.flatMap(f)
        }
    }

    given dbMoandFuture(using ExecutionContext): DBMonad[Future] with {
        def pure[T](x: T): Future[T] = Future(x)

        extension [T] (x: Future[T]) {
            def map[R](f: T => R): Future[R] = x.map(f)

            def flatMap[R](f: T => Future[R]): Future[R] = x.flatMap(f)
        }
    }
}

trait DBFunctor[F[_]] {
    extension [T] (x: F[T]) def map[R](f: T => R): F[R]
}

trait DBMonad[F[_]] extends DBFunctor[F] {
    def pure[T](x: T): F[T]

    extension [T] (x: F[T]) def flatMap[R](f: T => F[R]): F[R]
}

case class Id[T](x: T) {
    def get: T = x

    def map[R](f: T => R): Id[R] = Id(f(x))

    def flatMap[R](f: T => Id[R]): Id[R] = f(x)
}

type Logger = String => Unit