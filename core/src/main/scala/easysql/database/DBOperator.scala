package easysql.database

import easysql.query.nonselect.*
import easysql.query.select.*
import easysql.query.ToSql
import easysql.ast.SqlDataType
import easysql.dsl.*
import easysql.dsl.AllColumn.`*`
import easysql.macros.*

import scala.concurrent.Future

trait DBOperator[F[_]](val db: DB)(using m: DBMonad[F]) {
    private[database] def runSql(sql: String): F[Int]

    private[database] def runSqlAndReturnKey(sql: String): F[List[Long]]

    private[database] def querySql(sql: String): F[List[Array[Any]]]

    private[database] def querySqlToMap(sql: String): F[List[Map[String, Any]]]

    private[database] def querySqlCount(sql: String): F[Long]

    def runMonad[T : NonSelect : ToSql](query: T)(using logger: Logger): F[Int] = {
        val sql = query.sql(db)
        logger.apply(s"execute sql: \n$sql")

        runSql(sql)
    }

    def runAndReturnKeyMonad(query: Insert[_, _])(using logger: Logger): F[List[Long]] = {
        val sql = query.sql(db)
        logger.apply(s"execute sql: \n$sql")

        runSqlAndReturnKey(sql)
    }

    def queryMonad(sql: String)(using logger: Logger): F[List[Map[String, Any]]] = {
        logger.apply(s"execute sql: \n$sql")

        querySqlToMap(sql)
    }

    inline def queryMonad[T <: Tuple](query: Select[T, _])(using logger: Logger): F[List[ResultType[T]]] = {
        val sql = query.sql(db)
        logger.apply(s"execute sql: \n$sql")

        for {
            data <- querySql(sql)
        } yield data.map(bind[ResultType[T]](0, _))
    }

    inline def querySkipNoneRowsMonad[T <: Tuple](query: Select[Tuple1[T], _])(using logger: Logger): F[List[T]] = {
        for {
            data <- queryMonad(query)
        } yield data.filter(_.nonEmpty).map(_.get)
    }  

    inline def findMonad[T <: Tuple](query: Select[T, _])(using logger: Logger): F[Option[ResultType[T]]] = {
        for {
            data <- queryMonad(query.limit(1))
        } yield data.headOption
    }
    
    inline def pageMonad[T <: Tuple](query: Select[T, _])(pageSize: Int, pageNumber: Int, queryCount: Boolean)(using logger: Logger): F[Page[ResultType[T]]] = {
        val data = if (pageSize == 0) {
            m.pure(Nil)
        } else {
            val offset = if pageNumber <= 1 then 0 else pageSize * (pageNumber - 1)
            val pageQuery = query.limit(pageSize).offset(offset)
            queryMonad(pageQuery)
        }

        val count = if queryCount then fetchCountMonad(query) else m.pure(0L)

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
        } yield Page(t, c, d)
    }

    def fetchCountMonad(query: Select[_, _])(using logger: Logger): F[Long] = {
        val sql = query.countSql(db)
        logger.apply(s"execute sql: \n$sql")

        querySqlCount(sql)
    }
}

object DBOperator {
    import scala.concurrent.ExecutionContext

    given dbMonadId: DBMonad[Id] with {
        def pure[T](x: T): Id[T] = 
            Id(x)

        extension [T] (x: Id[T]) {
            def map[R](f: T => R): Id[R] = 
                x.map(f)

            def flatMap[R](f: T => Id[R]): Id[R] = 
                x.flatMap(f)
        }
    }

    given dbMoandFuture(using ExecutionContext): DBMonad[Future] with {
        def pure[T](x: T): Future[T] = 
            Future(x)

        extension [T] (x: Future[T]) {
            def map[R](f: T => R): Future[R] = 
                x.map(f)

            def flatMap[R](f: T => Future[R]): Future[R] = 
                x.flatMap(f)
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
    def get: T = 
        x

    def map[R](f: T => R): Id[R] = 
        Id(f(x))

    def flatMap[R](f: T => Id[R]): Id[R] = 
        f(x)
}

type Logger = String => Unit