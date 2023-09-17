package easysql.database

import easysql.dsl.*
import easysql.macros.*
import easysql.query.nonselect.*
import easysql.query.select.*

import scala.concurrent.Future

trait DBOperator[D, F[_] : DBMonad] {
    def db(x: D): DB

    def runSql(x: D, sql: String, args: Array[Any]): F[Int]

    def runSqlAndReturnKey(x: D, sql: String, args: Array[Any]): F[List[Long]]

    def querySql(x: D, sql: String, args: Array[Any]): F[List[Array[Any]]]

    def querySqlCount(x: D, sql: String, args: Array[Any]): F[Long]

    def runMonad[T <: NonSelect](x: D, query: T)(using logger: Logger): F[Int] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        runSql(x, info._1, info._2)
    }

    def runAndReturnKeyMonad(x: D, query: Insert[?, ?])(using logger: Logger): F[List[Long]] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        runSqlAndReturnKey(x, info._1, info._2)
    }

    inline def queryMonad[T <: Tuple](x: D, query: Query[T, ?])(using logger: Logger): F[List[ResultType[T]]] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        for {
            data <- querySql(x, info._1, info._2)
        } yield data.map(bind[ResultType[T]](0, _))
    }

    inline def queryMonad[T <: Tuple](x: D, query: MonadicQuery[T, ?])(using logger: Logger): F[List[ResultType[T]]] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        for {
            data <- querySql(x, info._1, info._2)
        } yield data.map(bind[ResultType[T]](0, _))
    }

    inline def queryMonad[T <: Tuple](x: D, query: With[T])(using logger: Logger): F[List[ResultType[T]]] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        for {
            data <- querySql(x, info._1, info._2)
        } yield data.map(bind[ResultType[T]](0, _))
    }

    inline def queryMonad[T <: Tuple](x: D, query: NativeSql)(using logger: Logger): F[List[ResultType[T]]] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        for {
            data <- querySql(x, info._1, info._2)
        } yield data.map(bind[ResultType[T]](0, _))
    }

    inline def querySkipNoneRowsMonad[T](x: D, query: Query[Tuple1[T], ?])(using logger: Logger): F[List[T]] = {
        for {
            data <- queryMonad(x, query)
        } yield data.filter(_.nonEmpty).map(_.get)
    }

    inline def querySkipNoneRowsMonad[T](x: D, query: MonadicQuery[Tuple1[T], ?])(using logger: Logger): F[List[T]] = {
        for {
            data <- queryMonad(x, query)
        } yield data.filter(_.nonEmpty).map(_.get)
    }

    inline def querySkipNoneRowsMonad[T](x: D, query: With[Tuple1[T]])(using logger: Logger): F[List[T]] = {
        for {
            data <- queryMonad(x, query)
        } yield data.filter(_.nonEmpty).map(_.get)
    }

    inline def querySkipNoneRowsMonad[T](x: D, query: NativeSql)(using logger: Logger): F[List[T]] = {
        for {
            data <- queryMonad[Tuple1[T]](x, query)
        } yield data.filter(_.nonEmpty).map(_.get)
    }

    inline def findMonad[T <: Tuple](x: D, query: Select[T, ?])(using logger: Logger): F[Option[ResultType[T]]] = {
        for {
            data <- queryMonad(x, query.limit(1))
        } yield data.headOption
    }
    
    inline def pageMonad[T <: Tuple](x: D, query: Select[T, ?])(pageSize: Int, pageNumber: Int, queryCount: Boolean)(using logger: Logger): F[Page[ResultType[T]]] = {
        val data = if (pageSize == 0) {
            summon[DBMonad[F]].pure(Nil)
        } else {
            val offset = if pageNumber <= 1 then 0 else pageSize * (pageNumber - 1)
            val pageQuery = query.limit(pageSize).offset(offset)
            queryMonad(x, pageQuery)
        }

        val count = if queryCount then fetchCountMonad(x, query) else summon[DBMonad[F]].pure(0L)

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
        } yield Page(t, c, pageNumber, pageSize, d)
    }

    def fetchCountMonad(x: D, query: Select[?, ?])(using logger: Logger): F[Long] = {
        val info = query.preparedSql(db(x))
        logger.apply(s"execute sql: \n${info._1}")

        querySqlCount(x, info._1, info._2)
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