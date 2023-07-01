package easysql.query

import easysql.database.DB

trait ToSql[T] {
    extension (x: T) {
        def sql(db: DB): String

        def toSql(using db: DB): String = 
            sql(db)

        def preparedSql(db: DB): (String, Array[Any])

        def toPreparedSql(using db: DB): (String, Array[Any]) =
            preparedSql(db)
    }
}

trait ToCountSql[T] {
    extension (x: T) {
        def countSql(db: DB): String

        def toCountSql(using db: DB): String = 
            countSql(db)

        def preparedCountSql(db: DB): (String, Array[Any])

        def toPreparedCountSql(using db: DB): (String, Array[Any]) =
            preparedCountSql(db)
    }
}

trait ToPageSql[T] {
    extension (x: T) {
        def pageSql(pageSize: Int, pageNumber: Int)(db: DB): String

        def toPageSql(pageSize: Int, pageNumber: Int)(using db: DB): String =
            pageSql(pageSize, pageNumber)(db)

        def preparedPageSql(pageSize: Int, pageNumber: Int)(db: DB): (String, Array[Any])

        def toPreparedPageSql(pageSize: Int, pageNumber: Int)(using db: DB): (String, Array[Any]) =
            preparedPageSql(pageSize, pageNumber)(db)
    }
}