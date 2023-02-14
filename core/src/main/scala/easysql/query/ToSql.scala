package easysql.query

import easysql.database.DB

trait ToSql[T] {
    extension (x: T) {
        def sql(db: DB): String

        def toSql(using db: DB): String = 
            sql(db)
    }
}

trait ToCountSql[T] {
    extension (x: T) {
        def countSql(db: DB): String

        def countToSql(using db: DB): String = 
            countSql(db)
    }
}

trait ToPageSql[T] {
    extension (x: T) {
        def pageSql(pageSize: Int, pageNumber: Int)(db: DB): String

        def toPageSql(pageSize: Int, pageNumber: Int)(using db: DB): String =
            pageSql(pageSize, pageNumber)(db)
    }
}