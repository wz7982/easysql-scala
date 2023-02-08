package easysql.database

case class Page[T](totalPage: Long, totalCount: Long, data: List[T]) {
    def map[R](f: T => R): Page[R] =
        copy(data = data.map(f))

    def filter(f: T => Boolean): Page[T] =
        copy(data = data.filter(f))
}