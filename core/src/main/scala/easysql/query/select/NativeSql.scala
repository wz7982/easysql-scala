package easysql.query.select

case class NativeSql(sql: String, args: Array[Any]) {
    override def toString(): String = sql
}