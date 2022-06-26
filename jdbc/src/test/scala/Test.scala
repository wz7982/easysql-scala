import org.easysql.database.TableEntity
import org.easysql.dsl.{PrimaryKeyColumnExpr, TableColumnExpr, TableSchema}
import org.easysql.macros.bindEntityMacro

object Test extends App {
    println(xx[Post])
}

inline def xx[T <: TableEntity[_]]: T = {
    val result = Map("id" -> 15, "user_id" -> 59, "post_name" -> "xxx")
    bindEntityMacro[T](result)
}

case class Post(id: Int, userId: Int, name: String) extends TableEntity[Int]

object Post extends TableSchema {
    override val tableName: String = "post"
    val id = intColumn("id").primaryKey
    val userId = intColumn("user_id")
    val name = varcharColumn("post_name")
}