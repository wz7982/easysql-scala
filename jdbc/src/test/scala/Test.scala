import org.easysql.dsl.*
import org.easysql.ast.SqlDataType
import org.easysql.dsl.TableSchema
import org.easysql.database.*

import java.util.Date
import scala.util.Random
import scala.compiletime.ops.int

given Logger = java.util.logging.Logger.getLogger("")

object Test extends App {
    // val bind = bindSelect[Tuple3[TestTable, TestTable, TestTable]]
    // val data = bind(Array[Any](1, null, Date(), null, null, null, 1, null, Date()))
    // println(data)

    // val bind = bindSelect[(Option[TestTable], Option[TestTable], Option[Int])]
    // val data = bind(Array[Any](1, "x", Date(), null, null, null, 1, null, Date()))
    // println(data)

    

    val db: JdbcConnection = ???

    val q = select (tt) from tt

    
    val data = db.query(q)
    val data1 = db.find(q)
}

@Table("test_table")
case class TestTable(
    @IncrKey id: Int,
    @Column name: Option[String],
    @Column date: Option[Date]
)

val tt = asTable[TestTable]
