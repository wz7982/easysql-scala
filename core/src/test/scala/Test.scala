import easysql.dsl.*
import easysql.query.select.*
import easysql.database.DB
import easysql.macros.*
import easysql.ast.SqlDataType

import scala.compiletime.ops.any.*
import java.util.UUID

object Test extends App {
    given DB = DB.MYSQL

    // val s = select(tt.*) from tt where tt.id === 1 && tt.testNullable === "x"
    // println(s.toSql)


//    val testTable = TestTable("1", None)
//    val i = insert(testTable)
//    println(i.toSql)
//
//    val u = update(testTable, false)
//    println(u.toSql)
//
//    val d = delete[TestTable]("1")
//    println(d.toSql)
//
//    val sv = save[TestTable](testTable)
//    println(sv.sql(DB.MYSQL))
//    println(sv.sql(DB.PGSQL))
//    println(sv.sql(DB.SQLITE))
//
//    val f = findQuery[TestTable]("1")
//    println(f.toSql)

    // val s = from (tt) where tt.id === "x" || tt.name === "y"
    // println(s.toSql)

    // val s1 = select (post, post.id as "p1", post.name as "p2") from user join post on user.id === post.userId where user.id === 1
    // println(s1.toSql)

    // val sub1 = select (user.id as "c1", user.name as "c2") from user
    // val sub2 = select (user.id, user.name) from user
    // val sub = sub1 union sub2 union sub2 as "sub"


    // val s = select (sub.c1, sub.c2) from sub
    // println(s.toSql)

//    val s = (
//        select (user)
//        from user
//        where true
//            && user.createTime.between("2020-01-01", "2022-01-01")
//            && user.id + 1.0 > 2.3
//            || false
//    )
//
//    println(s.toSql)

    val q1 = select (user.id, count() + sum(user.id) as "c1") from user where (user.id > 0 || user.name === "") && true as "q1"
    
    val q2 = select (q1.id, q1.c1) from q1 as "q2"
    
    val q3 = select (q2.id) from q2 as "q3"
    
    val q4 = select (q3.id as "c1") from q3 as "q4"

    val q5 = select (q4.c1) from q4
    
    println(q5.toSql)

    val t1 = caseWhen(user.id > 0 thenIs 1, user.id < 0 thenIs 2.1)

    val t2 = user.id > 0 thenIs ""

    
    val expr: Expr[Int] = user.id
    val expr1 = user.id > BigDecimal(1)
}

@Table
case class TestTable(
    @PrimaryKeyGenerator("id", () => UUID.randomUUID().toString) id: String,
    name: Option[String]
)

val tt = asTable[TestTable]
