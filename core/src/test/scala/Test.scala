import org.easysql.dsl.*
import org.easysql.query.delete.Delete
import org.easysql.query.insert.Insert
import org.easysql.query.select.{Query, Select, UnionSelect}
import org.easysql.database.{DB, TableEntity}
import org.easysql.macros.*
import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.table.SqlJoinType

import scala.compiletime.ops.any.*
import scala.compiletime.ops.int.+
import java.util.Date
import scala.annotation.{experimental, tailrec}

object Test extends App {
//    val select = Select() select(User.name, User.id) from User
//    println(select.sql(DB.MYSQL))
//    val select1 = Select() select(Post.name, Post.id) from Post
//
//    val union = select union select1
//    println(union.sql(DB.PGSQL))
//
//    val s: Select[(Int, String | Null)] = Select() select (User.id) select (User.name) from User
//    println(s.sql(DB.MYSQL))\


//    println(User.$columns)
//    println(Post.$columns)

//    val t1 = User as "t1"
//    val t2 = User as "t2"
//    val s = select (t1.name) from t1 leftJoin t2 on t1.id === t2.id where t1.id === 1
//    println(s.sql(DB.MYSQL))

//    val sub = (select (User.id as "c1") from User) as "sub"
//    val t1 = User as "t1"
//    val s1 = select (sub.c1 as "x1") from sub leftJoin t1 on sub.c1 === t1.id where sub.c1 === 1
//    println(s1.sql(DB.MYSQL))

//    val i = insertInto(User)(User.*).values((1, "", ""))
//    println(i.sql(DB.MYSQL))

    val a = (User.*, User.id)
    val b: RecursiveInverseMap[a.type] = (1, "", "", 1)

    val a1 = (User.id, User.*)
    val b1: RecursiveInverseMap[a1.type] = (1, 1, "", "")

    val a2 = (User.*, User.*)
    val b2: RecursiveInverseMap[a2.type] = (1, "", "", 1, "", "")

    val a3 = (User.*, User.*, User.id)
    val b3: RecursiveInverseMap[a3.type] = (1, "", "", 1, "", "", 1)

    val a4 = (User.id, User.*, User.*, User.id)
    val b4: RecursiveInverseMap[a4.type] = (1, 1, "", "", 1, "", "", 1)

    val a5 = (User.id, (User.*, User.id))
    val b5: RecursiveInverseMap[a5.type] = (1, 1, "", "", 1)

    val a6 = ((User.id, User.id), (User.id, User.*, User.id))
    val b6: RecursiveInverseMap[a6.type] = (1, 1, 1, 1, "", "", 1)

//    val s = select (User.*, (User.id, User.*)) from User
//    println(s.sql(DB.MYSQL))
//
//    val ss = (select (User.*) from User) exceptAll (select (User.*) from User)
//    println(ss.sql(DB.PGSQL))

//    val groupCube = select (count()) from User groupBy cube(User.id, User.name)
//    println(groupCube.sql(DB.MYSQL))

//    val grouping = select (count()) from User groupBy (groupingSets((1, User.name), User.name, EmptyTuple), cube(User.id, User.name), rollup(User.id, User.name))
//    println(grouping.sql(DB.MYSQL))

//    import org.easysql.dsl.given
//
//    val t1 = User as "t1"
//    val t2 = Post as "t2"
//    val join = (select (User.*)
//        from User
//        leftJoin Post
//        rightJoin (t1 join t2 on t1.id === t2.userId join Post on true) on true
//        leftJoin (Post join User on true) on true
//        join Post on true)
//    println(join.sql(DB.MYSQL))
//
//    val list = List("x", "y", "z")
//    val dynamic = dynamicSelect(list.map(col): _*) from "t1"
//    val union = dynamic union (select (User.id, User.name) from User where User.id > dynamic) union dynamic
//    println(union.sql(DB.MYSQL))
//
//    val u = (select (User.*) from User) union List((1, "", ""), (2, "", ""))
//    println(u.sql(DB.MYSQL))
//    println(u.sql(DB.PGSQL))

//    val id = 1
//    val name = "xxx"
//    val s = sql"select user.* from user where id = $id and name = $name"
//    println(s)

    given DB = DB.PGSQL

//    val i = insertInto(User)(User.id, User.name).values((1, ""))
//
//    i.values((1, ""))
//    println(i.toSql)

//    val s = for {
//        u <- Query(User) if u.id === 1
//        p <- Query(Post) if u.id === p.userId
//    } yield (u.id, p.name)
//    import org.easysql.dsl.given


//    val s = for {
//        u <- User if u.name === "xxx"
//        p <- Post if u.id === p.userId
//    } yield (u.id, p.name)
//    println(s.toSql)

//    val nameList = List("x", "y")
//    val sql = sql"select * from user where name in $nameList"
//    println(sql)

//    val tables: Post.userId.QuoteTables = Tuple1(Post)



    val t1 = (select (max(User.id) as "uid") from User) union (select (min(User.id) as "uid") from User) as "t1"
    val s = select (t1.uid) from t1
    println(s.asSql)

//    val s2 = select (Post.*, User.id) from Post where Post.id === 1
//    println(s2.asSql)
}