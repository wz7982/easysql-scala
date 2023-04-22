import easysql.dsl
import easysql.dsl.*

import java.util.Date

@Table("user")
case class User(@IncrKey("uid") id: Int, name: String, createTime: Date)

val user = asTable[User]

@Table("post")
case class Post(@IncrKey id: Int, @Column("user_id") userId: Int, @Column name: String)

val post = asTable[Post]
