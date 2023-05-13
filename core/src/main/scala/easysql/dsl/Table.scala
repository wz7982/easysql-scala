package easysql.dsl

import easysql.ast.table.*
import easysql.macros.*
import easysql.ast.SqlDataType

import java.util.Date
import scala.reflect.Selectable.reflectiveSelectable
import scala.deriving.*

sealed trait AnyTable {
    infix def join(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.Join, table, None)

    infix def leftJoin(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.LeftJoin, table, None)

    infix def rightJoin(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.RightJoin, table, None)

    infix def innerJoin(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.InnerJoin, table, None)

    infix def crossJoin(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.CrossJoin, table, None)

    infix def fullJoin(table: AnyTable): JoinTable = 
        JoinTable(this, SqlJoinType.FullJoin, table, None)
}

class TableSchema[E <: Product](
    private[easysql] val __tableName: String,
    private[easysql] val __aliasName: Option[String],
    private[easysql] val __cols: List[ColumnExpr[_, _]]
) extends AnyTable with Selectable {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N): Expr[_] = {
        val tableName = __aliasName.getOrElse(__tableName)
        val columnInfo = __cols.find(_.identName == name).get
        inline exprMeta[E](name) match {
            case "pk" => PrimaryKeyExpr(tableName, columnInfo.columnName, columnInfo.identName, false)
            case "incr" => PrimaryKeyExpr(tableName, columnInfo.columnName, columnInfo.identName, true)
            case _ => columnInfo
        }
    }

    def unsafeAs(aliasName: String): TableSchema[E] =
        new TableSchema(this.__tableName, Some(aliasName), __cols.map(_.copy(tableName = aliasName)))

    infix def as(aliasName: String)(using NonEmpty[aliasName.type] =:= true): TableSchema[E] =
        unsafeAs(aliasName)
}

case class JoinTable(
    left: AnyTable, 
    joinType: SqlJoinType, 
    right: AnyTable, 
    onCondition: Option[Expr[Boolean]]
) extends AnyTable {
    infix def on(expr: Expr[Boolean]): JoinTable = 
        copy(onCondition = Some(expr))
}