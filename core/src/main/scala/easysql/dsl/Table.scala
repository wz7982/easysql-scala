package easysql.dsl

import easysql.ast.table.*
import easysql.macros.*
import easysql.ast.SqlDataType

import scala.language.dynamics
import scala.deriving.*

sealed trait AnyTable {
    infix def join(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.JOIN, table)

    infix def leftJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.LEFT_JOIN, table)

    infix def rightJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.RIGHT_JOIN, table)

    infix def innerJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.INNER_JOIN, table)

    infix def crossJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.CROSS_JOIN, table)

    infix def fullJoin(table: AnyTable): JoinTable = JoinTable(this, SqlJoinType.FULL_JOIN, table)
}

class TableSchema[E <: Product](
    private[easysql] val __tableName: String,
    private[easysql] val __aliasName: Option[String],
    private[easysql] val __cols: List[ColumnExpr[_, _]]
) extends AnyTable with Dynamic with SelectItem[E] {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N)(using m: Mirror.ProductOf[E]) = {
        val tableName = __aliasName.getOrElse(__tableName)
        inline exprMeta[E](name) match {
            case ("pk", n) => 
                PrimaryKeyExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, N] & SqlDataType, N](tableName, n, name, false)
            case ("incr", n) => 
                PrimaryKeyExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, N] & SqlDataType, N](tableName, n, name, true)
            case (_, n) => 
                ColumnExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, N] & SqlDataType, N](tableName, n, name)
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
    onCondition: Option[Expr[_]] = None
) extends AnyTable {
    infix def on(expr: Expr[_]): JoinTable = this.copy(onCondition = Some(expr))
}