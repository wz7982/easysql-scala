package easysql.dsl

import easysql.ast.table.*
import easysql.macros.*
import easysql.ast.SqlDataType

import java.util.Date
import scala.language.dynamics
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
) extends AnyTable with Dynamic {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N)(using m: Mirror.ProductOf[E]) = {
        val tableName = __aliasName.getOrElse(__tableName)
        inline exprMeta[E](name) match {
            case ("pk", n) => 
                PrimaryKeyExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, N] & SqlDataType, N](tableName, n, name, false)
            case ("incr", n) => 
                PrimaryKeyExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, N] & SqlDataType, N](tableName, n, name, true)
            case ("Int", n) =>
                ColumnExpr[Int, N](tableName, n, name)
            case ("Long", n) =>
                ColumnExpr[Long, N](tableName, n, name)
            case ("Float", n) =>
                ColumnExpr[Float, N](tableName, n, name)
            case ("Double", n) =>
                ColumnExpr[Double, N](tableName, n, name)
            case ("BigDecimal", n) =>
                ColumnExpr[BigDecimal, N](tableName, n, name)
            case ("Boolean", n) =>
                ColumnExpr[Boolean, N](tableName, n, name)
            case ("String", n) =>
                ColumnExpr[String, N](tableName, n, name)
            case ("Date", n) =>
                ColumnExpr[Date, N](tableName, n, name)
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
    onCondition: Option[Expr[Boolean]]
) extends AnyTable {
    infix def on(expr: Expr[Boolean]): JoinTable = 
        copy(onCondition = Some(expr))
}