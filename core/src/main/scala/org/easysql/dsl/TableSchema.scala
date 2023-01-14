package org.easysql.dsl

import org.easysql.dsl.{AllColumnExpr, TableColumnExpr}
import org.easysql.ast.SqlDataType
import org.easysql.ast.table.SqlJoinType
import org.easysql.macros.*
import org.easysql.query.select.SelectQuery

import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import scala.language.dynamics
import scala.deriving.*

sealed trait AnyTable {
    infix def join(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.JOIN, table)

    infix def leftJoin(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.LEFT_JOIN, table)

    infix def rightJoin(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.RIGHT_JOIN, table)

    infix def innerJoin(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.INNER_JOIN, table)

    infix def crossJoin(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.CROSS_JOIN, table)

    infix def fullJoin(table: AnyTable): JoinTableSchema = JoinTableSchema(this, SqlJoinType.FULL_JOIN, table)
}

trait TableSchema[E <: Product] extends AnyTable with Dynamic with SelectItem[E] {
    val _tableName: String

    var _aliasName: Option[String] = None

    val _cols: ListBuffer[TableColumnExpr[_, _]] = ListBuffer()

    transparent inline def selectDynamic(inline name: String)(using m: Mirror.ProductOf[E]) = {
        inline exprMetaMacro[E](name) match {
            case ("pk", n) => PrimaryKeyColumnExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, name.type] & SqlDataType, name.type](_tableName, n, name, this)
            case ("incr", n) => PrimaryKeyColumnExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, name.type] & SqlDataType, name.type](_tableName, n, name, this, true)
            case (_, n) => TableColumnExpr[ElementType[m.MirroredElemTypes, m.MirroredElemLabels, name.type] & SqlDataType, name.type](_tableName, n, name, this)
        }
    }

    transparent inline def * (using m: Mirror.ProductOf[E]) = {
        val fields = fieldNamesMacro[E].zip(identNamesMacro[E]).toArray.map((n, i) => TableColumnExpr(_tableName, n, i, this))
        Tuple.fromArray(fields).asInstanceOf[ExprType[m.MirroredElemTypes]]
    }

    inline infix def unsafeAs(aliasName: String) = {
        val table = aliasMacro[E, this.type](this)
        table._aliasName = Some(aliasName)
        val cols = table._cols.map(i => i.copy(schema = table))
        table._cols.clear()
        table._cols.addAll(cols)
        table
    }

    inline infix def as(aliasName: String)(using NonEmpty[aliasName.type] =:= Any) = unsafeAs(aliasName)
}

case class JoinTableSchema(
    left: AnyTable, 
    joinType: SqlJoinType, 
    right: AnyTable, 
    var onCondition: Option[Expr[_]] = None
) extends AnyTable {
    infix def on(expr: Expr[_]): JoinTableSchema = {
        this.onCondition = Some(expr)
        this
    }
}