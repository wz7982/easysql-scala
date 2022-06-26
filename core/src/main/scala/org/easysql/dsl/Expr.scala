package org.easysql.dsl

import org.easysql.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubQueryPredicate}
import org.easysql.visitor.*
import org.easysql.ast.order.SqlOrderByOption
import org.easysql.dsl.const
import org.easysql.query.select.SelectQuery
import org.easysql.ast.SqlSingleConstType
import org.easysql.util.anyToExpr

import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

sealed trait Expr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](var alias: Option[String] = None) {
    def +[V <: T & SqlSingleConstType](value: V) = BinaryExpr[T, Table](this, SqlBinaryOperator.ADD, const(value))

    def +[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[T, TableConcat[Table, ET]](this, SqlBinaryOperator.ADD, expr)

    def +[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[T, NothingTable](this, SqlBinaryOperator.ADD, SubQueryExpr(subQuery))

    def -[V <: T & SqlSingleConstType](value: V) = BinaryExpr[T, Table](this, SqlBinaryOperator.SUB, const(value))

    def -[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[T, TableConcat[Table, ET]](this, SqlBinaryOperator.SUB, expr)

    def -[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[T, NothingTable](this, SqlBinaryOperator.SUB, SubQueryExpr(subQuery))

    def *[V <: T & SqlSingleConstType](value: V) = BinaryExpr[T, Table](this, SqlBinaryOperator.MUL, const(value))

    def *[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[T, TableConcat[Table, ET]](this, SqlBinaryOperator.MUL, expr)

    def *[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[T, NothingTable](this, SqlBinaryOperator.MUL, SubQueryExpr(subQuery))

    def /[V <: T & SqlSingleConstType](value: V) = BinaryExpr[T, Table](this, SqlBinaryOperator.DIV, const(value))

    def /[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[T, TableConcat[Table, ET]](this, SqlBinaryOperator.DIV, expr)

    def /[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[T, NothingTable](this, SqlBinaryOperator.DIV, SubQueryExpr(subQuery))

    def %[V <: T & SqlSingleConstType](value: V) = BinaryExpr[T, Table](this, SqlBinaryOperator.MOD, const(value))

    def %[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[T, TableConcat[Table, ET]](this, SqlBinaryOperator.MOD, expr)

    def %[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[T, NothingTable](this, SqlBinaryOperator.MOD, SubQueryExpr(subQuery))

    def ==[V <: T](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.EQ, const(value))

    def ==[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.EQ, expr)

    def ==[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.EQ, SubQueryExpr(subQuery))

    def ===[V <: T](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.EQ, const(value))

    def ===[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.EQ, expr)

    def ===[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.EQ, SubQueryExpr(subQuery))

    def equal(expr: Any) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.EQ, anyToExpr(expr))

    def <>[V <: T](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.NE, const(value))

    def <>[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.NE, expr)

    def <>[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.NE, SubQueryExpr(subQuery))

    def >[V <: T & SqlSingleConstType](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.GT, const(value))

    def >[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.GT, expr)

    def >[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.GT, SubQueryExpr(subQuery))

    def >=[V <: T & SqlSingleConstType](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.GE, const(value))

    def >=[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.GE, expr)

    def >=[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.GE, SubQueryExpr(subQuery))

    def <[V <: T & SqlSingleConstType](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.LT, const(value))

    def <[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.LT, expr)

    def <[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.LT, SubQueryExpr(subQuery))

    def <=[V <: T & SqlSingleConstType](value: V) = BinaryExpr[Boolean, Table](this, SqlBinaryOperator.LE, const(value))

    def <=[V <: T | Null, ET <: TableSchema | Tuple](expr: Expr[V, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.LE, expr)

    def <=[V <: T | Null](subQuery: SelectQuery[Tuple1[V]]) = BinaryExpr[Boolean, NothingTable](this, SqlBinaryOperator.LE, SubQueryExpr(subQuery))

    infix def &&[ET <: TableSchema | Tuple](query: Expr[_, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.AND, query)

    infix def ||[ET <: TableSchema | Tuple](query: Expr[_, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.OR, query)

    infix def ^[ET <: TableSchema | Tuple](query: Expr[_, ET]) = BinaryExpr[Boolean, TableConcat[Table, ET]](this, SqlBinaryOperator.XOR, query)

    infix def in[V <: T](list: List[V | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]]]): Expr[Boolean, NothingTable] = {
        InListExpr(this, list)
    }

    infix def in[V <: T](list: (V | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]])*): Expr[Boolean, NothingTable] = {
        InListExpr(this, list.toList)
    }

    infix def notIn[V <: T](list: List[V | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]]]): Expr[Boolean, NothingTable] = {
        InListExpr(this, list, true)
    }

    infix def notIn[V <: T](list: (V | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]])*): Expr[Boolean, NothingTable] = {
        InListExpr(this, list.toList, true)
    }

    infix def in(subQuery: SelectQuery[Tuple1[T]] | SelectQuery[Tuple1[T | Null]]): Expr[Boolean, NothingTable] = InSubQueryExpr(this, subQuery)

    infix def notIn(subQuery: SelectQuery[Tuple1[T]] | SelectQuery[Tuple1[T | Null]]): Expr[Boolean, NothingTable] = InSubQueryExpr(this, subQuery, true)

    infix def between[V <: T](between: Tuple2[(V & SqlSingleConstType) | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]], (V & SqlSingleConstType) | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]]]): Expr[Boolean, NothingTable] = {
        BetweenExpr(this, between._1, between._2)
    }

    infix def notBetween[V <: T](between: Tuple2[(V & SqlSingleConstType) | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]], (V & SqlSingleConstType) | Expr[V, _] | Expr[V | Null, _] | SelectQuery[Tuple1[V]] | SelectQuery[Tuple1[V | Null]]]): Expr[Boolean, NothingTable] = {
        BetweenExpr(this, between._1, between._2, true)
    }

    def asc = OrderBy[Table](this, SqlOrderByOption.ASC)

    def desc = OrderBy[Table](this, SqlOrderByOption.DESC)

    infix def as(name: String)(using NonEmpty[name.type] =:= Any): Expr[T, Table] = {
        this.alias = Some(name)
        this
    }

    infix def unsafeAs(name: String): Expr[T, Table] = {
        this.alias = Some(name)
        this
    }
}

extension[T <: String | Null] (e: Expr[T, _]) {
    infix def like(value: String | Expr[String, _] | Expr[String | Null, _]): BinaryExpr[Boolean, NothingTable] = {
        val query = value match {
            case s: String => const(s)
            case q: Expr[_, _] => q
        }
        BinaryExpr(e, SqlBinaryOperator.LIKE, query)
    }

    infix def notLike(value: String | Expr[String, _] | Expr[String | Null, _]): BinaryExpr[Boolean, NothingTable] = {
        val query = value match {
            case s: String => const(s)
            case q: Expr[_, _] => q
        }
        BinaryExpr(e, SqlBinaryOperator.NOT_LIKE, query)
    }
}

case class ConstExpr[T <: SqlSingleConstType | Null](value: T) extends Expr[T, NothingTable]()

case class BinaryExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](left: Expr[_, _],
                                                      operator: SqlBinaryOperator,
                                                      right: Expr[_, _]) extends Expr[T, Table]() {
    def thenIs[TV <: SqlSingleConstType | Null](thenValue: TV | Expr[TV, _] | SelectQuery[Tuple1[TV]]): CaseBranch[TV] = {
        CaseBranch(this, thenValue)
    }
}

case class ColumnExpr[T <: SqlSingleConstType | Null](column: String) extends Expr[T, NothingTable]()

case class TableColumnExpr[T <: SqlSingleConstType | Null, Table <: TableSchema](table: String,
                                                    column: String) extends Expr[T, Table]() {
    def primaryKey: PrimaryKeyColumnExpr[T & SqlSingleConstType, Table] = {
        PrimaryKeyColumnExpr(table, column)
    }

    def nullable: TableColumnExpr[T | Null, Table] = {
        val copy: TableColumnExpr[T | Null, Table] = this.copy()
        copy
    }

    override infix def as(name: String)(using NonEmpty[name.type] =:= Any): Expr[T, Table] = {
        val copy: TableColumnExpr[T, Table] = this.copy()
        copy.alias = Some(name)
        copy
    }

    override infix def unsafeAs(name: String): Expr[T, Table] = {
        val copy: TableColumnExpr[T, Table] = this.copy()
        copy.alias = Some(name)
        copy
    }
}

extension [T <: Int | Long, Table <: TableSchema](t: TableColumnExpr[T, Table]) {
    def incr: PrimaryKeyColumnExpr[T, Table] = {
        PrimaryKeyColumnExpr(t.table, t.column, true)
    }
}

case class PrimaryKeyColumnExpr[T <: SqlSingleConstType, Table <: TableSchema](table: String,
                                                         column: String,
                                                         var isIncr: Boolean = false) extends Expr[T, Table]() {
    override infix def as(name: String)(using NonEmpty[name.type] =:= Any): Expr[T, Table] = {
        val copy: PrimaryKeyColumnExpr[T, Table] = this.copy()
        copy.alias = Some(name)
        copy
    }

    override infix def unsafeAs(name: String): Expr[T, Table] = {
        val copy: PrimaryKeyColumnExpr[T, Table] = this.copy()
        copy.alias = Some(name)
        copy
    }
}

case class SubQueryExpr[T <: SqlSingleConstType | Null](selectQuery: SelectQuery[Tuple1[T]]) extends Expr[T, NothingTable]()

case class NormalFunctionExpr[T <: SqlSingleConstType | Null](name: String, args: List[Expr[_, _]]) extends Expr[T, NothingTable]()

case class AggFunctionExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](name: String,
                                                           args: List[Expr[_, _]],
                                                           distinct: Boolean = false,
                                                           attributes: Map[String, Expr[_, _]] = Map(),
                                                           orderBy: List[OrderBy[_]] = List()) extends Expr[T, Table]() {
    def over: OverExpr[T, Table] = OverExpr(this)
}

case class CaseExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](conditions: List[CaseBranch[T]],
                                                    var default: T | Expr[T, _] | SelectQuery[Tuple1[T]] | Null = null) extends Expr[T, Table]() {
    infix def elseIs(value: T | Expr[T, _] | SelectQuery[Tuple1[T]] | Null) = {
        if (value != null) {
            CaseExpr[T, NothingTable](this.conditions, value)
        } else {
            this
        }
    }
}

case class ListExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](list: List[T | Expr[_, _] | SelectQuery[_]]) extends Expr[T, Table]()

case class InListExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](query: Expr[_, _],
                                                      list: List[T | Expr[_, _] | SelectQuery[_]],
                                                      isNot: Boolean = false) extends Expr[Boolean, Table]()

case class InSubQueryExpr[T <: SqlSingleConstType | Null](query: Expr[T, _], subQuery: SelectQuery[_], isNot: Boolean = false) extends Expr[Boolean, NothingTable]()

case class CastExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](query: Expr[_, _], castType: String) extends Expr[T, Table]()

case class BetweenExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](query: Expr[_, _],
                                                       start: T | Expr[_, _] | SelectQuery[_],
                                                       end: T | Expr[_, _] | SelectQuery[_],
                                                       isNot: Boolean = false) extends Expr[Boolean, Table]()

case class AllColumnExpr(owner: Option[String] = None) extends Expr[Nothing, NothingTable]()

case class OverExpr[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](function: AggFunctionExpr[_, _],
                                                    partitionBy: ListBuffer[Expr[_, _]] = ListBuffer(),
                                                    orderBy: ListBuffer[OrderBy[_]] = ListBuffer()) extends Expr[T, Table]() {
    def partitionBy(query: Expr[_, _]*): OverExpr[T, Table] = OverExpr(this.function, this.partitionBy.addAll(query), this.orderBy)

    def orderBy(order: OrderBy[_]*): OverExpr[T, Table] = OverExpr(this.function, this.partitionBy, this.orderBy.addAll(order))
}

case class SubQueryPredicateExpr[T <: SqlSingleConstType | Null](query: SelectQuery[_], predicate: SqlSubQueryPredicate) extends Expr[T, NothingTable]()

case class CaseBranch[T <: SqlSingleConstType | Null](query: Expr[_, _], thenValue: T | Expr[T, _] | SelectQuery[Tuple1[T]])

case class OrderBy[Table <: TableSchema | Tuple](query: Expr[_, Table], order: SqlOrderByOption)