package org.easysql.dsl

import org.easysql.ast.expr.{SqlBinaryOperator, SqlExpr, SqlSubQueryPredicate}
import org.easysql.visitor.*
import org.easysql.ast.order.SqlOrderByOption
import org.easysql.dsl.const
import org.easysql.query.select.SelectQuery
import org.easysql.ast.{SqlDataType, SqlNumberType}
import org.easysql.util.anyToExpr

import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.Tuple.Concat

trait SelectItem[T]

case class AliasExpr[T <: SqlDataType, Alias <: String](expr: Expr[T], name: Alias) extends SelectItem[T]

trait ExprOperator[T <: SqlDataType] {
    extension (v: T) {
        def ===(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

        def <>(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

        def >(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

        def >=(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

        def <(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

        def <=(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

        infix def in(list: List[Expr[T]]): Expr[Boolean] =
            if list.isEmpty then const(false) else InListExpr(const(v), list)

        infix def in(list: Expr[T]*): Expr[Boolean] =
            InListExpr(const(v), list.toList)

        infix def notIn(list: List[Expr[T]]): Expr[Boolean] =
            if list.isEmpty then const(true) else InListExpr(const(v), list, true)

        infix def notIn(list: Expr[T]*): Expr[Boolean] =
            InListExpr(const(v), list.toList, true)

        infix def between(start: Expr[T], end: Expr[T]): Expr[Boolean] =
            BetweenExpr(const(v), start, end)

        infix def notBetween(start: Expr[T], end: Expr[T]): Expr[Boolean] =
            BetweenExpr(const(v), start, end, true)
    }
}

sealed trait Expr[T <: SqlDataType] extends SelectItem[T] {
    def ===(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.EQ, const(value))

    def ===(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.EQ, const(v))
        case None => BinaryExpr(this, SqlBinaryOperator.EQ, const(null))
    }

    def ===(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.EQ, expr)

    def ===(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.EQ, SubQueryExpr(q))

    def <>(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.NE, const(value))

    def <>(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.NE, const(v))
        case None => BinaryExpr(this, SqlBinaryOperator.NE, const(null))
    }

    def <>(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.NE, expr)

    def <>(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.NE, SubQueryExpr(q))

    def >(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GT, const(value))

    def >(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GT, expr)

    def >(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GT, SubQueryExpr(q))

    def >=(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GE, const(value))

    def >=(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GE, expr)

    def >=(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.GE, SubQueryExpr(q))

    def <(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LT, const(value))

    def <(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LT, expr)

    def <(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LT, SubQueryExpr(q))

    def <=(value: T): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LE, const(value))

    def <=(expr: Expr[T]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LE, expr)

    def <=(q: SelectQuery[Tuple1[T], _]): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.LE, SubQueryExpr(q))

    infix def in(list: List[T | Expr[T]]): Expr[Boolean] =
        if list.isEmpty then const(false) else InListExpr(this, list)

    infix def in(list: (T | Expr[T])*): Expr[Boolean] =
        InListExpr(this, list.toList)

    infix def notIn(list: List[T | Expr[T]]): Expr[Boolean] =
        if list.isEmpty then const(true) else InListExpr(this, list, true)

    infix def notIn(list: (T | Expr[T])*): Expr[Boolean] =
        InListExpr(this, list.toList, true)

    infix def in(subQuery: SelectQuery[Tuple1[T], _]): Expr[Boolean] = InSubQueryExpr(this, subQuery)

    infix def notIn(subQuery: SelectQuery[Tuple1[T], _]): Expr[Boolean] = InSubQueryExpr(this, subQuery, true)

    infix def between(start: T | Expr[T] | SelectQuery[Tuple1[T], _], end: T | Expr[T] | SelectQuery[Tuple1[T], _]): Expr[Boolean] =
        BetweenExpr(this, start, end)

    infix def notBetween(start: T | Expr[T] | SelectQuery[Tuple1[T], _], end: T | Expr[T] | SelectQuery[Tuple1[T], _]): Expr[Boolean] =
        BetweenExpr(this, start, end, true)

    def equal(expr: Any): BinaryExpr[Boolean] = BinaryExpr(this, SqlBinaryOperator.EQ, anyToExpr(expr))

    def asc: OrderBy = OrderBy(this, SqlOrderByOption.ASC)

    def desc: OrderBy = OrderBy(this, SqlOrderByOption.DESC)

    inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[T, name.type](this, name)

    infix def unsafeAs(name: String) = AliasExpr[T, name.type](this, name)
}

object Expr {
    given numberOperator[T <: SqlNumberType]: ExprOperator[T] with {
        extension [R <: SqlNumberType] (e: Expr[T]) {
            def +(value: R): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.ADD, const(value))

            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.ADD, expr)

            def -(value: R): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.SUB, const(value))

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.SUB, expr)

            def *(value: R): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.MUL, const(value))

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.MUL, expr)

            def /(value: R): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.DIV, const(value))

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.DIV, expr)

            def %(value: R): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.MOD, const(value))

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(e, SqlBinaryOperator.MOD, expr)

            def ===(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.EQ, const(value))

            def ===(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.EQ, const(v))
                case None => BinaryExpr(e, SqlBinaryOperator.EQ, const(null))
            }

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.EQ, expr)

            def ===(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.EQ, SubQueryExpr(q))

            def <>(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.NE, const(value))

            def <>(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.NE, const(v))
                case None => BinaryExpr(e, SqlBinaryOperator.NE, const(null))
            }

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.NE, expr)

            def <>(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.NE, SubQueryExpr(q))

            def >(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GT, const(value))

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GT, expr)

            def >(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GT, SubQueryExpr(q))

            def >=(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GE, const(value))

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GE, expr)

            def >=(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GE, SubQueryExpr(q))

            def <(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LT, const(value))

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LT, expr)

            def <(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LT, SubQueryExpr(q))

            def <=(value: R): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LE, const(value))

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LE, expr)

            def <=(q: SelectQuery[Tuple1[R], _]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LE, SubQueryExpr(q))

            infix def in(list: List[R | Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(e, list)

            infix def in(list: (R | Expr[R])*): Expr[Boolean] =
                InListExpr(e, list.toList)

            infix def notIn(list: List[R | Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(e, list, true)

            infix def notIn(list: (R | Expr[R])*): Expr[Boolean] =
                InListExpr(e, list.toList, true)

            infix def in(subQuery: SelectQuery[Tuple1[R], _]): Expr[Boolean] = InSubQueryExpr(e, subQuery)

            infix def notIn(subQuery: SelectQuery[Tuple1[R], _]): Expr[Boolean] = InSubQueryExpr(e, subQuery, true)

            infix def between(start: R | Expr[R] | SelectQuery[Tuple1[R], _], end: R | Expr[R] | SelectQuery[Tuple1[R], _]): Expr[Boolean] =
                BetweenExpr(e, start, end)

            infix def notBetween(start: R | Expr[R] | SelectQuery[Tuple1[R], _], end: R | Expr[R] | SelectQuery[Tuple1[R], _]): Expr[Boolean] =
                BetweenExpr(e, start, end, true)
        }

        extension [R <: SqlNumberType] (v: Int) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

            infix def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(const(v), list)

            infix def in(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList)

            infix def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(const(v), list, true)

            infix def notIn(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList, true)

            infix def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end)

            infix def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end, true)

            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[BigDecimal, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[BigDecimal, name.type](const(v), name)
        }

        extension [R <: SqlNumberType] (v: Long) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

            infix def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(const(v), list)

            infix def in(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList)

            infix def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(const(v), list, true)

            infix def notIn(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList, true)

            infix def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end)

            infix def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end, true)
            
            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[BigDecimal, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[BigDecimal, name.type](const(v), name)
        }

        extension [R <: SqlNumberType] (v: Float) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

            infix def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(const(v), list)

            infix def in(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList)

            infix def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(const(v), list, true)

            infix def notIn(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList, true)

            infix def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end)

            infix def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end, true)

            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[BigDecimal, name.type](const(v.toDouble), name)

            infix def unsafeAs(name: String) = AliasExpr[BigDecimal, name.type](const(v.toDouble), name)
        }

        extension [R <: SqlNumberType] (v: Double) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

            infix def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(const(v), list)

            infix def in(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList)

            infix def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(const(v), list, true)

            infix def notIn(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList, true)

            infix def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end)

            infix def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end, true)

            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[BigDecimal, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[BigDecimal, name.type](const(v), name)
        }

        extension [R <: SqlNumberType] (v: BigDecimal) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = BinaryExpr(const(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.LE, expr)

            infix def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(false) else InListExpr(const(v), list)

            infix def in(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList)

            infix def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then const(true) else InListExpr(const(v), list, true)

            infix def notIn(list: (Expr[R])*): Expr[Boolean] =
                InListExpr(const(v), list.toList, true)

            infix def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end)

            infix def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(const(v), start, end, true)
            
            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[BigDecimal, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[BigDecimal, name.type](const(v), name)
        }
    }

    given stringOperator: ExprOperator[String] with {
        extension (e: Expr[String]) {
            infix def like(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.LIKE, const(value))

            infix def like(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.LIKE, expr)

            infix def notLike(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NOT_LIKE, const(value))

            infix def notLike(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NOT_LIKE, expr)

            def ->(json: Int): BinaryExpr[String] = BinaryExpr(e, SqlBinaryOperator.SUB_GT, const(json))

            def ->(json: String): BinaryExpr[String] = BinaryExpr(e, SqlBinaryOperator.SUB_GT, const(json))

            def ->>(json: Int): BinaryExpr[String] = BinaryExpr(e, SqlBinaryOperator.SUB_GT_GT, const(json))

            def ->>(json: String): BinaryExpr[String] = BinaryExpr(e, SqlBinaryOperator.SUB_GT_GT, const(json))
        }

        extension (v: String) {
            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[String, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[String, name.type](const(v), name)
        }
    }

    given boolOperator: ExprOperator[Boolean] with {
        extension (e: Expr[Boolean]) {
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.AND, query)

            def &&(v: Boolean): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.AND, const(v))

            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.OR, query)

            def ||(v: Boolean): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.OR, const(v))

            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.XOR, query)

            def ^(v: Boolean): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.XOR, const(v))

            def unary_! : NormalFunctionExpr[Boolean] = NormalFunctionExpr("NOT", List(e))
        }

        extension (v: Boolean) {
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.AND, query)

            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.OR, query)

            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] = BinaryExpr(const(v), SqlBinaryOperator.XOR, query)

            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[Boolean, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[Boolean, name.type](const(v), name)
        }
    }

    given dateOperator: ExprOperator[Date] with {
        extension (e: Expr[Date]) {
            def ===(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.EQ, const(s))

            def <>(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.NE, const(s))

            def >(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GT, const(s))

            def >=(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.GE, const(s))

            def <(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LT, const(s))

            def <=(s: String): BinaryExpr[Boolean] = BinaryExpr(e, SqlBinaryOperator.LE, const(s))

            infix def between(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, start, end)

            infix def notBetween(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, start, end, true)
        }

        extension (v: Date) {
            inline infix def as(inline name: String)(using NonEmpty[name.type] =:= Any) = AliasExpr[Date, name.type](const(v), name)

            infix def unsafeAs(name: String) = AliasExpr[Date, name.type](const(v), name)
        }
    }
}

case class ConstExpr[T <: SqlDataType](value: T) extends Expr[T]

case class BinaryExpr[T <: SqlDataType](
    left: Expr[_],
    operator: SqlBinaryOperator,
    right: Expr[_]
) extends Expr[T] {
    def thenIs[TV <: SqlDataType](thenValue: TV | Expr[TV] | SelectQuery[Tuple1[TV], _]): CaseBranch[TV] =
        CaseBranch(this, thenValue)
}

case class ColumnExpr[T <: SqlDataType](column: String) extends Expr[T]()

case class TableColumnExpr[T <: SqlDataType, Name <: String](
    table: String,
    column: String,
    ident: Name,
    schema: TableSchema[_]
) extends Expr[T]

case class PrimaryKeyColumnExpr[T <: SqlDataType, Name <: String](
    table: String,
    column: String,
    ident: Name,
    schema: TableSchema[_],
    isIncr: Boolean = false
) extends Expr[T]

case class SubQueryExpr[T <: SqlDataType](selectQuery: SelectQuery[Tuple1[T], _]) extends Expr[T]

case class NormalFunctionExpr[T <: SqlDataType](name: String, args: List[Expr[_]]) extends Expr[T]

case class AggFunctionExpr[T <: SqlDataType](
    name: String,
    args: List[Expr[_]],
    distinct: Boolean = false,
    attributes: Map[String, Expr[_]] = Map(),
    orderBy: List[OrderBy] = List()
) extends Expr[T] {
    def over: OverExpr[T] = OverExpr(this)
}

case class CaseExpr[T <: SqlDataType](
    conditions: List[CaseBranch[T]],
    default: T | Expr[T] | SelectQuery[Tuple1[T], _] = null
) extends Expr[T] {
    infix def elseIs(value: T | Expr[T] | SelectQuery[Tuple1[T], _]): CaseExpr[T] =
        if value != null then CaseExpr(this.conditions, value) else this
}

case class ListExpr[T <: SqlDataType](list: List[T | Expr[_] | SelectQuery[_, _]]) extends Expr[T]

case class InListExpr[T <: SqlDataType](
    query: Expr[_],
    list: List[T | Expr[_] | SelectQuery[_, _]],
    isNot: Boolean = false
) extends Expr[Boolean]

case class InSubQueryExpr[T <: SqlDataType](
    query: Expr[T], 
    subQuery: SelectQuery[_, _], 
    isNot: Boolean = false
) extends Expr[Boolean]

case class CastExpr[T <: SqlDataType](query: Expr[_], castType: String) extends Expr[T]

case class BetweenExpr[T <: SqlDataType](
    query: Expr[_],
    start: T | Expr[_] | SelectQuery[_, _],
    end: T | Expr[_] | SelectQuery[_, _],
    isNot: Boolean = false
) extends Expr[Boolean]

case class AllColumnExpr(owner: Option[String] = None) extends Expr[Nothing]()

case class OverExpr[T <: SqlDataType](
    function: AggFunctionExpr[_],
    partitionBy: ListBuffer[Expr[_]] = ListBuffer(),
    orderBy: ListBuffer[OrderBy] = ListBuffer()
) extends Expr[T] {
    def partitionBy(query: Expr[_]*): OverExpr[T] = OverExpr(this.function, this.partitionBy.addAll(query), this.orderBy)

    def orderBy(order: OrderBy*): OverExpr[T] = OverExpr(this.function, this.partitionBy, this.orderBy.addAll(order))
}

case class SubQueryPredicateExpr[T <: SqlDataType](query: SelectQuery[_, _], predicate: SqlSubQueryPredicate) extends Expr[T]

case class CaseBranch[T <: SqlDataType](query: Expr[_], thenValue: T | Expr[T] | SelectQuery[Tuple1[T], _])

case class OrderBy(query: Expr[_], order: SqlOrderByOption)