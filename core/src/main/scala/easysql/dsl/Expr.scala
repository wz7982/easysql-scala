package easysql.dsl

import easysql.ast.expr.SqlBinaryOperator
import easysql.ast.order.SqlOrderByOption
import easysql.ast.{SqlDataType, SqlNumberType}
import easysql.query.select.*

import java.util.Date
import scala.annotation.targetName

sealed trait Expr[T <: SqlDataType] {
    def ===(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.EQ, LiteralExpr(value))

    def ===(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.EQ, LiteralExpr(v))
        case None => BinaryExpr(this, SqlBinaryOperator.IS, NullExpr)
    }

    def ===(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.EQ, expr)

    def ===(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.EQ, SubQueryExpr(q))

    def <>(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.NE, LiteralExpr(value))

    def <>(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.NE, LiteralExpr(v))
        case None => BinaryExpr(this, SqlBinaryOperator.IS_NOT, NullExpr)
    }

    def <>(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.NE, expr)

    def <>(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.NE, SubQueryExpr(q))

    def >(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GT, LiteralExpr(value))

    def >(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GT, expr)

    def >(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GT, SubQueryExpr(q))

    def >=(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GE, LiteralExpr(value))

    def >=(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GE, expr)

    def >=(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.GE, SubQueryExpr(q))

    def <(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LT, LiteralExpr(value))

    def <(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LT, expr)

    def <(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LT, SubQueryExpr(q))

    def <=(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LE, LiteralExpr(value))

    def <=(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LE, expr)

    def <=(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.LE, SubQueryExpr(q))

    def in(list: List[T | Expr[T]]): Expr[Boolean] =
        if list.isEmpty then LiteralExpr(false) else {
            val inList = list map {
                case e: Expr[_] => e
                case t: SqlDataType => LiteralExpr(t)
            }
            InExpr(this, ListExpr(inList), false)
        }

    def in(list: (T | Expr[T])*): Expr[Boolean] =
        in(list.toList)

    def notIn(list: List[T | Expr[T]]): Expr[Boolean] =
        if list.isEmpty then LiteralExpr(true) else {
            val inList = list map {
                case e: Expr[_] => e
                case t: SqlDataType => LiteralExpr(t)
            }
            InExpr(this, ListExpr(inList), true)
        }

    def notIn(list: (T | Expr[T])*): Expr[Boolean] =
        notIn(list.toList)

    def in(subQuery: Query[Tuple1[T], _]): Expr[Boolean] = 
        InExpr(this, SubQueryExpr(subQuery), false)

    def notIn(subQuery: Query[Tuple1[T], _]): Expr[Boolean] = 
        InExpr(this, SubQueryExpr(subQuery), true)

    def between(start: T, end: T): Expr[Boolean] =
        BetweenExpr(this, LiteralExpr(start), LiteralExpr(end), false)

    def between(start: Expr[T], end: Expr[T]): Expr[Boolean] =
        BetweenExpr(this, start, end, false)

    def notBetween(start: T, end: T): Expr[Boolean] =
        BetweenExpr(this, LiteralExpr(start), LiteralExpr(end), true)

    def notBetween(start: Expr[T], end: Expr[T]): Expr[Boolean] =
        BetweenExpr(this, start, end, true)

    def asc: OrderBy = 
        OrderBy(this, SqlOrderByOption.ASC)

    def desc: OrderBy = 
        OrderBy(this, SqlOrderByOption.DESC)

    infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
        AliasExpr[T, name.type](this, name)

    infix def unsafeAs(name: String) = 
        AliasExpr[T, name.type](this, name)
}

case class LiteralExpr[T <: SqlDataType](value: T) extends Expr[T]

case object NullExpr extends Expr[Nothing]

case class BinaryExpr[T <: SqlDataType](left: Expr[_], op: SqlBinaryOperator, right: Expr[_]) extends Expr[T] {
    infix def thenIs(value: T): CaseBranch[T] = 
        CaseBranch(this, LiteralExpr(value))

    infix def thenIs(value: Expr[T]): CaseBranch[T] = 
        CaseBranch(this, value)
}

case class IdentExpr[T <: SqlDataType](column: String) extends Expr[T]

case class ColumnExpr[T <: SqlDataType, N <: String](
    tableName: String, 
    columnName: String, 
    identName: N
) extends Expr[T]

case class PrimaryKeyExpr[T <: SqlDataType, N <: String](
    tableName: String, 
    columnName: String, 
    identName: N, 
    inc: Boolean
) extends Expr[T]

case class SubQueryExpr[T <: SqlDataType](query: Query[Tuple1[T], _]) extends Expr[T]

case class FuncExpr[T <: SqlDataType](name: String, args: List[Expr[_]]) extends Expr[T]

case class AggExpr[T <: SqlDataType](
    name: String, 
    args: List[Expr[_]], 
    distinct: Boolean, 
    attrs: Map[String, Expr[_]], 
    orderBy: List[OrderBy]
) extends Expr[T] {
    def over: OverExpr[T] = 
        OverExpr(this, Nil, Nil)
}

case class CaseExpr[T <: SqlDataType](branches: List[CaseBranch[_]], default: Expr[_]) extends Expr[T] {
    infix def elseIs(value: T): CaseExpr[T] =
        copy(default = LiteralExpr(value))

    infix def elseIs(value: Expr[T]): CaseExpr[T] =
        copy(default = value)

    infix def elseIs(value: Option[T]): CaseExpr[T] =
        copy(default = value.map(LiteralExpr(_)).getOrElse(NullExpr))
}

case class ListExpr[T <: SqlDataType](list: List[Expr[_]]) extends Expr[T]

case class InExpr[T <: SqlDataType](expr: Expr[_], inExpr: Expr[_], not: Boolean) extends Expr[Boolean]

case class BetweenExpr[T <: SqlDataType](expr: Expr[_], start: Expr[_], end: Expr[_], not: Boolean) extends Expr[Boolean]

case class AllColumnExpr(owner: Option[String]) extends Expr[Nothing]

case class OverExpr[T <: SqlDataType](func: AggExpr[T], partitionBy: List[Expr[_]], orderBy: List[OrderBy]) extends Expr[T] {
    def partitionBy(p: Expr[_]*): OverExpr[T] =
        copy(partitionBy = partitionBy ++ p)

    def orderBy(o: OrderBy*): OverExpr[T] =
        copy(orderBy = orderBy ++ o)
}

case class CastExpr[T <: SqlDataType](expr: Expr[_], castType: String) extends Expr[T]

trait ExprOperator[T <: SqlDataType] {
    extension (v: T) {
        def ===(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

        def <>(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

        def >(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

        def >=(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

        def <(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

        def <=(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

        def in(list: List[Expr[T]]): Expr[Boolean] =
            if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

        def in(list: Expr[T]*): Expr[Boolean] =
            in(list.toList)

        def notIn(list: List[Expr[T]]): Expr[Boolean] =
            if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), true)

        def notIn(list: Expr[T]*): Expr[Boolean] =
            notIn(list.toList)

        def between(start: Expr[T], end: Expr[T]): Expr[Boolean] =
            BetweenExpr(LiteralExpr(v), start, end, false)

        def notBetween(start: Expr[T], end: Expr[T]): Expr[Boolean] =
            BetweenExpr(LiteralExpr(v), start, end, true)
    }
}

object Expr {
    given numberOperator[T <: SqlNumberType]: ExprOperator[T] with {
        extension [R <: SqlNumberType] (e: Expr[T]) {
            def +(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.ADD, LiteralExpr(value))

            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.ADD, expr)

            def -(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.SUB, LiteralExpr(value))

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.SUB, expr)

            def *(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.MUL, LiteralExpr(value))

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.MUL, expr)

            def /(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.DIV, LiteralExpr(value))

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.DIV, expr)

            def %(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.MOD, LiteralExpr(value))

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.MOD, expr)

            def ===(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.EQ, LiteralExpr(value))

            def ===(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.EQ, LiteralExpr(v))
                case None => BinaryExpr(e, SqlBinaryOperator.IS, NullExpr)
            }

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.EQ, expr)

            def ===(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.EQ, SubQueryExpr(q))

            def <>(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.NE, LiteralExpr(value))

            def <>(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.NE, LiteralExpr(v))
                case None => BinaryExpr(e, SqlBinaryOperator.IS_NOT, NullExpr)
            }

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.NE, expr)

            def <>(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.NE, SubQueryExpr(q))

            def >(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GT, LiteralExpr(value))

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GT, expr)

            def >(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GT, SubQueryExpr(q))

            def >=(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GE, LiteralExpr(value))

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GE, expr)

            def >=(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GE, SubQueryExpr(q))

            def <(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LT, LiteralExpr(value))

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LT, expr)

            def <(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LT, SubQueryExpr(q))

            def <=(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LE, LiteralExpr(value))

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LE, expr)

            def <=(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LE, SubQueryExpr(q))

            def in(list: List[R | Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else {
                    val inList = list map {
                        case expr: Expr[_] => expr
                        case t: SqlDataType => LiteralExpr(t)
                    }
                    InExpr(e, ListExpr(inList), false)
                }

            def in(list: (R | Expr[R])*): Expr[Boolean] =
                in(list.toList)

            def notIn(list: List[R | Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else {
                    val inList = list map {
                        case expr: Expr[_] => expr
                        case t: SqlDataType => LiteralExpr(t)
                    }
                    InExpr(e, ListExpr(inList), true)
                }

            def notIn(list: (R | Expr[R])*): Expr[Boolean] =
                notIn(list.toList)

            def in(subQuery: Query[Tuple1[R], _]): Expr[Boolean] = 
                InExpr(e, SubQueryExpr(subQuery), false)

            def notIn(subQuery: Query[Tuple1[R], _]): Expr[Boolean] = 
                InExpr(e, SubQueryExpr(subQuery), true)

            def between(start: R, end: R): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), false)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(e, start, end, false)

            def notBetween(start: R, end: R): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), true)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(e, start, end, true)
        }

        extension [R <: SqlNumberType] (v: Int) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

            def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

            def in(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), false)

            def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else InExpr(LiteralExpr(v), ListExpr(list), true)

            def notIn(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), true)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, false)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, true)

            def unsafeAs(name: String) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: Long) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

            def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

            def in(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), false)

            def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else InExpr(LiteralExpr(v), ListExpr(list), true)

            def notIn(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), true)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, false)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, true)

            def unsafeAs(name: String) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
            
            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: Float) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

            def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

            def in(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), false)

            def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else InExpr(LiteralExpr(v), ListExpr(list), true)

            def notIn(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), true)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, false)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, true)

            def unsafeAs(name: String) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v.toDouble), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v.toDouble), name)
        }

        extension [R <: SqlNumberType] (v: Double) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

            def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

            def in(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), false)

            def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else InExpr(LiteralExpr(v), ListExpr(list), true)

            def notIn(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), true)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, false)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, true)

            def unsafeAs(name: String) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: BigDecimal) {
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.ADD, expr)

            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.SUB, expr)

            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MUL, expr)

            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.DIV, expr)

            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.MOD, expr)

            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.EQ, expr)

            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.NE, expr)

            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GT, expr)

            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.GE, expr)

            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LT, expr)

            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.LE, expr)

            def in(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(false) else InExpr(LiteralExpr(v), ListExpr(list), false)

            def in(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), false)

            def notIn(list: List[Expr[R]]): Expr[Boolean] =
                if list.isEmpty then LiteralExpr(true) else InExpr(LiteralExpr(v), ListExpr(list), true)

            def notIn(list: (Expr[R])*): Expr[Boolean] =
                InExpr(LiteralExpr(v), ListExpr(list.toList), true)

            def between(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, false)

            def notBetween(start: Expr[R], end: Expr[R]): Expr[Boolean] =
                BetweenExpr(LiteralExpr(v), start, end, true)

            def unsafeAs(name: String) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
            
            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[BigDecimal, name.type](LiteralExpr(v), name)
        }
    }

    given stringOperator: ExprOperator[String] with {
        extension (e: Expr[String]) {
            infix def like(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.LIKE, LiteralExpr(value))

            infix def like(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.LIKE, expr)

            infix def notLike(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NOT_LIKE, LiteralExpr(value))

            infix def notLike(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NOT_LIKE, expr)

            def ->(json: Int): BinaryExpr[String] = 
                BinaryExpr(e, SqlBinaryOperator.JSON, LiteralExpr(json))

            def ->(json: String): BinaryExpr[String] = 
                BinaryExpr(e, SqlBinaryOperator.JSON, LiteralExpr(json))

            def ->>(json: Int): BinaryExpr[String] = 
                BinaryExpr(e, SqlBinaryOperator.JSON_TEXT, LiteralExpr(json))

            def ->>(json: String): BinaryExpr[String] = 
                BinaryExpr(e, SqlBinaryOperator.JSON_TEXT, LiteralExpr(json))
        }

        extension (v: String) {
            def unsafeAs(name: String) = 
                AliasExpr[String, name.type](LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[String, name.type](LiteralExpr(v), name)
        }
    }

    given boolOperator: ExprOperator[Boolean] with {
        extension (e: Expr[Boolean]) {
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.AND, query)

            def &&(v: Boolean): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.AND, LiteralExpr(v))

            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.OR, query)

            def ||(v: Boolean): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.OR, LiteralExpr(v))

            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.XOR, query)

            def ^(v: Boolean): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.XOR, LiteralExpr(v))

            def unary_! : FuncExpr[Boolean] = 
                FuncExpr("NOT", List(e))
        }

        extension (v: Boolean) {
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.AND, query)

            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.OR, query)

            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.XOR, query)

            def unsafeAs(name: String) = 
                AliasExpr[Boolean, name.type](LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[Boolean, name.type](LiteralExpr(v), name)            
        }
    }

    given dateOperator: ExprOperator[Date] with {
        extension (e: Expr[Date]) {
            def ===(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.EQ, LiteralExpr(s))

            def <>(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.NE, LiteralExpr(s))

            def >(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GT, LiteralExpr(s))

            def >=(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.GE, LiteralExpr(s))

            def <(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LT, LiteralExpr(s))

            def <=(s: String): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.LE, LiteralExpr(s))

            def between(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), false)

            def notBetween(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), true)
        }

        extension (v: Date) {
            def unsafeAs(name: String) = 
                AliasExpr[Date, name.type](LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true) = 
                AliasExpr[Date, name.type](LiteralExpr(v), name)
        }
    }
}

case class AliasExpr[T <: SqlDataType, Alias <: String](expr: Expr[T], name: Alias)

case class CaseBranch[T](expr: Expr[_], thenValue: Expr[_])

case class OrderBy(expr: Expr[_], order: SqlOrderByOption)