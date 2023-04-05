package easysql.dsl

import easysql.ast.expr.SqlBinaryOperator
import easysql.ast.order.SqlOrderByOption
import easysql.ast.{SqlDataType, SqlNumberType}
import easysql.query.select.*

import java.util.Date
import scala.annotation.targetName

sealed trait Expr[T <: SqlDataType] {
    @targetName("eq")
    def ===(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Eq, LiteralExpr(value))

    @targetName("eq")
    def ===(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.Eq, LiteralExpr(v))
        case None => BinaryExpr(this, SqlBinaryOperator.Is, NullExpr)
    }

    @targetName("eq")
    def ===(expr: Expr[T]): BinaryExpr[Boolean] =
        BinaryExpr(this, SqlBinaryOperator.Eq, expr)

    @targetName("eq")
    def ===(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] =
        BinaryExpr(this, SqlBinaryOperator.Eq, SubQueryExpr(q))
    
    @targetName("ne")
    def <>(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ne, LiteralExpr(value))

    @targetName("ne")
    def <>(value: Option[T]): BinaryExpr[Boolean] = value match {
        case Some(v) => BinaryExpr(this, SqlBinaryOperator.Ne, LiteralExpr(v))
        case None => BinaryExpr(this, SqlBinaryOperator.IsNot, NullExpr)
    }

    @targetName("ne")
    def <>(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ne, expr)

    @targetName("ne")
    def <>(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ne, SubQueryExpr(q))

    @targetName("gt")
    def >(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Gt, LiteralExpr(value))

    @targetName("gt")
    def >(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Gt, expr)

    @targetName("gt")
    def >(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Gt, SubQueryExpr(q))

    @targetName("ge")
    def >=(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ge, LiteralExpr(value))

    @targetName("ge")
    def >=(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ge, expr)

    @targetName("ge")
    def >=(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Ge, SubQueryExpr(q))

    @targetName("lt")
    def <(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Lt, LiteralExpr(value))

    @targetName("lt")
    def <(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Lt, expr)

    @targetName("lt")
    def <(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Lt, SubQueryExpr(q))

    @targetName("le")
    def <=(value: T): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Le, LiteralExpr(value))

    @targetName("le")
    def <=(expr: Expr[T]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Le, expr)

    @targetName("le")
    def <=(q: Query[Tuple1[T], _]): BinaryExpr[Boolean] = 
        BinaryExpr(this, SqlBinaryOperator.Le, SubQueryExpr(q))

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
        OrderBy(this, SqlOrderByOption.Asc)

    def desc: OrderBy = 
        OrderBy(this, SqlOrderByOption.Desc)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[T, name.type] =
        AliasExpr(this, name)

    def unsafeAs(name: String): AliasExpr[T, name.type] =
        AliasExpr(this, name)
}

case class LiteralExpr[T <: SqlDataType](value: T) extends Expr[T]

case object NullExpr extends Expr[Nothing]

case class BinaryExpr[T <: SqlDataType](left: Expr[_], op: SqlBinaryOperator, right: Expr[_]) extends Expr[T]

extension (x: BinaryExpr[Boolean]) {
    infix def thenIs[R <: SqlDataType](value: R): CaseBranch[R] = 
        CaseBranch(x, LiteralExpr(value))

    infix def thenIs[R <: SqlDataType](value: Expr[R]): CaseBranch[R] = 
        CaseBranch(x, value)
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
        @targetName("eq")
        def ===(expr: Expr[T]): BinaryExpr[Boolean] = 
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

        @targetName("ne")
        def <>(expr: Expr[T]): BinaryExpr[Boolean] =
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

        @targetName("gt")
        def >(expr: Expr[T]): BinaryExpr[Boolean] =
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

        @targetName("ge")
        def >=(expr: Expr[T]): BinaryExpr[Boolean] =
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

        @targetName("lt")
        def <(expr: Expr[T]): BinaryExpr[Boolean] =
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

        @targetName("le")
        def <=(expr: Expr[T]): BinaryExpr[Boolean] =
            BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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
            @targetName("plus")
            def +(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Add, LiteralExpr(value))

            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Sub, LiteralExpr(value))

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Mul, LiteralExpr(value))

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Div, LiteralExpr(value))

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(value: R): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Mod, LiteralExpr(value))

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(e, SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Eq, LiteralExpr(value))

            @targetName("eq")
            def ===(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.Eq, LiteralExpr(v))
                case None => BinaryExpr(e, SqlBinaryOperator.Is, NullExpr)
            }

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Eq, expr)

            @targetName("eq")
            def ===(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Eq, SubQueryExpr(q))

            @targetName("ne")
            def <>(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ne, LiteralExpr(value))

            @targetName("ne")
            def <>(value: Option[R]): BinaryExpr[Boolean] = value match {
                case Some(v) => BinaryExpr(e, SqlBinaryOperator.Ne, LiteralExpr(v))
                case None => BinaryExpr(e, SqlBinaryOperator.IsNot, NullExpr)
            }

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ne, expr)

            @targetName("ne")
            def <>(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ne, SubQueryExpr(q))

            @targetName("gt")
            def >(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Gt, LiteralExpr(value))

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Gt, expr)

            @targetName("gt")
            def >(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Gt, SubQueryExpr(q))

            @targetName("ge")
            def >=(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ge, LiteralExpr(value))

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ge, expr)

            @targetName("ge")
            def >=(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Ge, SubQueryExpr(q))

            @targetName("lt")
            def <(value: R): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Lt, LiteralExpr(value))

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Lt, expr)

            @targetName("lt")
            def <(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] = 
                BinaryExpr(e, SqlBinaryOperator.Lt, SubQueryExpr(q))

            @targetName("le")
            def <=(value: R): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Le, LiteralExpr(value))

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Le, expr)

            @targetName("le")
            def <=(q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Le, SubQueryExpr(q))

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
            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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

            def unsafeAs(name: String): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: Long) {
            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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

            def unsafeAs(name: String): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
            
            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: Float) {
            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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

            def unsafeAs(name: String): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v.toDouble), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v.toDouble), name)
        }

        extension [R <: SqlNumberType] (v: Double) {
            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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

            def unsafeAs(name: String): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }

        extension [R <: SqlNumberType] (v: BigDecimal) {
            @targetName("plus")
            def +(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Add, expr)

            @targetName("minus")
            def -(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Sub, expr)

            @targetName("times")
            def *(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mul, expr)

            @targetName("div")
            def /(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Div, expr)

            @targetName("mod")
            def %(expr: Expr[R]): BinaryExpr[BigDecimal] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Mod, expr)

            @targetName("eq")
            def ===(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Eq, expr)

            @targetName("ne")
            def <>(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ne, expr)

            @targetName("gt")
            def >(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Gt, expr)

            @targetName("ge")
            def >=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Ge, expr)

            @targetName("lt")
            def <(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Lt, expr)

            @targetName("le")
            def <=(expr: Expr[R]): BinaryExpr[Boolean] = 
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Le, expr)

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

            def unsafeAs(name: String): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
            
            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[BigDecimal, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }
    }

    given stringOperator: ExprOperator[String] with {
        extension (e: Expr[String]) {
            def like(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Like, LiteralExpr(value))

            def like(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Like, expr)

            def notLike(value: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NotLike, LiteralExpr(value))

            def notLike(expr: Expr[String]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.NotLike, expr)

            @targetName("json")
            def ->(json: Int): BinaryExpr[String] =
                BinaryExpr(e, SqlBinaryOperator.Json, LiteralExpr(json))

            @targetName("json")
            def ->(json: String): BinaryExpr[String] =
                BinaryExpr(e, SqlBinaryOperator.Json, LiteralExpr(json))

            @targetName("jsonText")
            def ->>(json: Int): BinaryExpr[String] =
                BinaryExpr(e, SqlBinaryOperator.JsonText, LiteralExpr(json))

            @targetName("jsonText")
            def ->>(json: String): BinaryExpr[String] =
                BinaryExpr(e, SqlBinaryOperator.JsonText, LiteralExpr(json))
        }

        extension (v: String) {
            def unsafeAs(name: String): AliasExpr[String, name.type] =
                AliasExpr(LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[String, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }
    }

    given boolOperator: ExprOperator[Boolean] with {
        extension (e: Expr[Boolean]) {
            @targetName("and")
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.And, query)

            @targetName("and")
            def &&(v: Boolean): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.And, LiteralExpr(v))

            @targetName("or")
            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Or, query)

            @targetName("or")
            def ||(v: Boolean): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Or, LiteralExpr(v))

            @targetName("xor")
            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Xor, query)

            @targetName("xor")
            def ^(v: Boolean): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Xor, LiteralExpr(v))

            @targetName("not")
            def unary_! : FuncExpr[Boolean] =
                FuncExpr("NOT", List(e))
        }

        extension (v: Boolean) {
            @targetName("and")
            def &&(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.And, query)

            @targetName("or")
            def ||(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Or, query)

            @targetName("xor")
            def ^(query: Expr[Boolean]): BinaryExpr[Boolean] =
                BinaryExpr(LiteralExpr(v), SqlBinaryOperator.Xor, query)

            def unsafeAs(name: String): AliasExpr[Boolean, name.type] =
                AliasExpr(LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[Boolean, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }
    }

    given dateOperator: ExprOperator[Date] with {
        extension (e: Expr[Date]) {
            @targetName("eq")
            def ===(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Eq, LiteralExpr(s))

            @targetName("ne")
            def <>(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Ne, LiteralExpr(s))

            @targetName("gt")
            def >(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Gt, LiteralExpr(s))

            @targetName("ge")
            def >=(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Ge, LiteralExpr(s))

            @targetName("lt")
            def <(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Lt, LiteralExpr(s))

            @targetName("le")
            def <=(s: String): BinaryExpr[Boolean] =
                BinaryExpr(e, SqlBinaryOperator.Le, LiteralExpr(s))

            def between(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), false)

            def notBetween(start: String, end: String): Expr[Boolean] =
                BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), true)
        }

        extension (v: Date) {
            def unsafeAs(name: String): AliasExpr[Date, name.type] =
                AliasExpr(LiteralExpr(v), name)

            infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasExpr[Date, name.type] =
                AliasExpr(LiteralExpr(v), name)
        }
    }
}

given unsafeOperator: ExprOperator[_] with {
    extension (e: Expr[_]) {
        @targetName("plus")
        def +(value: SqlNumberType): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Add, LiteralExpr(value))

        @targetName("plus")
        def +(expr: Expr[_]): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Add, expr)

        @targetName("minus")
        def -(value: SqlNumberType): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Sub, LiteralExpr(value))

        @targetName("minus")
        def -(expr: Expr[_]): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Sub, expr)

        @targetName("times")
        def *(value: SqlNumberType): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Mul, LiteralExpr(value))

        @targetName("times")
        def *(expr: Expr[_]): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Mul, expr)

        @targetName("div")
        def /(value: SqlNumberType): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Div, LiteralExpr(value))

        @targetName("div")
        def /(expr: Expr[_]): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Div, expr)

        @targetName("mod")
        def %(value: SqlNumberType): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Mod, LiteralExpr(value))

        @targetName("mod")
        def %(expr: Expr[_]): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Mod, expr)

        @targetName("eq")
        def ===(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Eq, LiteralExpr(value))

        @targetName("eq")
        def ===(value: Option[SqlDataType]): BinaryExpr[Boolean] = value match {
            case Some(v) => BinaryExpr(e, SqlBinaryOperator.Eq, LiteralExpr(v))
            case None => BinaryExpr(e, SqlBinaryOperator.Is, NullExpr)
        }

        @targetName("eq")
        def ===(expr: Expr[_]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Eq, expr)

        @targetName("eq")
        def ===[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Eq, SubQueryExpr(q))

        @targetName("ne")
        def <>(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ne, LiteralExpr(value))

        @targetName("ne")
        def <>(value: Option[SqlDataType]): BinaryExpr[Boolean] = value match {
            case Some(v) => BinaryExpr(e, SqlBinaryOperator.Ne, LiteralExpr(v))
            case None => BinaryExpr(e, SqlBinaryOperator.IsNot, NullExpr)
        }

        @targetName("ne")
        def <>(expr: Expr[_]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ne, expr)

        @targetName("ne")
        def <>[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ne, SubQueryExpr(q))

        @targetName("gt")
        def >(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Gt, LiteralExpr(value))

        @targetName("gt")
        def >(expr: Expr[SqlDataType]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Gt, expr)

        @targetName("gt")
        def >[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Gt, SubQueryExpr(q))

        @targetName("ge")
        def >=(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ge, LiteralExpr(value))

        @targetName("ge")
        def >=(expr: Expr[SqlDataType]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ge, expr)

        @targetName("ge")
        def >=[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Ge, SubQueryExpr(q))

        @targetName("lt")
        def <(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Lt, LiteralExpr(value))

        @targetName("lt")
        def <(expr: Expr[SqlDataType]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Lt, expr)

        @targetName("lt")
        def <[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Lt, SubQueryExpr(q))

        @targetName("le")
        def <=(value: SqlDataType): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Le, LiteralExpr(value))

        @targetName("le")
        def <=(expr: Expr[SqlDataType]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Le, expr)

        @targetName("le")
        def <=[R <: SqlDataType](q: Query[Tuple1[R], _]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Le, SubQueryExpr(q))

        def in(list: List[SqlDataType | Expr[_]]): Expr[Boolean] =
            if list.isEmpty then LiteralExpr(false) else {
                val inList = list map {
                    case expr: Expr[_] => expr
                    case t: SqlDataType => LiteralExpr(t)
                }
                InExpr(e, ListExpr(inList), false)
            }

        def in(list: (SqlDataType | Expr[_])*): Expr[Boolean] =
            in(list.toList)

        def notIn(list: List[SqlDataType | Expr[_]]): Expr[Boolean] =
            if list.isEmpty then LiteralExpr(true) else {
                val inList = list map {
                    case expr: Expr[_] => expr
                    case t: SqlDataType => LiteralExpr(t)
                }
                InExpr(e, ListExpr(inList), true)
            }

        def notIn(list: (SqlDataType | Expr[_])*): Expr[Boolean] =
            notIn(list.toList)

        def in[R <: SqlDataType](subQuery: Query[Tuple1[R], _]): Expr[Boolean] = 
            InExpr(e, SubQueryExpr(subQuery), false)

        def notIn[R <: SqlDataType](subQuery: Query[Tuple1[R], _]): Expr[Boolean] = 
            InExpr(e, SubQueryExpr(subQuery), true)

        def between(start: SqlDataType, end: SqlDataType): Expr[Boolean] =
            BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), false)

        def between(start: Expr[_], end: Expr[_]): Expr[Boolean] =
            BetweenExpr(e, start, end, false)

        def notBetween(start: SqlDataType, end: SqlDataType): Expr[Boolean] =
            BetweenExpr(e, LiteralExpr(start), LiteralExpr(end), true)

        def notBetween(start: Expr[_], end: Expr[_]): Expr[Boolean] =
            BetweenExpr(e, start, end, true)

        def like(value: String): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Like, LiteralExpr(value))

        def like(expr: Expr[_]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.Like, expr)

        def notLike(value: String): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.NotLike, LiteralExpr(value))

        def notLike(expr: Expr[_]): BinaryExpr[Boolean] =
            BinaryExpr(e, SqlBinaryOperator.NotLike, expr)

        @targetName("json")
        def ->(json: Int): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Json, LiteralExpr(json))

        @targetName("json")
        def ->(json: String): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.Json, LiteralExpr(json))

        @targetName("jsonText")
        def ->>(json: Int): BinaryExpr[Nothing] =
            BinaryExpr(e, SqlBinaryOperator.JsonText, LiteralExpr(json))

        @targetName("jsonText")
        def ->>(json: String): BinaryExpr[Nothing] = 
            BinaryExpr(e, SqlBinaryOperator.JsonText, LiteralExpr(json))
    }
}

case class AliasExpr[T <: SqlDataType, Alias <: String](expr: Expr[T], name: Alias)

case class CaseBranch[T](expr: Expr[_], thenValue: Expr[_])

case class OrderBy(expr: Expr[_], order: SqlOrderByOption)