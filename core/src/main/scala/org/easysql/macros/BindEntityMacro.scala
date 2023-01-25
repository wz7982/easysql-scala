package org.easysql.macros

import org.easysql.ast.SqlDataType
import org.easysql.dsl.*

import scala.compiletime.*

inline def bindEntityMacro[T](inline nextIndex: Int): (Int, Array[Any] => T) = ${ bindEntityMacroImpl[T]('nextIndex) }

inline def bindOptionMacro[T](inline nextIndex: Int): (Int, Array[Any] => Option[T]) = ${ bindOptionMacroImpl[T]('nextIndex) }

inline def bindSingleton[T](nextIndex: Int): (Int, Array[Any] => Any) = {
    inline erasedValue[T] match {
        case _: Option[t] => {
            inline erasedValue[t] match {
                case _: Product => bindOptionMacro[t](nextIndex)
                case _: BigDecimal => nextIndex + 1 -> { (data: Array[Any]) => if data(nextIndex) == null then None else Some(BigDecimal(data(nextIndex).toString())) }
                case _ => nextIndex + 1 -> { (data: Array[Any]) => if data(nextIndex) == null then None else Some(data(nextIndex)).asInstanceOf[Option[t]] }
            }
        }
        case _: Product => bindEntityMacro[T](nextIndex)
        case _: BigDecimal => nextIndex + 1 -> { (data: Array[Any]) => BigDecimal(data(nextIndex).toString()) }
        case _ => nextIndex + 1 -> { (data: Array[Any]) => data(nextIndex).asInstanceOf[T] }
    }
}

inline def bindSelect[T](index: Int, data: Array[Any]): T = {
    val bindData = inline erasedValue[T] match {
        case _: *:[x, t] => {
            val singleton = bindSingleton[x](index)
            singleton._2.apply(data) *: bindSelect[t](singleton._1, data)
        }
        case _: EmptyTuple => EmptyTuple
        case _ => bindSingleton[T](0)._2.apply(data)
    }
    bindData.asInstanceOf[T]
}