package easysql.dsl

import easysql.query.select.Select

import scala.language.dynamics
import scala.deriving.*

class JPA[E <: Product](val table: TableSchema[E]) extends Dynamic {
    inline def applyDynamic[N <: String & Singleton](
        inline name: N
    )(using m: Mirror.ProductOf[E])(
        args: JPAArgsType[m.MirroredElemTypes, m.MirroredElemLabels, SplitUnderline[N]]
    ): Select[Tuple1[E], _] = {
        ???
    }
}

object JPA {
    def apply[E <: Product](table: TableSchema[E]): JPA[E] =
        new JPA[E](table)

    given singletonToTuple1[T]: Conversion[T, Tuple1[T]] = 
        Tuple1(_)
}