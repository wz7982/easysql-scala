package easysql.dsl

import scala.language.dynamics
import scala.deriving.*

trait JPA[E <: Product] extends Dynamic {
    transparent inline def applyDynamic[N <: String & Singleton](
        inline name: N
    )(using m: Mirror.ProductOf[E])(
        args: JPAArgsType[m.MirroredElemTypes, m.MirroredElemLabels, Split[N]]
    ) = {
        ???
    }
}

object JPA {
    given singletonToTuple1[T]: Conversion[T, Tuple1[T]] = 
        Tuple1(_)
}