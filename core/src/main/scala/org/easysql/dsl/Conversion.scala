package org.easysql.dsl

import org.easysql.ast.{SqlDataType, SqlNumberType}
import org.easysql.query.select.{SelectQuery, ValuesSelect, Select}

import scala.compiletime.ops.any.*
import scala.compiletime.ops.int.*
import java.util.Date

type InverseMap[X <: Tuple] <: Tuple = X match {
    case SelectItem[x] *: t => x *: InverseMap[t]
    case EmptyTuple => EmptyTuple
}

type RecursiveInverseMap[X <: Tuple] <: Tuple = X match {
    case x *: t => x match {
        case Tuple => Tuple.Concat[RecursiveInverseMap[x], RecursiveInverseMap[t]]
        case SelectItem[y] => y *: RecursiveInverseMap[t]
    }
    case EmptyTuple => EmptyTuple
}

type ExtractAliasNames[T <: Tuple] <: Tuple = T match {
    case AliasExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case TableColumnExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case PrimaryKeyColumnExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case _ => EmptyTuple
}

type QueryType[T <: Tuple | Expr[_] | TableSchema[_]] <: Tuple = T match {
    case h *: t => h *: t
    case _ => Tuple1[T]
}

type Union[X <: Tuple, Y <: Tuple] <: Tuple = (X, Y) match {
    case (a *: at, b *: bt) => UnionTo[a, b] *: Union[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple
}

type UnionTo[A, B] = A match {
    case B => B
    case SqlNumberType => B match {
        case SqlNumberType => BigDecimal
    }
    case _ => B match {
        case A => A
    }
}

type NonEmpty[T <: String] = T == "" match {
    case false => Any
    case true => Nothing
}

type FindTypeByName[T <: Tuple, I <: Int, Name <: String] = I >= 0 match {
    case true => Tuple.Elem[T, I] match {
        case Tuple2[t, n] => n == Name match {
            case true => t
            case false => FindTypeByName[T, I - 1, Name]
        }
        case _ => Nothing
    }
    case false => Nothing
}

type ElementType[T <: Tuple, N <: Tuple, Name <: String] = (T, N) match {
    case (t *: tt, n *: nt) => n == Name match {
        case true => t match {
            case SqlDataType => t
            case Option[o] => o match {
                case SqlDataType => o
            }
        }
        case false => ElementType[tt, nt, Name]
    }
    case (EmptyTuple, EmptyTuple) => Nothing
}

type ExprType[T <: Tuple] = T match {
    case h *: t => Expr[h] *: ExprType[t]
    case EmptyTuple => EmptyTuple
}

type ResultType[T <: Tuple] = T match {
    case Tuple1[t] => Option[t]
    case _ => MapOption[T]
}

type MapOption[T <: Tuple] = T match {
    case h *: t => Option[h] *: MapOption[t]
    case EmptyTuple => EmptyTuple
}

type UpdateType[T <: SqlDataType] <: SqlDataType = T match {
    case SqlNumberType => SqlNumberType
    case String => String
    case Boolean => Boolean
    case Date => Date | String
}