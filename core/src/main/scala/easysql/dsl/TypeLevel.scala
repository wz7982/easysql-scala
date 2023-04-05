package easysql.dsl

import easysql.ast.{SqlDataType, SqlNumberType}

import java.util.Date
import scala.Tuple.Concat
import scala.compiletime.ops.any.*
import scala.compiletime.ops.boolean.*
import scala.compiletime.ops.int.*
import scala.compiletime.ops.string.{CharAt, Length, Substring}

type InverseMap[X <: Tuple] <: Tuple = X match {
    case Expr[x] *: t => x *: InverseMap[t]
    case AliasExpr[x, _] *: t => x *: InverseMap[t]
    case TableSchema[x] *: t => x *: InverseMap[t]
    case EmptyTuple => EmptyTuple
}

type HasAliasName[T <: Tuple] <: Boolean = T match {
    case AliasExpr[_, n] *: t => true && HasAliasName[t]
    case ColumnExpr[_, n] *: t => true && HasAliasName[t]
    case PrimaryKeyExpr[_, n] *: t => true && HasAliasName[t]
    case EmptyTuple => true
    case _ => false
}

type ExtractAliasNames[T <: Tuple] <: Tuple = T match {
    case AliasExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case ColumnExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case PrimaryKeyExpr[_, n] *: t => n *: ExtractAliasNames[t]
    case _ => EmptyTuple
}

type AliasNames[T <: Tuple] <: Tuple = HasAliasName[T] match {
    case true => ExtractAliasNames[T]
    case false => EmptyTuple
}

type UnionType[X <: Tuple, Y <: Tuple] <: Tuple = (X, Y) match {
    case (a *: at, b *: bt) => UnionTo[a, b] *: UnionType[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple
}

type UnionTo[A, B] = A match {
    case B => B
    case SqlNumberType => B match {
        case SqlNumberType => BigDecimal
    }
}

type NonEmpty[T <: String] = T != ""

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
            case _ => Nothing
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

type InsertType[T <: Tuple] <: Tuple = T match {
    case h *: t => (h | Option[h]) *: InsertType[t]
    case EmptyTuple => EmptyTuple
}

type MonadicJoin[T, JT] = T match {
    case TableSchema[t] => (T, JT)
    case h *: t => Tuple.Concat[(h *: t), Tuple1[JT]]
}

type Split[S <: String, Begin <: Int, I <: Int] <: Tuple = I < Length[S] match {
    case false => EmptyTuple
    case true => CharAt[S, I] match {
        case '_' => Substring[S, Begin, I] *: Split[S, I + 1, I + 1]
        case _ => Split[S, Begin, I + 1]
    }
}

type SplitUnderline[S <: String] = Split[scala.compiletime.ops.string.+[S, "_"], 0, 0]

type JPAArgsType[ElementTypes <: Tuple, ElementLabels <: Tuple, NS <: Tuple] <: Tuple = NS match {
    case h *: t => h match {
        case ("by" | "and" | "or") => t match {
            case name *: tt => tt match {
                case ("in" | "notIn") *: ttt => 
                    List[ElementType[ElementTypes, ElementLabels, name]] *: JPAArgsType[ElementTypes, ElementLabels, ttt]
                case ("like" | "notLike" | "startingWith" | "endingWith") *: ttt =>
                    String *: JPAArgsType[ElementTypes, ElementLabels, ttt]
                case ("gt" | "ge" | "lt" | "le" | "not") *: ttt =>
                    ElementType[ElementTypes, ElementLabels, name] *: JPAArgsType[ElementTypes, ElementLabels, ttt]
                case ("between" | "notBetween") *: ttt =>
                    ElementType[ElementTypes, ElementLabels, name] *: ElementType[ElementTypes, ElementLabels, name] *: JPAArgsType[ElementTypes, ElementLabels, ttt]
                case ("isNull" | "isNotNull") *: ttt =>
                    JPAArgsType[ElementTypes, ElementLabels, ttt]
                case _ => 
                    ElementType[ElementTypes, ElementLabels, name] *: JPAArgsType[ElementTypes, ElementLabels, tt]
            }      
        }
        case _ => JPAArgsType[ElementTypes, ElementLabels, t]
    }
    case EmptyTuple => 
        EmptyTuple
}