package org.easysql.dsl

import org.easysql.ast.SqlSingleConstType
import org.easysql.database.TableEntity
import org.easysql.query.select.{SelectQuery, ValuesSelect}

import scala.compiletime.ops.any.*
import java.util.Date

given stringToExpr: Conversion[String, ConstExpr[String]] = ConstExpr[String](_)

given intToExpr: Conversion[Int, ConstExpr[Int]] = ConstExpr[Int](_)

given longToExpr: Conversion[Long, ConstExpr[Long]] = ConstExpr[Long](_)

given doubleToExpr: Conversion[Double, ConstExpr[Double]] = ConstExpr[Double](_)

given floatToExpr: Conversion[Float, ConstExpr[Float]] = ConstExpr[Float](_)

given boolToExpr: Conversion[Boolean, ConstExpr[Boolean]] = ConstExpr[Boolean](_)

given dateToExpr: Conversion[Date, ConstExpr[Date]] = ConstExpr[Date](_)

given queryToExpr[T <: SqlSingleConstType | Null]: Conversion[SelectQuery[Tuple1[T]], SubQueryExpr[T]] = SubQueryExpr(_)

type InverseMap[X <: Tuple, F[_ <: SqlSingleConstType | Null, _ <: TableSchema | Tuple]] <: Tuple = X match {
    case F[x, _] *: t => x *: InverseMap[t, F]
    case EmptyTuple => EmptyTuple
}

type RecursiveInverseMap[X <: Tuple, F[_ <: SqlSingleConstType | Null, _ <: TableSchema | Tuple]] <: Tuple = X match {
    case x *: t => x match {
        case Tuple => Tuple.Concat[RecursiveInverseMap[x, F], RecursiveInverseMap[t, F]]
        case F[y, _] => y *: RecursiveInverseMap[t, F]
    }
    case EmptyTuple => EmptyTuple
}

type QueryType[T <: Tuple | Expr[_, _] | TableSchema] <: Tuple = T match {
    case h *: t => h *: t
    case _ => Tuple1[T]
}

type MapUnionNull[T <: Tuple] <: Tuple = T match {
    case h *: t => (h | Null) *: MapUnionNull[t]
    case EmptyTuple => EmptyTuple
}

type PK[T <: TableEntity[_]] = T match {
    case TableEntity[t] => t
}

type Union[X <: Tuple, Y <: Tuple] <: Tuple = (X, Y) match {
    case (a *: at, b *: bt) => UnionTo[a, b] *: Union[at, bt]
    case (EmptyTuple, EmptyTuple) => EmptyTuple
}

type UnionTo[A, B] = A match {
    case B => B
    case _ => B match {
        case A => A
    }
}

type MatchTypeLeft[L, R] <: Boolean = L match {
    case R => true
    case R | Null => true
    case _ => false
}

type MatchTypeRight[L, R] <: Boolean = R match {
    case L => true
    case L | Null => true
    case _ => false
}

type NonEmpty[T <: String] = T == "" match {
    case false => Any
    case true => Nothing
}

type TableConcat[A <: TableSchema | Tuple, B <: TableSchema | Tuple] <: Tuple = A match {
    case NothingTable => Tuple1[NothingTable]
    case TableSchema => B match {
        case TableSchema => A *: B *: EmptyTuple
        case Tuple => Tuple.Concat[Tuple1[A], B]
    }
    case Tuple1[NothingTable] => Tuple1[NothingTable]
    case Tuple => B match {
        case NothingTable => Tuple1[NothingTable]
        case TableSchema => Tuple.Concat[A, Tuple1[B]]
        case Tuple1[NothingTable] => Tuple1[NothingTable]
        case Tuple => Tuple.Concat[A, B]
    }
}

type TableContains[From <: TableSchema | Tuple, X <: TableSchema | Tuple] = From match {
    case TableSchema => X match {
        case NothingTable => Any
        case TableSchema => From match {
            case X => Any
            case _ => Nothing
        }
        case Tuple1[t] => t match {
            case From => Any
            case NothingTable => Any
            case _ => Nothing
        }
        case _ => Nothing
    }
    case Tuple => X match {
        case NothingTable => Any
        case TableSchema => TableInTuple[From, X]
        case Tuple1[NothingTable] => Nothing
        case _ => TupleContains[From, X]
    }
}

type TableInTuple[T <: Tuple, Table <: TableSchema] = T match {
    case h *: t => h match {
        case Table => Any
        case _ => TableInTuple[t, Table]
    }
    case EmptyTuple => Nothing
}

type TupleContains[T1 <: Tuple, T2 <: Tuple] = T2 match {
    case h *: t => h match {
        case TableSchema => TableInTuple[T1, h] match {
            case Any => TupleContains[T1, t]
            case Nothing => Nothing
        }
        case _ => Nothing
    }
    case EmptyTuple => Any
}