package easysql.query.select

import easysql.ast.statement.SqlQuery
import easysql.ast.table.SqlTable
import easysql.ast.statement.SqlUnionType
import easysql.ast.SqlDataType
import easysql.dsl.*
import easysql.database.DB
import easysql.util.queryToString
import easysql.query.ToSql

import scala.language.dynamics
import scala.compiletime.ops.int.*
import scala.compiletime.*
import scala.Tuple.{Zip, Size}

trait Query[T <: Tuple, A <: Tuple, Q[_ <: Tuple, _ <: Tuple]] {
    def ast(query: Q[T, A]): SqlQuery
    
    def selectItems(query: Q[T, A]): Map[String, String]

    def unionClause[RT <: Tuple, RA <: Tuple, RQ[_, _]](left: Q[T, A], unionType: SqlUnionType, right: RQ[RT, RA])(using rq: Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
        Union(selectItems(left), ast(left), SqlUnionType.UNION, rq.ast(right))

    def unionClause[RT <: Tuple](left: Q[T, A], unionType: SqlUnionType, right: RT): Union[UnionType[T, RT], A] =
        Union(selectItems(left), ast(left), unionType, Values(right).ast)

    def unionClause[RT <: Tuple](left: Q[T, A], unionType: SqlUnionType, right: List[RT]): Union[UnionType[T, RT], A] =
        Union(selectItems(left), ast(left), unionType, Values(right).ast)

    extension (query: Q[T, A]) {
        infix def union[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION, right)

        infix def union[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION, right)

        infix def union[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION, right)

        infix def unionAll[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION_ALL, right)

        infix def unionAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION_ALL, right)

        infix def unionAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.UNION_ALL, right)

        infix def except[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT, right)

        infix def except[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT, right)

        infix def except[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT, right)

        infix def exceptAll[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT_ALL, right)

        infix def exceptAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT_ALL, right)

        infix def exceptAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.EXCEPT_ALL, right)

        infix def intersect[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT, right)

        infix def intersect[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT, right)

        infix def intersect[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT, right)

        infix def intersectAll[RT <: Tuple, RA <: Tuple, RQ[_, _]](right: RQ[RT, RA])(using Query[RT, RA, RQ]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT_ALL, right)

        infix def intersectAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT_ALL, right)

        infix def intersectAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
            unionClause(query, SqlUnionType.INTERSECT_ALL, right)
    }
}

class AliasQuery[T <: Tuple, A <: Tuple](
    private[select] val __selectItems: Map[String, String],
    private[select] val __ast: SqlQuery,
    private[select] val __queryName: String
) extends Dynamic {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N): ColumnExpr[_, _] = {
        inline erasedValue[A] match {
            case _: EmptyTuple => error("value " + name + " is not a member of this query")
            case _ => {
                inline erasedValue[FindTypeByName[Zip[T, A], Size[T] - 1, N]] match {
                    case _: Nothing => error("value " + name + " is not a member of this query")
                    case _ => {
                        val item = __selectItems(name)
                        ColumnExpr[FindTypeByName[Zip[T, A], Size[T] - 1, N] & SqlDataType, N](__queryName, item, name)
                    }
                }
            }
        }
    }
}