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

trait Query[T <: Tuple, A <: Tuple](
    private[easysql] val ast: SqlQuery,
    private[select] val selectItems: Map[String, String]
) {
    private def unionClause[U <: Tuple](right: Query[U, _], unionType: SqlUnionType): Union[UnionType[T, U], A] =
        Union(this.selectItems, this, unionType, right)

    private def unionClause[U <: Tuple](right: U, unionType: SqlUnionType): Union[UnionType[T, U], A] =
        Union(this.selectItems, this, unionType, Values(right))

    private def unionClause[U <: Tuple](right: List[U], unionType: SqlUnionType): Union[UnionType[T, U], A] =
        Union(this.selectItems, this, unionType, Values(right))

    infix def union[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION)

    infix def union[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION)

    infix def union[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION)

    infix def unionAll[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION_ALL)

    infix def unionAll[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION_ALL)

    infix def unionAll[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.UNION_ALL)

    infix def except[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT)

    infix def except[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT)

    infix def except[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT)

    infix def exceptAll[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT_ALL)

    infix def exceptAll[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT_ALL)

    infix def exceptAll[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.EXCEPT_ALL)

    infix def intersect[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT)

    infix def intersect[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT)

    infix def intersect[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT)

    infix def intersectAll[U <: Tuple](query: Query[U, _]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT_ALL)

    infix def intersectAll[U <: Tuple](query: U): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT_ALL)

    infix def intersectAll[U <: Tuple](query: List[U]): Union[UnionType[T, U], A] =
        unionClause(query, SqlUnionType.INTERSECT_ALL)
}

object Query {
    extension [T <: SqlDataType] (query: Query[Tuple1[T], _]) {
        def toExpr: SubQueryExpr[T] = SubQueryExpr(query)
    }

    given queryToSql[T <: Tuple, A <: Tuple]: ToSql[Query[T, A]] with {
        extension (q: Query[T, A]) def sql(db: DB) = queryToString(q.ast, db)
    }
}

class AliasQuery[T <: Tuple, A <: Tuple](
    private[select] val __query: Query[T, A], 
    private[select] val __queryName: String
) extends Dynamic {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N): ColumnExpr[_, _] = {
        inline erasedValue[A] match {
            case _: EmptyTuple => error("value " + name + " is not a member of this query")
            case _ => {
                inline erasedValue[FindTypeByName[Zip[T, A], Size[T] - 1, N]] match {
                    case _: Nothing => error("value " + name + " is not a member of this query")
                    case _ => {
                        val item = __query.selectItems(name)
                        ColumnExpr[FindTypeByName[Zip[T, A], Size[T] - 1, N] & SqlDataType, N](__queryName, item, name)
                    }
                }
            }
        }
    }
}