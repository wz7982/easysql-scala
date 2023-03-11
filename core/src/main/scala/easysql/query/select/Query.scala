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

trait Query[T <: Tuple, A <: Tuple] {
    def getAst: SqlQuery

    def getSelectItems: Map[String, String]

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: Query[RT, _]): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, right.getAst)

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: RT): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, Values(right).ast)

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: List[RT]): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, Values(right).ast)

    infix def union[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION, right)

    infix def union[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION, right)

    infix def union[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION, right)

    infix def unionAll[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION_ALL, right)

    infix def unionAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION_ALL, right)

    infix def unionAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UNION_ALL, right)

    infix def except[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT, right)

    infix def except[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT, right)

    infix def except[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT, right)

    infix def exceptAll[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT_ALL, right)

    infix def exceptAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT_ALL, right)

    infix def exceptAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.EXCEPT_ALL, right)

    infix def intersect[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT, right)

    infix def intersect[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT, right)

    infix def intersect[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT, right)

    infix def intersectAll[RT <: Tuple](right: Query[RT, _]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT_ALL, right)

    infix def intersectAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT_ALL, right)

    infix def intersectAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.INTERSECT_ALL, right)

    def unsafeAs(name: String): AliasQuery[T, A] =
        AliasQuery(getSelectItems, getAst, name)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasQuery[T, A] =
        AliasQuery(getSelectItems, getAst, name)
}

object Query {
    extension [T <: SqlDataType] (s: Query[Tuple1[T], _]) {
        def toExpr: SubQueryExpr[T] =
            SubQueryExpr(s)
    }

    given queryToSql: ToSql[Query[_, _]] with {
        extension (x: Query[_, _]) def sql(db: DB): String =
            queryToString(x.getAst, db)
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