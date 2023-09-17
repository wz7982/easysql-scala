package easysql.query.select

import easysql.ast.statement.SqlQuery
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

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, right.getAst)

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: RT): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, Values(right).ast)

    def unionClause[RT <: Tuple](unionType: SqlUnionType, right: List[RT]): Union[UnionType[T, RT], A] =
        Union(getSelectItems, getAst, unionType, Values(right).ast)

    infix def union[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Union, right)

    infix def union[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Union, right)

    infix def union[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Union, right)

    infix def unionAll[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UnionAll, right)

    infix def unionAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UnionAll, right)

    infix def unionAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.UnionAll, right)

    infix def except[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Except, right)

    infix def except[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Except, right)

    infix def except[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Except, right)

    infix def exceptAll[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.ExceptAll, right)

    infix def exceptAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.ExceptAll, right)

    infix def exceptAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.ExceptAll, right)

    infix def intersect[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Intersect, right)

    infix def intersect[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Intersect, right)

    infix def intersect[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.Intersect, right)

    infix def intersectAll[RT <: Tuple](right: Query[RT, ?]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.IntersectAll, right)

    infix def intersectAll[RT <: Tuple](right: RT): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.IntersectAll, right)

    infix def intersectAll[RT <: Tuple](right: List[RT]): Union[UnionType[T, RT], A] =
        unionClause(SqlUnionType.IntersectAll, right)

    def unsafeAs(name: String): AliasQuery[T, A] =
        AliasQuery(getSelectItems, getAst, name)

    infix def as(name: String)(using NonEmpty[name.type] =:= true): AliasQuery[T, A] =
        AliasQuery(getSelectItems, getAst, name)
}

object Query {
    extension [T <: SqlDataType] (s: Query[Tuple1[T], ?]) {
        def toExpr: SubQueryExpr[T] =
            SubQueryExpr(s)
    }

    given queryToSql: ToSql[Query[?, ?]] with {
        extension (x: Query[?, ?]) {
            def sql(db: DB): String =
                queryToString(x.getAst, db, false)._1

            def preparedSql(db: DB): (String, Array[Any]) =
                queryToString(x.getAst, db, true)
        }
    }
}

class AliasQuery[T <: Tuple, A <: Tuple](
    private[select] val __selectItems: Map[String, String],
    private[select] val __ast: SqlQuery,
    private[select] val __queryName: String
) extends Dynamic {
    transparent inline def selectDynamic[N <: String & Singleton](inline name: N): ColumnExpr[?, ?] = {
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