package org.easysql.query.select

import org.easysql.ast.statement.select.{SqlSelectQuery, SqlUnionType}
import org.easysql.database.DB
import org.easysql.dsl.*
import org.easysql.query.BasedQuery
import org.easysql.ast.SqlDataType

import scala.collection.mutable.Map
import scala.language.dynamics
import scala.compiletime.ops.int.*

trait SelectQuery[T <: Tuple, AliasNames <: Tuple] extends BasedQuery with Dynamic {
    def getSelect: SqlSelectQuery

    private[select] val selectItems: Map[String, String] = Map()

    var aliasName: Option[String] = None

    infix def as(name: String)(using NonEmpty[name.type] =:= Any): SelectQuery[T, AliasNames] = {
        this.aliasName = Some(name)
        this
    }

    infix def unsafeAs(name: String): SelectQuery[T, AliasNames]  = {
        this.aliasName = Some(name)
        this
    }

    private def unionClause[U <: Tuple](select: SelectQuery[U, _], unionType: SqlUnionType): UnionSelect[Union[T, U], AliasNames] = {
        val union = new UnionSelect[Union[T, U], AliasNames](this, unionType, select)
        union.selectItems.clear()
        union.selectItems.addAll(selectItems)
        union
    }

    private def unionClause(tuple: T, unionType: SqlUnionType): UnionSelect[T, AliasNames] = {
        val values = new ValuesSelect[T]
        values.addRow(tuple)
        val union = new UnionSelect[T, AliasNames](this, unionType, values)
        union.selectItems.clear()
        union.selectItems.addAll(selectItems)
        union
    }

    private def unionClause(list: List[T], unionType: SqlUnionType): UnionSelect[T, AliasNames] = {
        val values = new ValuesSelect[T]
        list.foreach(it => values.addRow(it))
        val union = new UnionSelect[T, AliasNames](this, unionType, values)
        union.selectItems.clear()
        union.selectItems.addAll(selectItems)
        union
    }

    infix def union[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.UNION)

    infix def union(tuple: T) = unionClause(tuple, SqlUnionType.UNION)

    infix def union(list: List[T]) = unionClause(list, SqlUnionType.UNION)

    infix def unionAll[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.UNION_ALL)

    infix def unionAll(tuple: T) = unionClause(tuple, SqlUnionType.UNION_ALL)

    infix def unionAll(list: List[T]) = unionClause(list, SqlUnionType.UNION_ALL)

    infix def except[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.EXCEPT)

    infix def except(tuple: T) = unionClause(tuple, SqlUnionType.EXCEPT)

    infix def except(list: List[T]) = unionClause(list, SqlUnionType.EXCEPT)

    infix def exceptAll[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.EXCEPT_ALL)

    infix def exceptAll(tuple: T) = unionClause(tuple, SqlUnionType.EXCEPT_ALL)

    infix def exceptAll(list: List[T]) = unionClause(list, SqlUnionType.EXCEPT_ALL)

    infix def intersect[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.INTERSECT)

    infix def intersect(tuple: T) = unionClause(tuple, SqlUnionType.INTERSECT)

    infix def intersect(list: List[T]) = unionClause(list, SqlUnionType.INTERSECT)

    infix def intersectAll[U <: Tuple](select: SelectQuery[U, _]) = unionClause(select, SqlUnionType.INTERSECT_ALL)

    infix def intersectAll(tuple: T) = unionClause(tuple, SqlUnionType.INTERSECT_ALL)

    infix def intersectAll(list: List[T]) = unionClause(list, SqlUnionType.INTERSECT_ALL)

    transparent inline def selectDynamic(inline name: String) = {
        val item = selectItems(name)

        TableColumnExpr[FindTypeByName[Tuple.Zip[T, AliasNames], Tuple.Size[T] - 1, name.type] & SqlDataType, name.type](aliasName.get, item, name, table(aliasName.get))
    }
}

object SelectQuery {
    extension [T <: SqlDataType] (x: SelectQuery[Tuple1[T], _]) {
        def toExpr = SubQueryExpr(x)
    }
}