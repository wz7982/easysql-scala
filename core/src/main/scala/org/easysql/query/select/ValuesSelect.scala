package org.easysql.query.select

import org.easysql.ast.statement.select.{SqlSelectQuery, SqlValuesSelect}
import org.easysql.database.DB
import org.easysql.util.toSqlString
import org.easysql.util.anyToExpr
import org.easysql.ast.SqlSingleConstType
import org.easysql.dsl.Expr
import org.easysql.dsl.MapUnionNull
import org.easysql.dsl.InverseMap
import org.easysql.visitor.getExpr

import java.sql.Connection

class ValuesSelect[T <: Tuple] extends SelectQueryImpl[T] {
    private var sqlValuesSelect = SqlValuesSelect()

    def addRow[U <: Tuple](row: U): ValuesSelect[InverseMap[MapUnionNull[U]]] = {
        val valuesSelect = new ValuesSelect[InverseMap[MapUnionNull[U]]]()
        valuesSelect.sqlValuesSelect = sqlValuesSelect
        val addRow = row.toList.map(it => getExpr(anyToExpr(it)))
        valuesSelect.sqlValuesSelect.values.addOne(addRow)
        valuesSelect
    }

    override def sql(db: DB): String = toSqlString(sqlValuesSelect, db)

    override def toSql(using db: DB): String = toSqlString(sqlValuesSelect, db)

    override def getSelect: SqlSelectQuery = this.sqlValuesSelect
}

object ValuesSelect {
    def apply(): ValuesSelect[EmptyTuple] = new ValuesSelect()
}