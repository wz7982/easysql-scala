package org.easysql.query.select

import org.easysql.ast.expr.SqlIdentifierExpr
import org.easysql.ast.statement.select.{SqlSelectQuery, SqlWithItem, SqlWithSelect}
import org.easysql.database.DB
import org.easysql.visitor.getExpr
import org.easysql.dsl.col
import org.easysql.util.toSqlString

import java.sql.Connection

class WithSelect extends SelectQueryImpl[Nothing] {
    private val sqlWithSelect = SqlWithSelect()

    def recursive: WithSelect = {
        this.sqlWithSelect.recursive = true
        this
    }

    def add(name: String, columns: List[String], query: SelectQuery[_]): WithSelect = {
        val withItem = SqlWithItem(
            getExpr(col(name)),
            query.getSelect,
            columns.map(it => SqlIdentifierExpr(it))
        )

        this.sqlWithSelect.withList.addOne(withItem)
        this
    }

    def select(query: Select[_, _] => SelectQuery[_]): WithSelect = {
        sqlWithSelect.query = Some(query(Select()).getSelect)
        this
    }

    override def sql(db: DB): String = toSqlString(sqlWithSelect, db)

    override def toSql(using db: DB): String = toSqlString(sqlWithSelect, db)

    override def getSelect: SqlSelectQuery = this.sqlWithSelect
}

object WithSelect {
    def apply(): WithSelect = new WithSelect()
}