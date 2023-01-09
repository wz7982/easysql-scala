package org.easysql.query.update

import org.easysql.ast.SqlDataType
import org.easysql.ast.expr.{SqlIdentExpr, SqlPropertyExpr}
import org.easysql.ast.statement.update.SqlUpdate
import org.easysql.ast.table.SqlIdentTable
import org.easysql.database.DB
import org.easysql.dsl.*
import org.easysql.query.ReviseQuery
import org.easysql.query.select.SelectQuery
import org.easysql.util.toSqlString
import org.easysql.util.anyToExpr
import org.easysql.visitor.{getExpr, visitExpr}
import org.easysql.macros.*

import java.sql.{Connection, SQLException}
import java.util.Date

class Update extends ReviseQuery {
    private val sqlUpdate = SqlUpdate()

    def update(table: TableSchema[_]): Update = {
        this.sqlUpdate.table = Some(SqlIdentTable(table._tableName))
        this
    }

    inline def update[T <: Product](entity: T, skipNull: Boolean = true): Update = {
        val (tableName, pkList, updateList) = updateMacro[T]

        sqlUpdate.table = Some(SqlIdentTable(tableName))
        updateList.foreach { u =>
            val value = u._2.apply(entity)
            if (!skipNull || value != null && value != None) {
                val updatePair = getExpr(ColumnExpr(u._1)) -> getExpr(anyToExpr(value))
                sqlUpdate.setList.addOne(updatePair)
            }
        }

        if (sqlUpdate.setList.size == 0) {
            throw Exception("no fields need to be updated in the entity class")
        }

        pkList.foreach { pk =>
            sqlUpdate.addCondition(visitExpr(ColumnExpr(pk._1).equal(pk._2.apply(entity))))
        }

        this
    }

    def set[T <: SqlDataType](items: (TableColumnExpr[_, _] | ColumnExpr[_], T | Expr[_] | SelectQuery[_, _])*): Update = {
        items.foreach { item =>
            val (column, value) = item

            val columnExpr = column match {
                case t: TableColumnExpr[_, _] => visitExpr(ColumnExpr(t.column))
                case c: ColumnExpr[_] => visitExpr(c)
            }

            val valueExpr = getExpr(value)
            sqlUpdate.setList.addOne(columnExpr -> valueExpr)
        }
        
        this
    }

    def where(condition: Expr[_]): Update = {
        sqlUpdate.addCondition(getExpr(condition))
        this
    }

    def where(test: () => Boolean, condition: Expr[_]): Update = {
        if (test()) {
            sqlUpdate.addCondition(getExpr(condition))
        }
        this
    }

    def where(test: Boolean, condition: Expr[_]): Update = {
        if (test) {
            sqlUpdate.addCondition(getExpr(condition))
        }
        this
    }

    override def sql(db: DB): String = toSqlString(sqlUpdate, db)

    override def toSql(using db: DB): String = toSqlString(sqlUpdate, db)
}

object Update {
    def apply(): Update = new Update()
}