package org.easysql.query.update

import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.expr.{SqlIdentifierExpr, SqlPropertyExpr}
import org.easysql.ast.statement.update.SqlUpdate
import org.easysql.database.{DB, TableEntity}
import org.easysql.dsl.*
import org.easysql.query.ReviseQuery
import org.easysql.query.select.SelectQuery
import org.easysql.util.toSqlString
import org.easysql.util.anyToExpr
import org.easysql.visitor.{getExpr, visitExpr}
import org.easysql.macros.updateMacro

import java.sql.{Connection, SQLException}
import java.util.Date

class Update extends ReviseQuery {
    private val sqlUpdate = SqlUpdate()

    infix def update(table: TableSchema | String): Update = {
        val tableName = table match {
            case i: TableSchema => i.tableName
            case s: String => s
        }
        this.sqlUpdate.table = Some(SqlIdentifierExpr(tableName))
        this
    }

    inline infix def update[T <: TableEntity[_]](entity: T, skipNull: Boolean = true): Update = {
        updateMacro(this, entity, skipNull)

        this
    }

    def set[T <: SqlSingleConstType | Null](items: (TableColumnExpr[_, _] | ColumnExpr[_], T | Expr[_, _] | SelectQuery[_])*): Update = {
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

    infix def where(condition: Expr[_, _]): Update = {
        sqlUpdate.addCondition(getExpr(condition))
        this
    }

    def where(test: () => Boolean, condition: Expr[_, _]): Update = {
        if (test()) {
            sqlUpdate.addCondition(getExpr(condition))
        }
        this
    }

    def where(test: Boolean, condition: Expr[_, _]): Update = {
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