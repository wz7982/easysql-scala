package org.easysql.query.delete

import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.expr.SqlIdentifierExpr
import org.easysql.ast.statement.delete.SqlDelete
import org.easysql.database.{DB, TableEntity}
import org.easysql.dsl.{Expr, TableColumnExpr, TableSchema, PK}
import org.easysql.query.ReviseQuery
import org.easysql.util.toSqlString
import org.easysql.visitor.getExpr
import org.easysql.macros.deleteMacro

import java.sql.{Connection, SQLException}

class Delete extends ReviseQuery {
    private val sqlDelete = SqlDelete()

    infix def deleteFrom(table: TableSchema | String): Delete = {
        val tableName = table match {
            case i: TableSchema => i.tableName
            case s: String => s
        }
        this.sqlDelete.table = Some(SqlIdentifierExpr(tableName))
        this
    }

    inline infix def delete[T <: TableEntity[_]](pk: PK[T]): Delete = {
        deleteMacro[T](this, pk)
    }

    infix def where(condition: Expr[_, _]): Delete = {
        sqlDelete.addCondition(getExpr(condition))
        this
    }

    def where(test: () => Boolean, condition: Expr[_, _]): Delete = {
        if (test()) {
            sqlDelete.addCondition(getExpr(condition))
        }
        this
    }

    def where(test: Boolean, condition: Expr[_, _]): Delete = {
        if (test) {
            sqlDelete.addCondition(getExpr(condition))
        }
        this
    }

    override def sql(db: DB): String = toSqlString(sqlDelete, db)

    override def toSql(using db: DB): String = toSqlString(sqlDelete, db)
}

object Delete {
    def apply(): Delete = new Delete()
}
