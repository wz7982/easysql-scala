package org.easysql.query.insert

import org.easysql.ast.expr.SqlIdentifierExpr
import org.easysql.ast.statement.insert.SqlInsert
import org.easysql.ast.SqlSingleConstType
import org.easysql.database.{DB, TableEntity}
import org.easysql.dsl.{ConstExpr, Expr, InverseMap, PrimaryKeyColumnExpr, TableColumnExpr, TableSchema, col}
import org.easysql.query.ReviseQuery
import org.easysql.util.{anyToExpr, toSqlString}
import org.easysql.visitor.getExpr
import org.easysql.macros.insertMacro
import org.easysql.query.select.*

import java.sql.Connection
import scala.collection.mutable.ListBuffer

class Insert[T <: Tuple, S <: InsertState] extends ReviseQuery {
    var sqlInsert: SqlInsert = SqlInsert()

    inline infix def insert[T <: TableEntity[_], SS >: S <: InsertEntity](entity: T*): Insert[_, InsertEntity] = {
        insertMacro(this, entity)

        this.asInstanceOf[Insert[_, InsertEntity]]
    }

    infix def insertInto(table: TableSchema)(columns: Tuple): Insert[InverseMap[columns.type], Nothing] = {
        type ValueTypes = InverseMap[columns.type]

        val insert = new Insert[ValueTypes, Nothing]()
        insert.sqlInsert.table = Some(SqlIdentifierExpr(table.tableName))
        insert.sqlInsert.columns.addAll(columns.toArray.map {
            case t: TableColumnExpr[_, _] => getExpr(col(t.column))
            case p: PrimaryKeyColumnExpr[_, _] => getExpr(col(p.column))
        })

        insert
    }

    infix def values[SS >: S <: InsertValues](values: T*): Insert[T, InsertValues] = {
        values.foreach { value =>
            val row = value.toArray.map(it => getExpr(anyToExpr(it))).toList
            sqlInsert.values.addOne(row)
        }

        this.asInstanceOf[Insert[T, InsertValues]]
    }

    infix def select[SS >: S <: InsertSelect](s: SelectQuery[T]): Insert[T, InsertSelect] = {
        sqlInsert.query = Some(s.getSelect)
        this.asInstanceOf[Insert[T, InsertSelect]]
    }

    override def sql(db: DB): String = toSqlString(sqlInsert, db)

    override def toSql(using db: DB): String = toSqlString(sqlInsert, db)
}

object Insert {
    def apply(): Insert[EmptyTuple, Nothing] = new Insert()
}

sealed trait InsertState

final class InsertEntity extends InsertState

final class InsertValues extends InsertState

final class InsertSelect extends InsertState
