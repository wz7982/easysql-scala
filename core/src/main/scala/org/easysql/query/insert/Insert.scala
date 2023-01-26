package org.easysql.query.insert

import org.easysql.ast.statement.insert.SqlInsert
import org.easysql.ast.SqlDataType
import org.easysql.ast.table.SqlIdentTable
import org.easysql.database.DB
import org.easysql.dsl.*
import org.easysql.query.ReviseQuery
import org.easysql.util.{anyToExpr, toSqlString}
import org.easysql.visitor.*
import org.easysql.query.select.*
import org.easysql.macros.*

import java.sql.Connection
import scala.collection.mutable.ListBuffer

class Insert[T <: Tuple, S <: InsertState](val sqlInsert: SqlInsert = SqlInsert()) extends ReviseQuery {
    inline def insert[T <: Product, SS >: S <: InsertEntity](entities: T*): Insert[Tuple1[T], InsertEntity] = {
        val insertMetaData = insertMacro[T]

        sqlInsert.table = Some(SqlIdentTable(insertMetaData._1))
        val insertList = entities.toList map { entity =>
            insertMetaData._2 map { i =>
                visitExpr(anyToExpr(i._2.apply(entity)))
            }
        }
        sqlInsert.values.addAll(insertList)
        sqlInsert.columns.addAll(insertMetaData._2.map(e => visitExpr(ColumnExpr(e._1))))

        new Insert[Tuple1[T], InsertEntity](sqlInsert)
    }

    infix def insertInto(table: TableSchema[_])(columns: Tuple): Insert[InverseMap[columns.type], Nothing] = {
        type ValueTypes = InverseMap[columns.type]

        val insert = new Insert[ValueTypes, Nothing](sqlInsert)
        insert.sqlInsert.table = Some(SqlIdentTable(table._tableName))
        insert.sqlInsert.columns.addAll {
            columns.toArray.map {
                case t: TableColumnExpr[_, _] => getExpr(col(t.column))
                case p: PrimaryKeyColumnExpr[_, _] => getExpr(col(p.column))
            }
        }

        insert
    }

    infix def values[SS >: S <: InsertValues](values: T*): Insert[T, InsertValues] = {
        values.foreach { value =>
            val row = value.toArray.map(it => getExpr(anyToExpr(it))).toList
            sqlInsert.values.addOne(row)
        }

        new Insert[T, InsertValues](sqlInsert)
    }

    infix def select[SS >: S <: InsertSelect](s: SelectQuery[T, _]): Insert[T, InsertSelect] = {
        sqlInsert.query = Some(s.getSelect)
        new Insert[T, InsertSelect](sqlInsert)
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
