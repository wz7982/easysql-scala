package org.easysql.dsl

import org.easysql.database.TableEntity
import org.easysql.dsl.{AllColumnExpr, TableColumnExpr}
import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.table.SqlJoinType
import org.easysql.macros.columnsMacro
import org.easysql.query.select.{SelectQuery, Query}

import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.language.dynamics

trait TableSchema {
    val tableName: String

    var $columns: ListBuffer[TableColumnExpr[?, this.type]] = ListBuffer[TableColumnExpr[?, this.type]]()

    def column[T <: SqlSingleConstType](name: String): TableColumnExpr[T, this.type] = {
        val c = TableColumnExpr[T, this.type](tableName, name)
        $columns.addOne(c)
        c
    }

    def intColumn(name: String): TableColumnExpr[Int, this.type] = this.column[Int](name)
    
    def varcharColumn(name: String): TableColumnExpr[String, this.type] = this.column[String](name)

    def longColumn(name: String): TableColumnExpr[Long, this.type] = this.column[Long](name)

    def floatColumn(name: String): TableColumnExpr[Float, this.type] = this.column[Float](name)

    def doubleColumn(name: String): TableColumnExpr[Double, this.type] = this.column[Double](name)

    def booleanColumn(name: String): TableColumnExpr[Boolean, this.type] = this.column[Boolean](name)

    def dateColumn(name: String): TableColumnExpr[Date, this.type] = this.column[Date](name)

    def decimalColumn(name: String): TableColumnExpr[BigDecimal, this.type] = this.column[BigDecimal](name)
}

object TableSchema {
    inline given tableToQuery[T <: TableSchema]: Conversion[T, Query[T]] = Query[T](_)
}

class NothingTable extends TableSchema {
    override val tableName: String = ""
}

class AnyTable extends TableSchema {
    override val tableName: String = ""
}

extension[T <: TableSchema] (t: T) {
    inline infix def as(aliasName: String)(using NonEmpty[aliasName.type] =:= Any): AliasedTableSchema = {
        val columns = columnsMacro[T](t)
        AliasedTableSchema(t.tableName, aliasName, columns)
    }
    
    inline infix def unsafeAs(aliasName: String): AliasedTableSchema = {
        val columns = columnsMacro[T](t)
        AliasedTableSchema(t.tableName, aliasName, columns)
    }
    
    infix def join(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.JOIN, table)

    infix def leftJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.LEFT_JOIN, table)

    infix def rightJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.RIGHT_JOIN, table)

    infix def innerJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.INNER_JOIN, table)

    infix def crossJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.CROSS_JOIN, table)

    infix def fullJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(t, SqlJoinType.FULL_JOIN, table)
}

case class AliasedTableSchema(tableName: String, aliasName: String, columns: Map[String, TableColumnExpr[_, _]]) extends Dynamic {
    def selectDynamic(name: String) = columns(name).copy(table = aliasName)

    infix def join(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.JOIN, table)

    infix def leftJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.LEFT_JOIN, table)

    infix def rightJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.RIGHT_JOIN, table)

    infix def innerJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.INNER_JOIN, table)

    infix def crossJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.CROSS_JOIN, table)

    infix def fullJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.FULL_JOIN, table)
}

case class JoinTableSchema(left: TableSchema | JoinTableSchema | AliasedTableSchema, joinType: SqlJoinType, right: TableSchema | JoinTableSchema | AliasedTableSchema, var onCondition: Option[Expr[_, _]] = None) {
    infix def join(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.JOIN, table)

    infix def leftJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.LEFT_JOIN, table)

    infix def rightJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.RIGHT_JOIN, table)

    infix def innerJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.INNER_JOIN, table)

    infix def crossJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.CROSS_JOIN, table)

    infix def fullJoin(table: TableSchema | JoinTableSchema | AliasedTableSchema) = JoinTableSchema(this, SqlJoinType.FULL_JOIN, table)

    infix def on(expr: Expr[_, _]) = {
        this.onCondition = Some(expr)
        this
    }
}