package org.easysql.dsl

import org.easysql.ast.SqlSingleConstType
import org.easysql.ast.expr.SqlSubQueryPredicate
import org.easysql.database.{DB, TableEntity}
import org.easysql.dsl.ConstExpr
import org.easysql.query.delete.Delete
import org.easysql.query.insert.Insert
import org.easysql.query.save.Save
import org.easysql.query.select.{Select, SelectQuery}
import org.easysql.query.truncate.Truncate
import org.easysql.query.update.Update
import org.easysql.macros.findMacro

def const[T <: SqlSingleConstType | Null](value: T) = ConstExpr[T](value)

def col[T <: SqlSingleConstType | Null](column: String) = ColumnExpr[T](column)

def caseWhen[T <: SqlSingleConstType | Null](conditions: CaseBranch[T]*) = CaseExpr[T, NothingTable](conditions.toList)

def exists(select: SelectQuery[_]) = SubQueryPredicateExpr[Boolean](select, SqlSubQueryPredicate.EXISTS)

def notExists(select: SelectQuery[_]) = SubQueryPredicateExpr[Boolean](select, SqlSubQueryPredicate.NOT_EXISTS)

def all[T <: SqlSingleConstType | Null](select: SelectQuery[Tuple1[T]]) = SubQueryPredicateExpr[T](select, SqlSubQueryPredicate.ALL)

def any[T <: SqlSingleConstType | Null](select: SelectQuery[Tuple1[T]]) = SubQueryPredicateExpr[T](select, SqlSubQueryPredicate.ANY)

def some[T <: SqlSingleConstType | Null](select: SelectQuery[Tuple1[T]]) = SubQueryPredicateExpr[T](select, SqlSubQueryPredicate.SOME)

def cast[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](expr: Expr[_, Table], castType: String) = CastExpr[T, Table](expr, castType)

def table(name: String) = new TableSchema {
    override val tableName: String = name
}

extension [T <: SqlSingleConstType | Null, Table <: TableSchema](e: TableColumnExpr[T, Table] | ColumnExpr[T]) {
    def to[V <: T](value: V | Expr[V, _] | SelectQuery[Tuple1[V]]) = (e, value)
}

def ** = AllColumnExpr()

def from(table: TableSchema) = Select().from(table)

inline def find[T <: TableEntity[_]](pk: PK[T]): Select[_, _] = findMacro[T](Select(), pk)

def insertInto(table: TableSchema)(columns: Tuple) = Insert().insertInto(table)(columns)

inline def insert[T <: TableEntity[_]](entity: T*) = Insert().insert(entity: _*)

inline def save[T <: TableEntity[_]](entity: T): Save = Save().save(entity)

def update(table: TableSchema | String): Update = Update().update(table)

inline def update[T <: TableEntity[_]](entity: T, skipNull: Boolean = true): Update = Update().update(entity, skipNull)

def deleteFrom(table: TableSchema | String): Delete = Delete().deleteFrom(table)

inline def delete[T <: TableEntity[_]](pk: PK[T]): Delete = Delete().delete[T](pk)

def truncate(table: TableSchema | String): Truncate = Truncate().truncate(table)

extension (s: StringContext) {
    def sql(args: (SqlSingleConstType | List[SqlSingleConstType])*): String = {
        import org.easysql.util.*
        import org.easysql.visitor.*

        val pit = s.parts.iterator
        val builder = StringBuilder(pit.next())
        args.foreach { arg =>
            val visitor = getOutPutVisitor(DB.MYSQL)
            val expr = getExpr(anyToExpr(arg))
            visitor.visitSqlExpr(expr)
            builder.append(visitor.sql())
            builder.append(pit.next())
        }
        builder.toString
    }
}