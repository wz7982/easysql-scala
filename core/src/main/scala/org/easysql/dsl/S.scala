package org.easysql.dsl

import org.easysql.database.TableEntity
import org.easysql.query.delete.Delete
import org.easysql.query.insert.{Insert, InsertEntity}
import org.easysql.query.save.Save
import org.easysql.query.select.Select
import org.easysql.query.truncate.Truncate
import org.easysql.query.update.Update
import org.easysql.macros.findMacro

object S {
    infix def from(table: TableSchema) = Select().from(table)

    inline infix def find[T <: TableEntity[_]](pk: PK[T]): Select[_, _] = findMacro[T](Select(), pk)

    infix def insertInto(table: TableSchema)(columns: Tuple) = Insert().insertInto(table)(columns)

    inline infix def insert[T <: TableEntity[_]](entity: T*) = Insert().insert(entity: _*)

    inline infix def save[T <: TableEntity[_]](entity: T): Save = Save().save(entity)

    infix def update(table: TableSchema | String): Update = Update().update(table)

    inline infix def update[T <: TableEntity[_]](entity: T, skipNull: Boolean = true): Update = Update().update(entity, skipNull)

    infix def deleteFrom(table: TableSchema | String): Delete = Delete().deleteFrom(table)

    inline infix def delete[T <: TableEntity[_]](pk: PK[T]): Delete = Delete().delete[T](pk)

    infix def truncate(table: TableSchema | String): Truncate = Truncate().truncate(table)
}
