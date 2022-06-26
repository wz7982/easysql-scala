package org.easysql.macros

import org.easysql.database.TableEntity
import org.easysql.query.select.Select

inline def findMacro[T <: TableEntity[_]](select: Select[_, _], primaryKey: Any): Select[_, _] = ${ findMacroImpl[T]('select, 'primaryKey) }
