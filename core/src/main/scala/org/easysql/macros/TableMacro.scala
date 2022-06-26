package org.easysql.macros

import org.easysql.dsl.{TableColumnExpr, TableSchema, NothingTable}

inline def columnsMacro[T <: TableSchema](table: T): Map[String, TableColumnExpr[_, _]] = ${ columnsMacroImpl[T]('table) }
