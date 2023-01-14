package org.easysql.macros

import org.easysql.dsl.{TableColumnExpr, TableSchema}

inline def aliasMacro[E <: Product, T <: TableSchema[E]](table: T): T = ${ aliasMacroImpl[E, T]('table) }

inline def fetchTableNameMacro[T <: Product]: String = ${ fetchTableNameMacroImpl[T] }

inline def exprMetaMacro[T](inline name: String): (String, String) = ${ exprMetaMacroImpl[T]('name) }

inline def fieldNamesMacro[T]: List[String] = ${ fieldNamesMacroImpl[T] }

inline def identNamesMacro[T]: List[String] = ${ identNamesMacroImpl[T] }