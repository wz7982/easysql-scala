package easysql.macros

import easysql.dsl.{ColumnExpr, TableSchema}

inline def fetchTableName[T <: Product]: String = ${ fetchTableNameMacro[T] }

inline def exprMeta[T](inline name: String): (String, String) = ${ exprMetaMacro[T]('name) }

inline def fieldNames[T]: List[String] = ${ fieldNamesMacro[T] }

inline def identNames[T]: List[String] = ${ identNamesMacro[T] }