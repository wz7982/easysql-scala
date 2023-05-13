package easysql.macros

import easysql.dsl.*

inline def fetchTableName[T <: Product]: String = 
    ${ fetchTableNameMacro[T] }

inline def exprMeta[T](inline name: String): String = 
    ${ exprMetaMacro[T]('name) }

inline def columnsMeta[T]: List[(String, String)] =
    ${ columnsMetaMacro[T] }

inline def fieldNames[T]: List[String] = 
    ${ fieldNamesMacro[T] }

inline def identNames[T]: List[String] = 
    ${ identNamesMacro[T] }

transparent inline def tableInfo[T <: Product]: Any = 
    ${ tableInfoMacro[T] }