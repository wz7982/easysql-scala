package easysql.macros

import easysql.ast.SqlDataType

inline def insertMetaData[T <: Product](x: T): List[(String, Any)] =
    ${ insertMetaDataMacro[T]('x) }

inline def updateMetaData[T <: Product]: (String, List[(String, T => SqlDataType)], List[(String, T => SqlDataType | Option[SqlDataType])]) = 
    ${ updateMetaDataMacro[T] }

inline def fetchPk[T <: Product, PK <: SqlDataType | Tuple]: (String, List[String]) = 
    ${ fetchPkMacro[T, PK] }