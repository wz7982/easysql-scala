package easysql.macros

import easysql.ast.SqlDataType

inline def insertMetaData[T <: Product]: (String, List[(String, T => SqlDataType | Option[SqlDataType])]) = 
    ${ insertMetaDataMacro[T] }

inline def updateMetaData[T <: Product]: (String, List[(String, T => SqlDataType)], List[(String, T => SqlDataType | Option[SqlDataType])]) = 
    ${ updateMetaDataMacro[T] }

inline def fetchPk[T <: Product, PK <: SqlDataType | Tuple]: (String, List[String]) = 
    ${ fetchPkMacro[T, PK] }