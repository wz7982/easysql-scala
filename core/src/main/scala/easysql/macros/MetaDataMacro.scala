package easysql.macros

import easysql.ast.SqlDataType

inline def insertMetaData[T <: Product](entity: T): List[(String, SqlDataType | Option[SqlDataType])] =
    ${ insertMetaDataMacro[T]('entity) }

inline def updateMetaData[T <: Product](entity: T): (List[(String, SqlDataType)], List[(String, SqlDataType | Option[SqlDataType])]) = 
    ${ updateMetaDataMacro[T]('entity) }

inline def fetchPk[T <: Product, PK <: SqlDataType | Tuple]: (String, List[String]) = 
    ${ fetchPkMacro[T, PK] }