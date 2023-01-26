package org.easysql.macros

import org.easysql.ast.SqlDataType

inline def insertMacro[T <: Product]: (String, List[(String, T => Any)]) = ${ insertMacroImpl[T] }

inline def updateMacro[T <: Product]: (String, List[(String, T => Any)], List[(String, T => Any)]) = ${ updateMacroImpl[T] }

inline def pkMacro[T <: Product, PK <: SqlDataType | Tuple]: (String, List[String]) = ${ pkMacroImpl[T, PK] }