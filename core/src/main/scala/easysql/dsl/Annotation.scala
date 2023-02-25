package easysql.dsl

import easysql.ast.SqlDataType

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
case class Table(tableName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class PrimaryKey(columnName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class IncrKey(columnName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class PrimaryKeyGenerator(columnName: String, gernerator: () => SqlDataType) extends StaticAnnotation

@scala.annotation.meta.field
case class Column(columnName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class CustomColumn[T, D <: SqlDataType](columnName: String, toValue: T => D, fromValue: Any => T) extends StaticAnnotation