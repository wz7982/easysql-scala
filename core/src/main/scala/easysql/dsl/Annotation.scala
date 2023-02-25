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
case class CustomColumn[T](columnName: String, toValue: T => SqlDataType, fromValue: Any => T) extends StaticAnnotation