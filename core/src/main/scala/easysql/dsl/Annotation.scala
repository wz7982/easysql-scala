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
case class PrimaryKeyGenerator(columnName: String, generator: () => SqlDataType) extends StaticAnnotation

@scala.annotation.meta.field
case class Column(columnName: String = "") extends StaticAnnotation

trait CustomSerializer[T, D] {
    def toValue(x: T): D

    def fromValue(x: Any): T
}

@scala.annotation.meta.field
case class CustomColumn[T, D <: SqlDataType](columnName: String, serializer: CustomSerializer[T, D]) extends StaticAnnotation