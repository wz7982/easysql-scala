package easysql.dsl

import easysql.ast.SqlDataType
import easysql.macros.columnsMeta
import easysql.query.select.Select

import scala.collection.BufferedIterator
import scala.collection.mutable.ListBuffer
import scala.deriving.*
import scala.language.dynamics

class JPA[E <: Product](val table: TableSchema[E]) extends Dynamic {
    inline def applyDynamic[N <: String & Singleton](
        inline name: N
    )(using m: Mirror.ProductOf[E])(
        args: JPAArgsType[m.MirroredElemTypes, m.MirroredElemLabels, SplitUnderline[N]]
    ): Select[Tuple1[E], _] = {
        import scala.language.unsafeNulls

        val query = select(table).from(table)

        val tokens = name.split("_").iterator
        val argValues = args.toList.iterator
        val columns = columnsMeta[E].toMap

        fetchCondition(table.__tableName, tokens.buffered, argValues, columns) match {
            case None => query
            case Some(value) => query.where(value)
        }
    }

    def fetchCondition(tableName: String, tokens: BufferedIterator[String], args: Iterator[Any], columns: Map[String, String]): Option[Expr[Boolean]] = {
        import easysql.dsl.unsafeOperator

        var condition: Option[Expr[Boolean]] = None

        while tokens.hasNext do {
            val token = tokens.next

            token match {
                case ("by" | "and" | "or") => {
                    val columnName = columns(tokens.next)
                    val column = col(s"$tableName.$columnName")

                    val predicate = tokens.headOption
                    val tempCondition = predicate match {
                        case Some("in") => {
                            val value = args.next.asInstanceOf[List[SqlDataType]]
                            column.in(value.map(LiteralExpr(_)))
                        }
                        case Some("notIn") => {
                            val value = args.next.asInstanceOf[List[SqlDataType]]
                            column.notIn(value.map(LiteralExpr(_)))
                        }
                        case Some("like") => {
                            val value = args.next.asInstanceOf[String]
                            column.like("%" + value + "%")
                        }
                        case Some("notLike") => {
                            val value = args.next.asInstanceOf[String]
                            column.notLike("%" + value + "%")
                        }
                        case Some("startingWith") => {
                            val value = args.next.asInstanceOf[String]
                            column.notLike(value + "%")
                        }
                        case Some("endingWith") => {
                            val value = args.next.asInstanceOf[String]
                            column.notLike("%" + value)
                        }
                        case Some("gt") => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column > value
                        }
                        case Some("ge") => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column >= value
                        }
                        case Some("lt") => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column < value
                        }
                        case Some("le") => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column <= value
                        }
                        case Some("not") => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column <> value
                        }
                        case Some("between") => {
                            val start = args.next.asInstanceOf[SqlDataType]
                            val end = args.next.asInstanceOf[SqlDataType]
                            column.between(start, end)
                        }
                        case Some("notBetween") => {
                            val start = args.next.asInstanceOf[SqlDataType]
                            val end = args.next.asInstanceOf[SqlDataType]
                            column.notBetween(start, end)
                        }
                        case Some("isNull") => 
                            column === None
                        case Some("isNotNull") =>
                            column <> None
                        case _ => {
                            val value = args.next.asInstanceOf[SqlDataType]
                            column === value
                        }
                    } 

                    if token != "or" then {
                        condition = condition match {
                            case None => Some(tempCondition)
                            case Some(expr) => Some(expr && tempCondition)
                        }
                    } else {
                        condition = condition match {
                            case None => Some(tempCondition)
                            case Some(expr) => Some(expr || tempCondition)
                        }
                    }
                    
                }
                case _ =>
            }
        }

        condition
    }
}  

object JPA {
    def apply[E <: Product](table: TableSchema[E]): JPA[E] =
        new JPA[E](table)

    given singletonToTuple1[T]: Conversion[T, Tuple1[T]] = 
        Tuple1(_)
}