package org.easysql.macros

import org.easysql.dsl.{TableColumnExpr, TableSchema}
import org.easysql.util.*

import scala.quoted.{Expr, Quotes, Type}
import scala.annotation.experimental
import scala.collection.mutable.ListBuffer

def aliasMacroImpl[E <: Product, T <: TableSchema[E]](table: Expr[T])(using quotes: Quotes, tt: Type[T], et: Type[E]): Expr[T] = {
    import quotes.reflect.*

    val className = TypeRepr.of[T].typeSymbol.fullName
    if (className.trim != "org.easysql.dsl.TableSchema") {
        val classSym = Symbol.requiredClass(className)
        val tree = Apply(Select.unique(New(TypeIdent(classSym)), "<init>"), Nil)
        tree.asExprOf[T]
    } else '{
        new TableSchema[E] {
            override val _tableName: String = $table._tableName
            override val _cols = $table._cols
        }.asInstanceOf[T]
    }
}

def fetchTableNameMacroImpl[T <: Product](using quotes: Quotes, tpe: Type[T]): Expr[String] = {
    import quotes.reflect.*

    val sym = TypeTree.of[T].symbol
    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(name)), _), Literal(v) :: Nil) if name == "Table" => v.value.toString()
        case _ => ""
    }.find(_ != "") match {
        case None => camelToSnake(sym.name)
        case Some(value) => value
    }

    Expr(tableName)
}

@experimental
def exprMetaMacroImpl[T](name: Expr[String])(using q: Quotes, t: Type[T]): Expr[(String, String)] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val eles = sym.declaredFields.map(_.name)
    if (!eles.contains(name.value.get)) {
        report.error(s"value ${name.value.get} is not a member of ${sym.name}")
    }

    val ele = sym.declaredField(name.value.get)

    var eleTag = "column"
    var eleName = camelToSnake(name.value.get)

    ele.annotations.find {
        case Apply(Select(New(TypeIdent(name)), _), _) if name == "PrimaryKey" || name == "IncrKey" || name == "Column" => true
        case _ => false
    } match {
        case Some(Apply(Select(New(TypeIdent(name)), _), args)) => {
            name match {
                case "PrimaryKey" => eleTag = "pk"
                case "IncrKey" => eleTag = "incr"
                case _ =>
            }

            args match {
                case Literal(v) :: _ => eleName = v.value.toString
                case _ =>
            }
        }

        case _ =>
    }

    Expr(eleTag *: eleName *: EmptyTuple)
}

def fieldNamesMacroImpl[T](using q: Quotes, t: Type[T]): Expr[List[String]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields.map { f =>
        var fieldName = camelToSnake(f.name)

        f.annotations.find {
            case Apply(Select(New(TypeIdent(name)), _), _) if name == "PrimaryKey" || name == "IncrKey" || name == "Column" => true
            case _ => false
        } match {
            case Some(Apply(_, args)) => {
                args match {
                    case Literal(v) :: _ => fieldName = v.value.toString
                    case _ =>
                }
            }

            case _ =>
        }

        Expr(fieldName)
    }

    Expr.ofList(fields)
}

def identNamesMacroImpl[T](using q: Quotes, t: Type[T]): Expr[List[String]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields.map { f =>
        var fieldName = f.name

        Expr(fieldName)
    }

    Expr.ofList(fields)
}