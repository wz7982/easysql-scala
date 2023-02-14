package easysql.macros

import easysql.util.*

import scala.quoted.{Expr, Quotes, Type}
import scala.annotation.experimental
import scala.collection.mutable.ListBuffer

def fetchTableNameMacro[T <: Product](using quotes: Quotes, tpe: Type[T]): Expr[String] = {
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

def exprMetaMacro[T](name: Expr[String])(using q: Quotes, t: Type[T]): Expr[(String, String)] = {
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

def fieldNamesMacro[T](using q: Quotes, t: Type[T]): Expr[List[String]] = {
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

def identNamesMacro[T](using q: Quotes, t: Type[T]): Expr[List[String]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields.map { f =>
        var fieldName = f.name

        Expr(fieldName)
    }

    Expr.ofList(fields)
}