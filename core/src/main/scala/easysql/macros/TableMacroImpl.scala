package easysql.macros

import easysql.util.*
import easysql.dsl.*
import easysql.ast.SqlDataType

import scala.quoted.{Expr, Quotes, Type}

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

def exprMetaMacro[T](name: Expr[String])(using q: Quotes, t: Type[T]): Expr[String] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val ele = sym.declaredField(name.value.get)
    var eleTag = "column"

    val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")

    ele.annotations.find {
        case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
        case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), _) if name == "CustomColumn" => true
        case _ => false
    } match {
        case Some(Apply(Select(New(TypeIdent(name)), _), args)) => {
            name match {
                case "PrimaryKey" | "PrimaryKeyGenerator" => eleTag = "pk"
                case "IncrKey" => eleTag = "incr"
                case _ =>
            }
        }

        case _ =>
    }

    Expr(eleTag)
}

def columnsMetaMacro[T](using q: Quotes, t: Type[T]): Expr[List[(String, String)]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val eles = sym.declaredFields

    val info = eles.map { e =>
        var eleName = camelToSnake(e.name)

        val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")

        e.annotations.find {
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), _) if name == "CustomColumn" => true
            case _ => false
        } match {
            case Some(Apply(_, args)) => {
                args match {
                    case Literal(v) :: _ => eleName = v.value.toString
                    case _ =>
                }
            }

            case _ =>
        }

        e.name -> eleName
    }

    Expr(info)
}

def fieldNamesMacro[T](using q: Quotes, t: Type[T]): Expr[List[String]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields.map { f =>
        var fieldName = camelToSnake(f.name)

        val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")

        f.annotations.find {
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), _) if name == "CustomColumn" => true
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
    val fields = sym.declaredFields.map(f => Expr(f.name))

    Expr.ofList(fields)
}

def tableInfoMacro[T <: Product](using q: Quotes, t: Type[T]): Expr[Any] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields
    val tableName = fetchTableNameMacro[T]
    val typs = fields.map { field =>
        val singletonName = Singleton(Expr(field.name).asTerm)
        val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")
        var columnName = field.name

        val columnType: (String, Option[Type[_]], List[Term]) = field.annotations.find {
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), _) if name == "CustomColumn" => true
            case _ => false
        } match {
            case Some(Apply(Select(New(TypeIdent("PrimaryKey")), _), args)) => ("pk", None, args)
            case Some(Apply(Select(New(TypeIdent("PrimaryKeyGenerator")), _), args)) => ("pk", None, args)
            case Some(Apply(Select(New(TypeIdent("IncrKey")), _), args)) => ("pk", None, args)
            case Some(Apply(TypeApply(Select(New(TypeIdent(_)), _), t), args)) => ("custom", Some(t(1).tpe.asType), args)
            case Some(Apply(Select(New(TypeIdent(_)), _), args)) => ("column", None, args)
            case _ => ("column", None, Nil)
        }

        columnType._3 match {
            case Literal(v) :: _ => columnName = v.value.toString
            case _ =>
        }

        singletonName.tpe.asType match {
            case '[n] => columnType match {
                case ("custom", Some(customType), _) => {
                    customType match {
                        case '[c] => 
                            (field.name, TypeRepr.of[ColumnExpr[c & SqlDataType, n & String]], columnName)
                    }
                }
                case ("pk", _, _) => field.tree match {
                    case vd: ValDef => {
                        val vdt = vd.tpt.tpe.asType
                        vdt match {
                            case '[t] =>
                                (field.name, TypeRepr.of[PrimaryKeyExpr[t & SqlDataType, n & String]], columnName)
                        }
                    }
                }
                case _ => field.tree match {
                    case vd: ValDef => {
                        val vdt = vd.tpt.tpe.asType
                        vdt match {
                            case '[Option[t]] => 
                                (field.name, TypeRepr.of[ColumnExpr[t & SqlDataType, n & String]], columnName)
                            case '[t] => 
                                (field.name, TypeRepr.of[ColumnExpr[t & SqlDataType, n & String]], columnName)
                        }
                    }
                }
            }
        }
    }

    var refinement = Refinement(TypeRepr.of[TableSchema[T]], typs.head._1, typs.head._2)
    for (i <- 1 until typs.size) {
        refinement = Refinement(refinement, typs(i)._1, typs(i)._2)
    }
    
    refinement.asType match {
        case '[t] => '{
            val columns = columnsMeta[T].map((ident, column) => ColumnExpr($tableName, column, ident))
            new TableSchema[T]($tableName, None, columns).asInstanceOf[t]
        }
    }
}