package org.easysql.macros

import org.easysql.dsl.TableSchema
import org.easysql.ast.SqlDataType
import org.easysql.util.*

import scala.quoted.{Expr, Quotes, Type}
import scala.collection.mutable.*
import scala.annotation.experimental

def insertMacroImpl[T <: Product](using q: Quotes, tpe: Type[T]): Expr[(String, List[(String, T => Any)])] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val insertFieldExprs = ListBuffer[Expr[String]]()
    val insertFunctionExprs = ListBuffer[Expr[T => Any]]()

    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(name)), _), Literal(v) :: Nil) if name == "Table" => v.value.toString()
        case _ => ""
    }.find(_ != "") match {
        case None => camelToSnake(sym.name)
        case Some(value) => value
    }

    val fields = sym.declaredFields
    fields.foreach { field =>
        val annoInfo = field.annotations.map { a =>
            a match {
                case Apply(Select(New(TypeIdent(name)), _), args) if name == "PrimaryKey" || name == "Column" || name == "IncrKey" =>
                    args match {
                        case Literal(v) :: arg => (name, v.value.toString(), args)
                        case _ => (name, "", args)
                    }
                case _ => ("", "", Nil)
            }
        }
        
        val insertName = annoInfo.find(_._2 != "") match {
            case None => camelToSnake(field.name)
            case Some(_, value, _) => value
        }

        def createLambda = {
            val mtpe = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[Any])
            def rhsFn(sym: Symbol, paramRefs: List[Tree]) = {
                val x = paramRefs.head.asExprOf[T].asTerm
                Select.unique(x, field.name)
            }
            Lambda(field, mtpe, rhsFn).asExprOf[T => Any]
        }

        def createGeneratorLambda(s: Statement) = {
            val mtpe = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[Any])
            def rhsFn(sym: Symbol, paramRefs: List[Tree]): Tree = s match {
                case DefDef(_, _, _, t) => t.get
            }
            Lambda(field, mtpe, rhsFn).asExprOf[T => Any]
        }

        val lambda = annoInfo.find(_._1 != "") match {
            case Some("PrimaryKey", _, args) => args match {
                case _ :: NamedArg(_, Block(l, _)) :: _ => createGeneratorLambda(l.head)
                case _ :: Block(l, _) :: _ => createGeneratorLambda(l.head)
                case _ => createLambda
            }
            case _ => createLambda
        }

        annoInfo.find(_._1 == "IncrKey") match {
            case None => {
                insertFieldExprs.addOne(Expr.apply(insertName))
                insertFunctionExprs.addOne(lambda)
            }
            case _ =>
        }
    }

    if (insertFieldExprs.size == 0) {
        report.error(s"entity ${sym.name} has no field for inserting data")
    }

    val insertFields = Expr.ofList(insertFieldExprs.toList)
    val insertFunctions = Expr.ofList(insertFunctionExprs.toList)
    val tableNameExpr = Expr(tableName)

    '{ 
        val insertList = $insertFields.zip($insertFunctions)
        $tableNameExpr -> insertList
    }
}

def updateMacroImpl[T <: Product](using q: Quotes, tpe: Type[T]): Expr[(String, List[(String, T => Any)], List[(String, T => Any)])] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val pkFieldExprs = ListBuffer[Expr[String]]()
    val pkFunctionExprs = ListBuffer[Expr[T => Any]]()
    val updateFieldExprs = ListBuffer[Expr[String]]()
    val updateFunctionExprs = ListBuffer[Expr[T => Any]]()

    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(name)), _), Literal(v) :: Nil) if name == "Table" => v.value.toString()
        case _ => ""
    }.find(_ != "") match {
        case None => camelToSnake(sym.name)
        case Some(value) => value
    }

    val fields = sym.declaredFields
    fields.foreach { field =>
        val annoInfo = field.annotations.map { a =>
            a match {
                case Apply(Select(New(TypeIdent(name)), _), args) if name == "PrimaryKey" || name == "Column" || name == "IncrKey" =>
                    args match {
                        case Literal(v) :: _ => name -> v.value.toString()
                        case _ => name -> ""
                    }
                case _ => "" -> ""
            }
        }

        val fieldName = annoInfo.find(_._2 != "") match {
            case None => camelToSnake(field.name)
            case Some(_, value) => value
        }

        val mtpe = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[Any])
        def rhsFn(sym: Symbol, paramRefs: List[Tree]) = {
            val x = paramRefs.head.asExprOf[T].asTerm
            Select.unique(x, field.name)
        }
        val lambda = Lambda(field, mtpe, rhsFn).asExprOf[T => Any]

        annoInfo.find {
            case (name, _) if name == "PrimaryKey" || name == "IncrKey" => true
            case _ => false
        } match {
            case None => {
                updateFieldExprs.addOne(Expr.apply(fieldName))
                updateFunctionExprs.addOne(lambda)
            }
            case _ => {
                pkFieldExprs.addOne(Expr.apply(fieldName))
                pkFunctionExprs.addOne(lambda)
            }
        }
    }

    if (pkFieldExprs.size == 0) {
        report.error(s"primary key field is not defined in entity ${sym.name}")
    }

    if (updateFieldExprs.size == 0) {
        report.error(s"entity ${sym.name} has no fields to update")
    }

    val pkFields = Expr.ofList(pkFieldExprs.toList)
    val pkFunctions = Expr.ofList(pkFunctionExprs.toList)
    val updateFields = Expr.ofList(updateFieldExprs.toList)
    val updateFunctions = Expr.ofList(updateFunctionExprs.toList)
    val tableNameExpr = Expr(tableName)

    '{ 
        val pkList = $pkFields.zip($pkFunctions)
        val updateList = $updateFields.zip($updateFunctions)
        ($tableNameExpr, pkList, updateList)
    }
}

@experimental
def pkMacroImpl[T <: Product, PK <: SqlDataType | Tuple](using q: Quotes, t: Type[T], p: Type[PK]): Expr[(String, List[String])] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val pkFieldExprs = ListBuffer[Expr[String]]()

    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(name)), _), Literal(v) :: Nil) if name == "Table" => v.value.toString()
        case _ => ""
    }.find(_ != "") match {
        case None => camelToSnake(sym.name)
        case Some(value) => value
    }

    val fields = sym.declaredFields
    var argTypeNames = ListBuffer[String]()
    if (TypeRepr.of[PK].typeSymbol.name.startsWith("Tuple")) {
        val symTree = TypeRepr.of[PK].termSymbol.tree
        symTree match {
            case ValDef(_, vt, _) => 
                val args = vt.tpe.typeArgs.map(arg => arg.typeSymbol.name)
                argTypeNames.addAll(args)
        }
    } else {
        argTypeNames.addOne(TypeRepr.of[PK].typeSymbol.name)
    }

    val pkTypeNames = ListBuffer[String]()

    fields.foreach { field =>
        field.annotations.find {
            case Apply(Select(New(TypeIdent(name)), _), _) if name == "PrimaryKey" || name == "IncrKey" => true
            case _ => false
        } match {
            case Some(Apply(_, args)) => {
                val fieldName = args match {
                    case Literal(v) :: _ => v.value.toString()
                    case _ =>  camelToSnake(field.name)
                }
                pkFieldExprs.addOne(Expr.apply(fieldName))
                field.tree match {
                    case vd: ValDef => pkTypeNames.addOne(vd.tpt.tpe.typeSymbol.name)
                }
            }
            case _ =>
        }
    }

    if (pkFieldExprs.size == 0) {
        report.error(s"primary key field is not defined in entity ${sym.name}")
    }

    if (pkTypeNames.size != argTypeNames.size) {
        report.error(s"the parameter is inconsistent with the primary key type defined in entity class ${sym.name}")
    } else {
        pkTypeNames.zip(argTypeNames).foreach { (pt, at) =>
            if (pt != at) {
                report.error(s"the parameter is inconsistent with the primary key type defined in entity class ${sym.name}")
            }
        }
    }

    val pkFieldsExpr = Expr.ofList(pkFieldExprs.toList)
    val tableNameExpr = Expr(tableName)

    '{
        $tableNameExpr -> $pkFieldsExpr
    }
}