package easysql.macros

import easysql.ast.SqlDataType
import easysql.dsl.CustomSerializer
import easysql.util.*

import scala.annotation.experimental
import scala.collection.mutable.*
import scala.quoted.{Expr, Quotes, Type}

def insertMetaDataMacro[T <: Product](entity: Expr[T])(using q: Quotes, tpe: Type[T]): Expr[List[(String, Any)]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields
    val names = ListBuffer[Expr[String]]()
    val values = ListBuffer[Expr[Any]]()

    val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")

    val entityTerm = entity.asTerm

    fields foreach { field =>
        val annoInfo = field.annotations.map {
            case Apply(Select(New(TypeIdent(name)), _), args) if annoNames.contains(name) => {
                args match {
                    case Literal(v) :: arg => (name, v.value.toString(), args)
                    case _ => (name, "", args)
                }
            }
            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), args) if name == "CustomColumn" => {
                args match {
                    case Literal(v) :: arg => (name, v.value.toString(), args)
                    case _ => (name, "", args)
                }
            }
            case _ => ("", "", Nil)
        }
        
        val insertName = annoInfo.find(_._2 != "") match {
            case None => camelToSnake(field.name)
            case Some(_, value, _) => value
        }

        def generatorTerm(statement: Statement): Term = statement match {
            case DefDef(_, _, _, t) => t.get
        }

        def customTerm(statement: Statement): Term = {
            val fieldTerm = Select.unique(entityTerm, field.name)
            field.tree match {
                case vd: ValDef => {
                    val vdt = vd.tpt.tpe.asType
                    vdt match {
                        case '[t] => {
                            val expr = statement.asExprOf[CustomSerializer[t, _]]
                            val fieldExpr = fieldTerm.asExprOf[t]
                            '{ $expr.toValue($fieldExpr) }.asTerm
                        }
                    }
                }
            }
        }

        val term = annoInfo.find(_._1 != "") match {
            case Some("PrimaryKeyGenerator", _, args) => args match {
                case _ :: NamedArg(_, Block(l, _)) :: _ => generatorTerm(l.head)
                case _ :: Block(l, _) :: _ => generatorTerm(l.head)
                case _ => Select.unique(entityTerm, field.name)
            }
            case Some("CustomColumn", _, args) => customTerm(args(1))
            case _ => Select.unique(entityTerm, field.name)
        }

        annoInfo.find(_._1 == "IncrKey") match {
            case None => {
                names.addOne(Expr(insertName))
                values.addOne(term.asExpr)
            }
            case _ =>
        }
    }

    if (names.isEmpty) {
        report.error(s"entity ${sym.name} has no field for inserting data")
    }

    val namesExpr = Expr.ofList(names.toList)
    val valuesExpr = Expr.ofList(values.toList)

    '{
        $namesExpr.zip($valuesExpr)
    }
}

def updateMetaDataMacro[T <: Product](using q: Quotes, tpe: Type[T]): Expr[(String, List[(String, T => SqlDataType)], List[(String, T => SqlDataType | Option[SqlDataType])])] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val pkFieldExprs = ListBuffer[Expr[String]]()
    val pkFunctionExprs = ListBuffer[Expr[T => SqlDataType]]()
    val updateFieldExprs = ListBuffer[Expr[String]]()
    val updateFunctionExprs = ListBuffer[Expr[T => SqlDataType | Option[SqlDataType]]]()

    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(name)), _), Literal(v) :: Nil) if name == "Table" => v.value.toString()
        case _ => ""
    }.find(_ != "") match {
        case None => camelToSnake(sym.name)
        case Some(value) => value
    }

    val fields = sym.declaredFields

    val annoNames = List("PrimaryKey", "IncrKey", "Column", "PrimaryKeyGenerator", "CustomColumn")

    fields.foreach { field =>
        val annoInfo = field.annotations.map {
            case Apply(Select(New(TypeIdent(name)), _), args) if annoNames.contains(name) =>
                args match {
                    case Literal(v) :: _ => (name, v.value.toString(), Nil)
                    case _ => (name, "", Nil)
                }

            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), args) if name == "CustomColumn" => {
                args match {
                    case Literal(v) :: _ => (name, v.value.toString(), args)
                    case _ => (name, "", Nil)
                }
            }
            case _ => ("", "", Nil)
        }

        val fieldName = annoInfo.find(_._2 != "") match {
            case None => camelToSnake(field.name)
            case Some(_, value, _) => value
        }

        val mtpePk = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[SqlDataType])
        def rhsFnPk(sym: Symbol, paramRefs: List[Tree]) = {
            val x = paramRefs.head.asExprOf[T].asTerm
            Select.unique(x, field.name)
        }
        val lambdaPk = Lambda(field, mtpePk, rhsFnPk).asExprOf[T => SqlDataType]

        val mtpeUpdate = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[SqlDataType | Option[SqlDataType]])
        def rhsFnUpdate(sym: Symbol, paramRefs: List[Tree]) = {
            val x = paramRefs.head.asExprOf[T].asTerm
            Select.unique(x, field.name)
        }
        val lambdaUpdate = Lambda(field, mtpeUpdate, rhsFnUpdate).asExprOf[T => SqlDataType | Option[SqlDataType]]

        def lambdaCustomUpdate(s: Statement) = {
            val mtpe = MethodType(List("x"))(_ => List(TypeRepr.of[T]), _ => TypeRepr.of[SqlDataType | Option[SqlDataType]])
            def rhsFn(sym: Symbol, paramRefs: List[Tree]): Tree = {
                val x = paramRefs.head.asExprOf[T].asTerm
                val fieldTerm = Select.unique(x, field.name)
                field.tree match {
                    case vd: ValDef => {
                        val vdt = vd.tpt.tpe.asType
                        vdt match {
                            case '[t] => {
                                val expr = s.asExprOf[CustomSerializer[t, _]]
                                val fieldExpr = fieldTerm.asExprOf[t]
                                '{ $expr.toValue($fieldExpr) }.asTerm
                            }
                        }
                    }
                }
            }
            Lambda(field, mtpe, rhsFn).asExprOf[T => SqlDataType | Option[SqlDataType]]
        }

        annoInfo.find {
            case (name, _, _) if name == "PrimaryKey" || name == "IncrKey" || name == "PrimaryKeyGenerator" || name == "CustomColumn" => true
            case _ => false
        } match {
            case None => {
                updateFieldExprs.addOne(Expr(fieldName))
                updateFunctionExprs.addOne(lambdaUpdate)
            }
            case Some(name, _, args) => {
                name match {
                    case "CustomColumn" => {
                        updateFieldExprs.addOne(Expr(fieldName))
                        updateFunctionExprs.addOne(lambdaCustomUpdate(args(1)))
                    }
                    case _ => {
                        pkFieldExprs.addOne(Expr(fieldName))
                        pkFunctionExprs.addOne(lambdaPk)
                    }
                }
            }
        }
    }

    if (pkFieldExprs.isEmpty) {
        report.error(s"primary key field is not defined in entity ${sym.name}")
    }

    if (updateFieldExprs.isEmpty) {
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

def fetchPkMacro[T <: Product, PK <: SqlDataType | Tuple](using q: Quotes, t: Type[T], p: Type[PK]): Expr[(String, List[String])] = {
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
    val argTypeNames = ListBuffer[String]()
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
            case Apply(Select(New(TypeIdent(name)), _), _) if name == "PrimaryKey" || name == "IncrKey" || name == "PrimaryKeyGenerator" => true
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

    if (pkFieldExprs.isEmpty) {
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