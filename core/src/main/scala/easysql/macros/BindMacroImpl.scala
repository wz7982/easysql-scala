package easysql.macros

import easysql.ast.SqlDataType
import easysql.dsl.CustomSerializer

import java.sql.ResultSet
import java.util.Date
import scala.quoted.*

def entityOffsetMacro[T](using q: Quotes, tpe: Type[T]): Expr[Int] = {
    import q.reflect.*

    val size = TypeTree.of[T].symbol.declaredFields.size

    Expr(size)
}

def bindEntityMacro[T](nextIndex: Expr[Int], data: Expr[Array[Any]])(using q: Quotes, tpe: Type[T]): Expr[Option[T]] = {
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val tpr = TypeRepr.of[T]
    val ctor = tpr.typeSymbol.primaryConstructor
    val fields = sym.declaredFields
    val fieldSize = Expr(fields.size)
    var i = 0

    val bindExprs = fields map { f => 
        f.tree match {
            case vd: ValDef => {
                val vdt = vd.tpt.tpe.asType
                vdt match {
                    case '[Option[t]] => {
                        val offset = Expr(i)
                        val expr = '{ 
                            if $data($nextIndex + $offset) == null then None else Some($data($nextIndex + $offset)).asInstanceOf[Option[t]] 
                        }

                        i = i + 1
                        expr
                    }
                    case '[t] => {
                        val args = f.annotations.map {
                            case Apply(TypeApply(Select(New(TypeIdent(name)), _), _), args) if name == "CustomColumn" => args
                            case _ => Nil
                        }.find {
                            case Nil => false 
                            case _ => true
                        }

                        val expr = args match {
                            case None => {
                                val offset = Expr(i)
                                '{ $data($nextIndex + $offset).asInstanceOf[t] }
                            }
                            case Some(value) => {
                                val offset = Expr(i)
                                val expr = value(1).asExprOf[CustomSerializer[t, _]]
                                '{ $expr.fromValue($data($nextIndex + $offset)) }
                            }
                        }

                        i = i + 1
                        expr
                    }
                }
            }
        }
    }

    '{
        val bindFunc = (result: Array[Any]) =>
            ${
                val terms = bindExprs.map(_.asTerm)
                New(Inferred(tpr)).select(ctor).appliedToArgs(terms).asExprOf[T]
            }

        val bind = (result: Array[Any]) => {
            val isNull = 
                result.slice($nextIndex, $nextIndex + $fieldSize).map(i => i == null).reduce((l, r) => l && r)
            if isNull then None else Some(bindFunc(result))
        }

        bind($data)
    }
}