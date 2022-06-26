package org.easysql.macros

import org.easysql.database.TableEntity
import org.easysql.dsl.PrimaryKeyColumnExpr
import org.easysql.query.select.Select

import java.sql.SQLException
import scala.quoted.{Expr, Quotes, Type}

def findMacroImpl[T <: TableEntity[_]](select: Expr[Select[_, _]], primaryKey: Expr[Any])(using quotes: Quotes, tpe: Type[T]): Expr[Select[_, _]] = {
    import quotes.reflect.*

    val sym = TypeTree.of[T].symbol
    val comp = sym.companionClass
    val body = comp.tree.asInstanceOf[ClassDef].body
    val typeName = Expr(sym.name)

    val idents =
        for case deff @ ValDef(name, _, _) <- body
            yield name -> Ref(deff.symbol)

    val tableName = idents.find(ident => ident._1 == "tableName").map(ident => ident._2.asExprOf[String]).get

    val identsExpr: Expr[List[Any]] = Expr.ofList(idents.map(_._2.asExpr))

    '{
        val pkCols = $identsExpr
            .filter(it => it.isInstanceOf[PrimaryKeyColumnExpr[_, _]])
            .map(it => it.asInstanceOf[PrimaryKeyColumnExpr[_, _]])

        if (pkCols.isEmpty) {
            throw SQLException(s"实体类${$typeName}伴生对象中未定义主键字段")
        }

        val sel = $select.from(${tableName})

        if ($primaryKey.isInstanceOf[Tuple]) {
            val pkValues = $primaryKey.asInstanceOf[Tuple].productIterator.toList
            if (pkValues.size != pkCols.size) {
                throw SQLException(s"实体类${$typeName}中定义的主键字段数目与伴生对象不相符")
            }
            for (i <- pkValues.indices) {
                sel.unsafeWhere(pkCols(i).equal(pkValues(i)))
            }
            sel
        } else {
            val pkCol = pkCols.head

            sel.unsafeWhere(pkCol.equal($primaryKey))
        }
    }
}
