package org.easysql.macros

import org.easysql.database.TableEntity
import org.easysql.dsl.{PrimaryKeyColumnExpr, TableColumnExpr, col}
import org.easysql.query.update.Update
import org.easysql.util.anyToExpr

import java.sql.SQLException
import scala.quoted.{Expr, Quotes, Type}

def updateMacroImpl[T <: TableEntity[_]](update: Expr[Update], entity: Expr[T], skipNull: Expr[Boolean])(using quotes: Quotes, tpe: Type[T]): Expr[Update] = {
    import quotes.reflect.*

    val sym = TypeTree.of[T].symbol
    val names = sym.caseFields.map(it => it.name)
    val namesExpr = Expr.ofList(names.map(Expr(_)))

    val tree = entity.asTerm
    val fields = names.map(name => Select.unique(tree, name).asExpr)
    val fieldExprs = Expr.ofList(fields)

    val comp = sym.companionClass
    val body = comp.tree.asInstanceOf[ClassDef].body
    val typeName = Expr(sym.name)

    val idents =
        for case deff @ ValDef(name, _, _) <- body
            yield name -> Ref(deff.symbol)

    val tableName = idents.find(ident => ident._1 == "tableName").map(ident => ident._2.asExprOf[String]).get

    val compNamesExpr = Expr.ofList(idents.map(_._1).map(Expr(_)))
    val identsExpr: Expr[List[Any]] = Expr.ofList(idents.map(_._2.asExpr))

    '{
        $update.update($tableName)

        val values = $namesExpr.zip($fieldExprs).toMap
        val columnExprs = $compNamesExpr.zip($identsExpr).toMap

        var columns = columnExprs
            .filter(it => it._2.isInstanceOf[TableColumnExpr[_, _]])
            .map(it => it._1 -> it._2.asInstanceOf[TableColumnExpr[_, _]])
            .toMap

        val pkCols = columnExprs
            .filter(it => it._2.isInstanceOf[PrimaryKeyColumnExpr[_, _]])
            .map(it => it._1 -> it._2.asInstanceOf[PrimaryKeyColumnExpr[_, _]])
            .map(it => it._2 -> values(it._1))

        if (pkCols.isEmpty) {
            throw SQLException(s"实体类${$typeName}的伴生对象中没有设置主键字段")
        }

        pkCols.foreach { it =>
            if (it._2 == null || it._2 == None) {
                throw SQLException("主键字段的值为空")
            } else {
                $update.where(it._1.equal(it._2))
            }
        }

        if ($skipNull) {
            columns = columns.filter(it => values(it._1) != null && values(it._1) != None)
        }

        if (columns.isEmpty) {
            throw SQLException("更新的字段列表为空")
        }

        columns
            .map(it => it._2 -> anyToExpr(values(it._1)))
            .toSeq
            .foreach($update.set(_))

        $update
    }
}
