package org.easysql.dsl

import org.easysql.ast.SqlSingleConstType
import org.easysql.query.select.SelectQuery

def count() = AggFunctionExpr[Int, NothingTable]("COUNT", List())

def count[Table <: TableSchema | Tuple](query: Expr[_, Table]) = AggFunctionExpr[Int, Table]("COUNT", List(query))

def countDistinct[Table <: TableSchema | Tuple](query: Expr[_, Table]) = AggFunctionExpr[Int, Table]("COUNT", List(query), true)

def sum[T <: Int | Long | Float | Double | Null, Table <: TableSchema | Tuple](query: Expr[T, Table]) = AggFunctionExpr[T, Table]("SUM", List(query))

def avg[T <: Int | Long | Float | Double | Null, Table <: TableSchema | Tuple](query: Expr[T, Table]) = AggFunctionExpr[T, Table]("AVG", List(query))

def max[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](query: Expr[T, Table]) = AggFunctionExpr[T, Table]("MAX", List(query))

def min[T <: SqlSingleConstType | Null, Table <: TableSchema | Tuple](query: Expr[T, Table]) = AggFunctionExpr[T, Table]("MIN", List(query))

def rank() = AggFunctionExpr[Int, NothingTable]("RANK", List())

def denseRank() = AggFunctionExpr[Int, NothingTable]("DENSE_RANK", List())

def rowNumber() = AggFunctionExpr[Int, NothingTable]("ROW_NUMBER", List())

def cube(expr: Expr[_, _]*) = NormalFunctionExpr("CUBE", expr.toList)

def rollup(expr: Expr[_, _]*) = NormalFunctionExpr("ROLLUP", expr.toList)

def groupingSets(expr: (Expr[_, _] | Tuple)*) = {
    val name = "GROUPING SETS"
    val args = expr.toList.map {
        case e: Expr[_, _] => ListExpr(List(e))
        case t: Tuple => {
            val list = t.toList
            val exprList: List[Expr[_, _]] = list.map {
                case v: SqlSingleConstType => const(v)
                case expr: Expr[_, _] => expr
                case _ => const(null)
            }
            ListExpr(exprList)
        }
    }
    NormalFunctionExpr(name, args)
}