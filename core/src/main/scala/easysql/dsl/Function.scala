package easysql.dsl

import easysql.ast.*

def count(): AggExpr[Long] =
    AggExpr("COUNT", Nil, false, Map(), Nil)

def count(expr: Expr[?]): AggExpr[Long] =
    AggExpr("COUNT", List(expr), false, Map(), Nil)

def countDistinct(expr: Expr[?]): AggExpr[Long] =
    AggExpr("COUNT", List(expr), true, Map(), Nil)

def sum[T <: SqlNumberType](expr: Expr[T]): AggExpr[BigDecimal] =
    AggExpr("SUM", List(expr), false, Map(), Nil)

def avg[T <: SqlNumberType](expr: Expr[T]): AggExpr[BigDecimal] =
    AggExpr("AVG", List(expr), false, Map(), Nil)

def max[T <: SqlDataType](expr: Expr[T]): AggExpr[T] =
    AggExpr("MAX", List(expr), false, Map(), Nil)

def min[T <: SqlDataType](expr: Expr[T]): AggExpr[T] =
    AggExpr("MIN", List(expr), false, Map(), Nil)

def rank(): AggExpr[Long] =
    AggExpr("RANK", List(), false, Map(), Nil)

def denseRank(): AggExpr[Long] =
    AggExpr("DENSE_RANK", List(), false, Map(), Nil)

def rowNumber(): AggExpr[Long] =
    AggExpr("ROW_NUMBER", List(), false, Map(), Nil)

def cube(expr: Expr[?]*): FuncExpr[Nothing] =
    FuncExpr("CUBE", expr.toList)

def rollup(expr: Expr[?]*): FuncExpr[Nothing] =
    FuncExpr("ROLLUP", expr.toList)

def groupingSets(expr: (Expr[?] | Tuple | Unit)*): FuncExpr[Nothing] = {
    val name = "GROUPING SETS"
    val args = expr.toList.map {
        case e: Expr[?] => ListExpr(List(e))
        case t: Tuple => {
            val list = t.toList
            val exprList: List[Expr[?]] = list.map {
                case v: SqlDataType => LiteralExpr(v)
                case expr: Expr[?] => expr
            }
            ListExpr(exprList)
        }
        case u: Unit => ListExpr(Nil)
    }
    FuncExpr(name, args)
}
