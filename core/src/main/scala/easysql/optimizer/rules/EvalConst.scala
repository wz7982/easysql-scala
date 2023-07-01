package easysql.optimizer.rules

import easysql.optimizer.OptimizeRule
import easysql.ast.statement.*
import easysql.ast.expr.*
import easysql.ast.order.SqlOrderBy
import easysql.ast.table.*

class EvalConst extends OptimizeRule {
    override def optimize(query: SqlQuery): SqlQuery = query match {
        case SqlSelect(param, select, from, where, groupBy, orderBy, forUpdate, limit, having) => {
            val optimizedSelect = select.map(i => SqlSelectItem(evalConst(i.expr), i.alias))
            def optimizeFrom(table: SqlTable): SqlTable = table match {
                case t @ SqlIdentTable(_, _) => 
                    t
                case SqlSubQueryTable(query, lateral, alias) =>
                    SqlSubQueryTable(optimize(query), lateral, alias)
                case SqlJoinTable(left, joinType, right, on) =>
                    SqlJoinTable(optimizeFrom(left), joinType, optimizeFrom(right), on.map(evalConst))
            }
            val optimizedFrom = from.map(optimizeFrom)
            val optimizedWhere = where.map(evalConst)
            val optimizedGroupBy = groupBy.map(evalConst)
            val optimizedOrderBy = orderBy.map(o => SqlOrderBy(evalConst(o.expr), o.order))
            val optimizedHaving = having.map(evalConst)
            SqlSelect(param, optimizedSelect, optimizedFrom, optimizedWhere, optimizedGroupBy, optimizedOrderBy, forUpdate, limit, optimizedHaving)
        }
        case SqlUnion(left, unionType, right) =>
            SqlUnion(optimize(left), unionType, optimize(right))
        case SqlWith(withList, recursive, finalQuery) =>
            SqlWith(withList.map(w => SqlWithItem(w.name, optimize(w.query), w.columns)), recursive, finalQuery.map(optimize))
        case SqlValues(values) => 
            SqlValues(values)
    }

    def eval(left: Number, op: SqlBinaryOperator, right: Number): SqlExpr = {
        val leftValue = BigDecimal(left.toString)
        val rightValue = BigDecimal(right.toString)
        op match {
            case SqlBinaryOperator.Add => SqlNumberExpr(leftValue + rightValue)
            case SqlBinaryOperator.Sub => SqlNumberExpr(leftValue - rightValue)
            case SqlBinaryOperator.Mul => SqlNumberExpr(leftValue * rightValue)
            case SqlBinaryOperator.Div => SqlNumberExpr(leftValue / rightValue)
            case SqlBinaryOperator.Mod => SqlNumberExpr(leftValue % rightValue)
            case SqlBinaryOperator.Eq => SqlBooleanExpr(leftValue == rightValue)
            case SqlBinaryOperator.Ne => SqlBooleanExpr(leftValue != rightValue)
            case SqlBinaryOperator.Gt => SqlBooleanExpr(leftValue > rightValue)
            case SqlBinaryOperator.Ge => SqlBooleanExpr(leftValue >= rightValue)
            case SqlBinaryOperator.Lt => SqlBooleanExpr(leftValue < rightValue)
            case SqlBinaryOperator.Le => SqlBooleanExpr(leftValue <= rightValue)
            case _ => SqlBinaryExpr(SqlNumberExpr(left), op, SqlNumberExpr(right))
        }
    }

    def evalNumberConst(expr: SqlExpr): SqlExpr = expr match {
        case SqlBinaryExpr(SqlNumberExpr(l), op, SqlNumberExpr(r)) =>
            eval(l, op, r)
        case SqlBinaryExpr(left, op, right) => {
            val evalLeft = evalNumberConst(left)
            val evalRight = evalNumberConst(right)
            (evalLeft, evalRight) match {
                case (SqlNumberExpr(l), SqlNumberExpr(r)) =>
                    evalNumberConst(SqlBinaryExpr(evalLeft, op, evalRight))
                case _ =>
                    SqlBinaryExpr(evalLeft, op, evalRight)
            }
        }
        case SqlExprFuncExpr(name, args) => 
            SqlExprFuncExpr(name, args.map(evalNumberConst))
        case SqlAggFuncExpr(name, args, distinct, attrs, orderBy) =>
            SqlAggFuncExpr(name, args.map(evalNumberConst), distinct, attrs, orderBy)
        case SqlBetweenExpr(expr, start, end, not) =>
            SqlBetweenExpr(evalNumberConst(expr), evalNumberConst(start), evalNumberConst(end), not)
        case SqlInExpr(expr, inExpr, not) =>
            SqlInExpr(evalNumberConst(expr), evalNumberConst(inExpr), not)
        case SqlListExpr(items) => 
            SqlListExpr(items.map(evalNumberConst))
        case SqlOverExpr(agg, partitionBy, orderBy, between) =>
            SqlOverExpr(evalNumberConst(agg).asInstanceOf[SqlAggFuncExpr], partitionBy, orderBy, between)
        case SqlCastExpr(expr, castType) =>
            SqlCastExpr(evalNumberConst(expr), castType)
        case SqlCaseExpr(caseList, default) =>
            SqlCaseExpr(caseList.map(c => SqlCase(evalNumberConst(c.expr), evalNumberConst(c.thenExpr))), evalNumberConst(default))
        case SqlQueryExpr(query) => 
            SqlQueryExpr(optimize(query))
        case _ => expr
    }

    def evalBoolConst(expr: SqlExpr): SqlExpr = expr match {
        case SqlBinaryExpr(SqlBooleanExpr(true), SqlBinaryOperator.And, right) =>
            evalBoolConst(right)
        case SqlBinaryExpr(left, SqlBinaryOperator.And, SqlBooleanExpr(true)) =>
            evalBoolConst(left)
        case SqlBinaryExpr(SqlBooleanExpr(false), SqlBinaryOperator.And, _) =>
            SqlBooleanExpr(false)
        case SqlBinaryExpr(_, SqlBinaryOperator.And, SqlBooleanExpr(false)) =>
            SqlBooleanExpr(false)
        case SqlBinaryExpr(SqlBooleanExpr(true), SqlBinaryOperator.Or, _) =>
            SqlBooleanExpr(true)
        case SqlBinaryExpr(_, SqlBinaryOperator.Or, SqlBooleanExpr(true)) =>
            SqlBooleanExpr(true)
        case SqlBinaryExpr(SqlBooleanExpr(false), SqlBinaryOperator.Or, right) =>
            evalBoolConst(right)
        case SqlBinaryExpr(left, SqlBinaryOperator.Or, SqlBooleanExpr(false)) =>
            evalBoolConst(left)
        case SqlBinaryExpr(left, op, right) => {
            val evalLeft = evalBoolConst(left)
            val evalRight = evalBoolConst(right)
            (evalLeft, evalRight) match {
                case (SqlBooleanExpr(_), SqlBooleanExpr(_)) =>
                    evalBoolConst(SqlBinaryExpr(evalLeft, op, evalRight))
                case (SqlBooleanExpr(_), _) =>
                    evalBoolConst(SqlBinaryExpr(evalLeft, op, evalRight))
                case (_, SqlBooleanExpr(_)) =>
                    evalBoolConst(SqlBinaryExpr(evalLeft, op, evalRight))
                case _ =>
                    SqlBinaryExpr(evalLeft, op, evalRight)
            }
        }
        case SqlExprFuncExpr(name, args) => 
            SqlExprFuncExpr(name, args.map(evalBoolConst))
        case _ =>
            expr
    }

    def evalConst(expr: SqlExpr): SqlExpr = {
        val evalExpr = evalNumberConst(expr)
        evalBoolConst(evalExpr)
    }
}