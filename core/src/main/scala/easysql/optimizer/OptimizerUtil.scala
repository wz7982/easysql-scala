package easysql.optimizer

import easysql.ast.expr.*
import easysql.optimizer.rules.*
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.query.select.Query
import easysql.database.DB
import easysql.util.queryToString

def createStandardOptimizer: SqlOptimizer = {
    val removeAggSubQueryClause = new RemoveAggSubQueryClause
    val removeSubQueryPredicateClause = new RemoveSubQueryPredicateClause
    val liftSpjSubQuery = new LiftSpjSubQuery
    val simplifyHaving = new SimplifyHaving
    val reduceGroupByKey = new ReduceGroupByKey
    val evalConst = new EvalConst
    val inferenceEquivalenceClass = new InferenceEquivalenceClass
    val extractCommonCondition = new ExtractCommonCondition
    val transferConst = new TransferConst
    val reduceColumn = new ReduceColumn
    val removeRightJoin = new RemoveRightJoin
    val removeOuterJoin = new RemoveOuterJoin
    val removeUselessJoinTerm = new RemoveUselessJoinTerm

    val rules = List(
        removeAggSubQueryClause, 
        removeSubQueryPredicateClause,
        liftSpjSubQuery, 
        simplifyHaving,
        reduceGroupByKey,
        evalConst,
        inferenceEquivalenceClass,
        extractCommonCondition,
        transferConst,
        reduceColumn,
        removeRightJoin,
        removeOuterJoin,
        removeUselessJoinTerm
    )

    new SqlOptimizer(rules)
}

extension (q: Query[_, _]) {
    def toOptimizedSql(using db: DB): String = {
        val ast = q.getAst
        val optimizedAst = createStandardOptimizer.optimize(ast)
        queryToString(optimizedAst, db, false)._1
    }
}

def isLiteral(expr: SqlExpr): Boolean = expr match {
    case _: SqlNumberExpr => true
    case _: SqlCharExpr => true
    case _: SqlBooleanExpr => true
    case _: SqlDateExpr => true
    case SqlNullExpr => true
    case _ => false
}

def hasAgg(expr: SqlExpr): Boolean = 
    if isLiteral(expr) 
    then false 
    else expr match {
        case SqlAggFuncExpr(_, _, _, _, _) => true
        case SqlBinaryExpr(left, _, right) => hasAgg(left) || hasAgg(right)
        case _ => false
    }
    
// todo select中不能包含聚合函数
def isSpjQuery(query: SqlQuery): Boolean = query match {
    case SqlSelect(param, select, from, where, groupBy, orderBy, forUpdate, limit, having) => {
        def isSpjFrom(table: SqlTable): Boolean = table match {
            case SqlIdentTable(_, _) => true
            case SqlJoinTable(left, _, right, _) => isSpjFrom(left) && isSpjFrom(right)
            case SqlSubQueryTable(_, _, _) =>  false
        }

        param.isEmpty 
            && from.map(isSpjFrom).getOrElse(true) 
            && groupBy.isEmpty 
            && orderBy.isEmpty 
            && !forUpdate 
            && limit.isEmpty 
            && having.isEmpty
    }
    case SqlUnion(_, _, _) => false
    case SqlValues(_) => false
    case SqlWith(_, _, _) => false
}

def isStrict(expr: SqlBinaryExpr): Boolean = expr match {
    case SqlBinaryExpr(_, SqlBinaryOperator.IsNot, SqlNullExpr) => true
    case SqlBinaryExpr(_, SqlBinaryOperator.Eq, right) =>
        isLiteral(right)
    case _ => false
}

def conjunctiveTermList(expr: SqlExpr): List[SqlExpr] = expr match {
    case SqlBinaryExpr(left, SqlBinaryOperator.And, right) => 
        conjunctiveTermList(left) ++ conjunctiveTermList(right)
    case _ =>
        expr :: Nil
}