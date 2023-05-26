package easysql.ast.expr

enum SqlOverBetweenType(val show: String) {
    case CurrentRow extends SqlOverBetweenType("CURRENT ROW")
    case UnboundedPreceding extends SqlOverBetweenType("UNBOUNDED PRECEDING")
    case Preceding(n: SqlNumberExpr) extends SqlOverBetweenType(s"PRECEDING")
    case UnboundedFollowing extends SqlOverBetweenType("UNBOUNDED FOLLOWING")
    case Following(n: SqlNumberExpr) extends SqlOverBetweenType("FOLLOWING")
}

enum SqlOverBetween(val start: SqlOverBetweenType, val end: SqlOverBetweenType) {
    case Rows(override val start: SqlOverBetweenType, override val end: SqlOverBetweenType) extends SqlOverBetween(start, end)
    case Range(override val start: SqlOverBetweenType, override val end: SqlOverBetweenType) extends SqlOverBetween(start, end)
}