package easysql.ast.expr

enum SqlIntervalUnit(val unit: String) {
    case Year extends SqlIntervalUnit("YEAR")
    case Month extends SqlIntervalUnit("MONTH")
    case Week extends SqlIntervalUnit("WEEK")
    case Day extends SqlIntervalUnit("DAY")
    case Hour extends SqlIntervalUnit("HOUR")
    case Minute extends SqlIntervalUnit("MINUTE")
    case Second extends SqlIntervalUnit("SECOND")
}