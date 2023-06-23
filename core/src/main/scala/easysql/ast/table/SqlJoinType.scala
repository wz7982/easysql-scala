package easysql.ast.table

enum SqlJoinType(val joinType: String) {
    case Join extends SqlJoinType("JOIN")
    case InnerJoin extends SqlJoinType("INNER JOIN")
    case LeftJoin extends SqlJoinType("LEFT JOIN")
    case RightJoin extends SqlJoinType("RIGHT JOIN")
    case FullJoin extends SqlJoinType("FULL JOIN")
    case CrossJoin extends SqlJoinType("CROSS JOIN")
    case SemiJoin extends SqlJoinType("SEMI JOIN")
    case AntiJoin extends SqlJoinType("ANTI JOIN")
}