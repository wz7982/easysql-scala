package easysql.ast.table

enum SqlJoinType(val joinType: String) {
    case JOIN extends SqlJoinType("JOIN")
    case INNER_JOIN extends SqlJoinType("INNER JOIN")
    case LEFT_JOIN extends SqlJoinType("LEFT JOIN")
    case RIGHT_JOIN extends SqlJoinType("RIGHT JOIN")
    case FULL_JOIN extends SqlJoinType("FULL JOIN")
    case CROSS_JOIN extends SqlJoinType("CROSS JOIN")
}