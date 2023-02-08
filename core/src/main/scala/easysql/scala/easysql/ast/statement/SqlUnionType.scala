package easysql.ast.statement

enum SqlUnionType(val unionType: String) {
    case UNION extends SqlUnionType("UNION")
    case UNION_ALL extends SqlUnionType("UNION ALL")
    case EXCEPT extends SqlUnionType("EXCEPT")
    case EXCEPT_ALL extends SqlUnionType("EXCEPT ALL")
    case INTERSECT extends SqlUnionType("INTERSECT")
    case INTERSECT_ALL extends SqlUnionType("INTERSECT ALL")
}