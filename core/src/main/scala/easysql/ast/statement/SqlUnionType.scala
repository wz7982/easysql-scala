package easysql.ast.statement

enum SqlUnionType(val unionType: String) {
    case Union extends SqlUnionType("UNION")
    case UnionAll extends SqlUnionType("UNION ALL")
    case Except extends SqlUnionType("EXCEPT")
    case ExceptAll extends SqlUnionType("EXCEPT ALL")
    case Intersect extends SqlUnionType("INTERSECT")
    case IntersectAll extends SqlUnionType("INTERSECT ALL")
}