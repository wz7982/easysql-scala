package easysql.ast.order

enum SqlOrderByOption(val order: String) {
    case Asc extends SqlOrderByOption("ASC")
    case Desc extends SqlOrderByOption("DESC")

    def turn: SqlOrderByOption = if this == Asc then Desc else Asc
}