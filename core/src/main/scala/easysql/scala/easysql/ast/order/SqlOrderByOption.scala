package easysql.ast.order

enum SqlOrderByOption(val order: String) {
    case ASC extends SqlOrderByOption("ASC")
    case DESC extends SqlOrderByOption("DESC")

    def turn: SqlOrderByOption = if this == ASC then DESC else ASC
}