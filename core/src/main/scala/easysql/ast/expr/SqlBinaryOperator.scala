package easysql.ast.expr

enum SqlBinaryOperator(val operator: String) {
    case Is extends SqlBinaryOperator("IS")
    case IsNot extends SqlBinaryOperator("IS NOT")
    case Eq extends SqlBinaryOperator("=")
    case Ne extends SqlBinaryOperator("<>")
    case Like extends SqlBinaryOperator("LIKE")
    case NotLike extends SqlBinaryOperator("NOT LIKE")
    case Gt extends SqlBinaryOperator(">")
    case Ge extends SqlBinaryOperator(">=")
    case Lt extends SqlBinaryOperator("<")
    case Le extends SqlBinaryOperator("<=")
    case And extends SqlBinaryOperator("AND")
    case Or extends SqlBinaryOperator("OR")
    case Xor extends SqlBinaryOperator("XOR")
    case Add extends SqlBinaryOperator("+")
    case Sub extends SqlBinaryOperator("-")
    case Mul extends SqlBinaryOperator("*")
    case Div extends SqlBinaryOperator("/")
    case Mod extends SqlBinaryOperator("%")
    case Json extends SqlBinaryOperator("->")
    case JsonText extends SqlBinaryOperator("->>")
}