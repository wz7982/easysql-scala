package easysql.ast.expr

enum SqlBinaryOperator(val operator: String) {
    case IS extends SqlBinaryOperator("IS")
    case IS_NOT extends SqlBinaryOperator("IS NOT")
    case EQ extends SqlBinaryOperator("=")
    case NE extends SqlBinaryOperator("<>")
    case LIKE extends SqlBinaryOperator("LIKE")
    case NOT_LIKE extends SqlBinaryOperator("NOT LIKE")
    case GT extends SqlBinaryOperator(">")
    case GE extends SqlBinaryOperator(">=")
    case LT extends SqlBinaryOperator("<")
    case LE extends SqlBinaryOperator("<=")
    case AND extends SqlBinaryOperator("AND")
    case OR extends SqlBinaryOperator("OR")
    case XOR extends SqlBinaryOperator("XOR")
    case ADD extends SqlBinaryOperator("+")
    case SUB extends SqlBinaryOperator("-")
    case MUL extends SqlBinaryOperator("*")
    case DIV extends SqlBinaryOperator("/")
    case MOD extends SqlBinaryOperator("%")
    case JSON extends SqlBinaryOperator("->")
    case JSON_TEXT extends SqlBinaryOperator("->>")
}