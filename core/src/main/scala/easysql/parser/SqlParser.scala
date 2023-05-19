package easysql.parser

import easysql.ast.expr.*

import scala.util.parsing.combinator.*
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import easysql.printer.MysqlPrinter

class SqlParser extends JavaTokenParsers {
    // todo 函数、聚合函数、窗口函数、is null

    def expr: Parser[SqlExpr] = 
        or

    def or: Parser[SqlExpr] =
        xor * ("or" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Or, r) })

    def xor: Parser[SqlExpr] =
        and * ("xor" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Xor, r) })

    def and: Parser[SqlExpr] =
        relation * ("and" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.And, r) })

    def relation: Parser[SqlExpr] =
        add ~ rep(
            ("=" | "<>" | "!=" | ">" | ">=" | "<" | "<=") ~ add ^^ { 
                case op ~ right => (op, right)
            } |
            opt("not") ~ "between" ~ add ~ "and" ~ add ^^ { 
                case n ~ op ~ start ~ _ ~ end => (n.isDefined, op, start, end)
            } |
            opt("not") ~ "in" ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ { 
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, op, in)
            } |
            opt("not") ~ "like" ~ add ^^ {
                case n ~ op ~ right => (n.isDefined, op, right)
            }
        ) ^^ { 
            case left ~ elems => elems.foldLeft(left) {
                case (acc, ("=", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Eq, right)
                case (acc, ("<>", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Ne, right)
                case (acc, ("!=", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Ne, right)
                case (acc, (">", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Gt, right)
                case (acc, (">=", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Ge, right)
                case (acc, ("<", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Lt, right)
                case (acc, ("<=", right)) => SqlBinaryExpr(acc, SqlBinaryOperator.Le, right)
                case (acc, (not: Boolean, "between", l: SqlExpr, r: SqlExpr)) => SqlBetweenExpr(acc, l, r, not)
                case (acc, (not: Boolean, "in", in: List[_])) => SqlInExpr(acc, SqlListExpr(in.asInstanceOf[List[SqlExpr]]), not)
                case (acc, (false, "like", expr: SqlExpr)) => SqlBinaryExpr(acc, SqlBinaryOperator.Like, expr)
                case (acc, (true, "like", expr: SqlExpr)) => SqlBinaryExpr(acc, SqlBinaryOperator.NotLike, expr)
                case _ => SqlNullExpr
            }
        }

    def add: Parser[SqlExpr] =
        mul * (
            "+" ^^^ ((a, b) => SqlBinaryExpr(a, SqlBinaryOperator.Add, b)) |
            "-" ^^^ ((a, b) => SqlBinaryExpr(a, SqlBinaryOperator.Sub, b))
        )

    def mul: Parser[SqlExpr] =
        primary * (
            "*" ^^^ ((a, b) => SqlBinaryExpr(a, SqlBinaryOperator.Mul, b)) |
            "/" ^^^ ((a, b) => SqlBinaryExpr(a, SqlBinaryOperator.Div, b)) |
            "%" ^^^ ((a, b) => SqlBinaryExpr(a, SqlBinaryOperator.Mod, b))
        )

    def primary : Parser[SqlExpr] =
        literal |
        caseWhen |
        ident ~ opt("." ~> ident | "(" ~> repsep(expr, ",") <~ ")") ^^ {
            case id ~ None => SqlIdentExpr(id)
            case table ~ Some(column: String) => SqlPropertyExpr(table, column)
            case id ~ Some(args: List[_]) => SqlExprFuncExpr(id, args.asInstanceOf[List[SqlExpr]])
            case _ ~ _ => SqlNullExpr
        } |
        "(" ~> expr <~ ")"

    def caseWhen: Parser[SqlExpr] = 
        "case" ~>
            rep1("when" ~> expr ~ "then" ~ expr ^^ { case e ~ _ ~ te => SqlCase(e, te) }) ~ 
            opt("else" ~> expr) <~ "end" ^^ {
                case branches ~ default => SqlCaseExpr(branches, default.getOrElse(SqlNullExpr))
            }
    
    def literal: Parser[SqlExpr] = 
        decimalNumber ^^ (i => SqlNumberExpr(BigDecimal(i))) |
        charLiteral ^^ (xs => SqlCharExpr(xs.toString.substring(1, xs.size -1).nn)) |
        "true" ^^ (_ => SqlBooleanExpr(true)) |
        "false" ^^ (_ => SqlBooleanExpr(false))

    def charLiteral: Parser[String] =
        ("'" + """([^'\x00-\x1F\x7F\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r

    def parse(text: String): SqlExpr = {
        val parseResult = parseAll(expr, text)
        parseResult match {
            case Success(result, next) => result
            case Error(msg, next) => throw ParseException("\n" + next.pos.longString)
            case Failure(msg, next) => throw ParseException("\n" + next.pos.longString)
        }
    }
}

class ParseException(msg: String) extends Exception {
    override def toString: String = 
        s"easysql.parser.ParseException: \n$msg"
}