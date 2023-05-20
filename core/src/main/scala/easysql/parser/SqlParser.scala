package easysql.parser

import easysql.ast.expr.*
import easysql.ast.order.*

import scala.util.parsing.combinator.*

class SqlParser extends JavaTokenParsers {
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
            "is" ~ opt("not") ~ "null" ^^ {
                case op ~ n ~ right => (n.isDefined, op, right)
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
                case (acc, (false, "is", _)) => SqlBinaryExpr(acc, SqlBinaryOperator.Is, SqlNullExpr)
                case (acc, (true, "is", _)) => SqlBinaryExpr(acc, SqlBinaryOperator.IsNot, SqlNullExpr)
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
        cast |
        function |
        aggFunction |
        ident ~ opt("." ~> (ident | "*")) ^^ {
            case id ~ None => SqlIdentExpr(id)
            case table ~ Some("*") => SqlAllColumnExpr(Some(table))
            case table ~ Some(column) => SqlPropertyExpr(table, column)
        } |
        "(" ~> expr <~ ")"

    def function: Parser[SqlExpr] =
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExprFuncExpr(funcName.toUpperCase.nn, args)
        }

    def aggFunction: Parser[SqlExpr] =
        "count" ~ "(" ~ "*" ~ ")" ^^ (_ => SqlAggFuncExpr("COUNT", SqlAllColumnExpr(None) :: Nil, false, Map(), Nil)) |
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlAggFuncExpr(funcName.toUpperCase.nn, args, false, Map(), Nil)
        } |
        ident ~ ("(" ~ "distinct" ~> expr <~ ")") ^^ {
            case funcName ~ arg =>  SqlAggFuncExpr(funcName.toUpperCase.nn, arg :: Nil, true, Map(), Nil)
        }

    def cast: Parser[SqlExpr] =
        "cast" ~> ("(" ~> expr ~ "as" ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => SqlCastExpr(expr, castType.toUpperCase.nn)
        }

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
        "false" ^^ (_ => SqlBooleanExpr(false)) |
        "null" ^^ (_ => SqlNullExpr)

    def charLiteral: Parser[String] =
        ("'" + """([^'\x00-\x1F\x7F\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r

    def parse(text: String): SqlExpr = {
        val parseResult = parseAll(expr, text)
        parseResult match {
            case Success(result, _) => result
            case Failure(_, next) => throw ParseException(next.pos.longString)
            case Error(_, next) => throw ParseException(next.pos.longString)
        }
    }
}

class ParseException(msg: String) extends Exception {
    override def toString: String = 
        s"easysql.parser.ParseException: \n$msg"
}