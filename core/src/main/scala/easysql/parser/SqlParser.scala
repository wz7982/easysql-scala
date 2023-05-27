package easysql.parser

import easysql.ast.expr.*
import easysql.ast.order.*

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.util.matching.Regex

class SqlParser extends StandardTokenParsers {
    class SqlLexical extends StdLexical {
        override def token: Parser[Token] = (
            identChar ~ rep(identChar | digit) ^^ { 
                case first ~ rest => processIdent((first :: rest).mkString("")) 
            } |
            '\"' ~> (identChar ~ rep(identChar | digit)) <~ '\"' ^^ { 
                case first ~ rest => processIdent((first :: rest).mkString("")) 
            } |
            '`' ~> (identChar ~ rep(identChar | digit)) <~ '`' ^^ { 
                case first ~ rest => processIdent((first :: rest).mkString("")) 
            } |
            rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
                case i ~ None => NumericLit(i mkString "")
                case i ~ Some(d) => NumericLit(i.mkString("") + "." + d.mkString(""))
            } |
            '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { 
                case _ ~ chars ~ _ => StringLit(chars.mkString("")) 
            } |
            EofCh ^^^ EOF |
            delim |
            failure("illegal character")
        )
    }

    override val lexical: SqlLexical = new SqlLexical()

    lexical.reserved += (
        "CAST", "AS", "AND", "XOR", "OR", "OVER", "BY", "PARTITION", "ORDER", "DISTINCT", "NOT",
        "CASE", "WHEN", "THEN", "ELSE", "END", "ASC", "DESC", "TRUE", "FALSE", "NULL",
        "BETWEEN", "IN", "LIKE", "IS",
        "cast", "as", "and", "xor", "or", "over", "by", "partition", "order", "distinct", "not",
        "case", "when", "then", "else", "end", "asc", "desc", "true", "false", "null",
        "between", "in", "like", "is"
    )

    lexical.delimiters += (
        "+", "-", "*", "/", "%", "=", "<>", "!=", ">", ">=", "<", "<=", "(", ")", ",", ".", "`", "\""
    )

    def expr: Parser[SqlExpr] = 
        or

    def or: Parser[SqlExpr] =
        xor * (("or" | "OR") ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Or, r) })

    def xor: Parser[SqlExpr] =
        and * (("xor" | "XOR") ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Xor, r) })

    def and: Parser[SqlExpr] =
        relation * (("and" | "AND") ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.And, r) })

    def relation: Parser[SqlExpr] =
        add ~ rep(
            ("=" | "<>" | "!=" | ">" | ">=" | "<" | "<=") ~ add ^^ { 
                case op ~ right => (op, right)
            } |
            ("is" | "IS") ~ opt("not" | "NOT") ~ ("null" | "NULL") ^^ {
                case op ~ n ~ right => (n.isDefined, "is", right)
            } |
            opt("not" | "NOT") ~ ("between" | "BETWEEN") ~ add ~ ("and" | "AND") ~ add ^^ { 
                case n ~ op ~ start ~ _ ~ end => (n.isDefined, "between", start, end)
            } |
            opt("not" | "NOT") ~ ("in" | "IN") ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ { 
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("not" | "NOT") ~ ("like" | "LIKE") ~ add ^^ {
                case n ~ op ~ right => (n.isDefined, "like", right)
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
        windownFunction |
        function |
        aggFunction |
        ident ~ opt("." ~> ident | "*") ^^ {
            case id ~ None => SqlIdentExpr(id)
            case table ~ Some("*") => SqlAllColumnExpr(Some(table))
            case table ~ Some(column) => SqlPropertyExpr(table, column)
        } |
        "(" ~> expr <~ ")"

    def function: Parser[SqlExpr] =
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExprFuncExpr(funcName.toUpperCase.nn, args)
        }

    def aggFunction: Parser[SqlAggFuncExpr] =
        ("count" | "COUNT") ~ "(" ~ "*" ~ ")" ^^ (_ => SqlAggFuncExpr("COUNT", SqlAllColumnExpr(None) :: Nil, false, Map(), Nil)) |
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlAggFuncExpr(funcName.toUpperCase.nn, args, false, Map(), Nil)
        } |
        ident ~ ("(" ~ ("distinct" | "DISTINCT") ~> expr <~ ")") ^^ {
            case funcName ~ arg =>  SqlAggFuncExpr(funcName.toUpperCase.nn, arg :: Nil, true, Map(), Nil)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderBy])] =
        "(" ~> ("partition" | "PARTITION") ~> ("by" | "BY") ~> rep1sep(expr, ",") ~ opt(("order" | "ORDER") ~> ("by" | "BY") ~> rep1sep(orderBy, ",")) <~ ")" ^^ {
            case partition ~ order => (partition, order.getOrElse(Nil))
        } |
        "(" ~> ("order" | "ORDER") ~> ("by" | "BY") ~> rep1sep(orderBy, ",") <~ ")" ^^ {
            case order => (Nil, order)
        }

    def windownFunction: Parser[SqlExpr] =
        aggFunction ~ ("over" | "OVER") ~ over ^^ {
            case agg ~ _ ~ o => SqlOverExpr(agg, o._1, o._2, None)
        }

    def orderBy: Parser[SqlOrderBy] =
        expr ~ opt(("asc" | "ASC") | ("desc" | "DESC")) ^^ {
            case e ~ (Some("desc") | Some("DESC")) => SqlOrderBy(e, SqlOrderByOption.Desc)
            case e ~ _ => SqlOrderBy(e, SqlOrderByOption.Asc)
        }

    def cast: Parser[SqlExpr] =
        ("cast" | "CAST") ~> ("(" ~> expr ~ ("as" | "AS") ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => SqlCastExpr(expr, castType.toUpperCase.nn)
        }

    def caseWhen: Parser[SqlExpr] = 
        ("case" | "CASE") ~>
            rep1(("when" | "WHEN") ~> expr ~ ("then" | "THEN") ~ expr ^^ { case e ~ _ ~ te => SqlCase(e, te) }) ~ 
            opt(("else" | "ELSE") ~> expr) <~ ("end" | "END") ^^ {
                case branches ~ default => SqlCaseExpr(branches, default.getOrElse(SqlNullExpr))
            }
    
    def literal: Parser[SqlExpr] = 
        numericLit ^^ (i => SqlNumberExpr(BigDecimal(i))) |
        stringLit ^^ (xs => SqlCharExpr(xs)) |
        ("true" | "TRUE") ^^ (_ => SqlBooleanExpr(true)) |
        ("false" | "FALSE") ^^ (_ => SqlBooleanExpr(false)) |
        ("null" | "NULL") ^^ (_ => SqlNullExpr)

    def parse(text: String): SqlExpr = {
        phrase(expr)(new lexical.Scanner(text)) match {
            case Success(result, _) => result
            case e => throw ParseException(e.toString)
        }
    }
}

class ParseException(msg: String) extends Exception {
    override def toString: String = 
        s"easysql.parser.ParseException: \n$msg"
}