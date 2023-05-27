package easysql.parser

import easysql.ast.expr.*
import easysql.ast.order.*

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.input.CharArrayReader.EofCh
import scala.util.matching.Regex

class SqlParser extends StandardTokenParsers {
    class SqlLexical extends StdLexical {
        override protected def processIdent(name: String) = { 
            val upperCased = name.toUpperCase.nn
            if reserved.contains(upperCased) then Keyword(upperCased) else Identifier(name)
        }

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
                case i ~ None => NumericLit(i.mkString(""))
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
        "BETWEEN", "IN", "LIKE", "IS"
    )

    lexical.delimiters += (
        "+", "-", "*", "/", "%", "=", "<>", "!=", ">", ">=", "<", "<=", "(", ")", ",", ".", "`", "\""
    )

    def expr: Parser[SqlExpr] = 
        or

    def or: Parser[SqlExpr] =
        xor * ("OR" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Or, r) })

    def xor: Parser[SqlExpr] =
        and * ("XOR" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.Xor, r) })

    def and: Parser[SqlExpr] =
        relation * ("AND" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlBinaryExpr(l, SqlBinaryOperator.And, r) })

    def relation: Parser[SqlExpr] =
        add ~ rep(
            ("=" | "<>" | "!=" | ">" | ">=" | "<" | "<=") ~ add ^^ { 
                case op ~ right => (op, right)
            } |
            "IS" ~ opt("NOT") ~ "NULL" ^^ {
                case op ~ n ~ right => (n.isDefined, "is", right)
            } |
            opt("NOT") ~ "BETWEEN" ~ add ~ "AND" ~ add ^^ { 
                case n ~ op ~ start ~ _ ~ end => (n.isDefined, "between", start, end)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ { 
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "LIKE" ~ add ^^ {
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
        ident ~ ("(" ~ "DISTINCT" ~> expr <~ ")") ^^ {
            case funcName ~ arg =>  SqlAggFuncExpr(funcName.toUpperCase.nn, arg :: Nil, true, Map(), Nil)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderBy])] =
        "(" ~> "PARTITION" ~> "BY" ~> rep1sep(expr, ",") ~ opt("ORDER" ~> "BY" ~> rep1sep(orderBy, ",")) <~ ")" ^^ {
            case partition ~ order => (partition, order.getOrElse(Nil))
        } |
        "(" ~> "ORDER" ~> "BY" ~> rep1sep(orderBy, ",") <~ ")" ^^ {
            case order => (Nil, order)
        }

    def windownFunction: Parser[SqlExpr] =
        aggFunction ~ "OVER" ~ over ^^ {
            case agg ~ _ ~ o => SqlOverExpr(agg, o._1, o._2, None)
        }

    def orderBy: Parser[SqlOrderBy] =
        expr ~ opt("ASC" | "DESC") ^^ {
            case e ~ Some("DESC") => SqlOrderBy(e, SqlOrderByOption.Desc)
            case e ~ _ => SqlOrderBy(e, SqlOrderByOption.Asc)
        }

    def cast: Parser[SqlExpr] =
        "CAST" ~> ("(" ~> expr ~ "AS" ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => SqlCastExpr(expr, castType.toUpperCase.nn)
        }

    def caseWhen: Parser[SqlExpr] = 
        "CASE" ~>
            rep1("WHEN" ~> expr ~ "THEN" ~ expr ^^ { case e ~ _ ~ te => SqlCase(e, te) }) ~ 
            opt("ELSE" ~> expr) <~ "END" ^^ {
                case branches ~ default => SqlCaseExpr(branches, default.getOrElse(SqlNullExpr))
            }
    
    def literal: Parser[SqlExpr] = 
        numericLit ^^ (i => SqlNumberExpr(BigDecimal(i))) |
        stringLit ^^ (xs => SqlCharExpr(xs)) |
        "TRUE" ^^ (_ => SqlBooleanExpr(true)) |
        "FALSE" ^^ (_ => SqlBooleanExpr(false)) |
        "NULL" ^^ (_ => SqlNullExpr)

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