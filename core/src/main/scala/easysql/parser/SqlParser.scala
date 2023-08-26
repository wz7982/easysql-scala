package easysql.parser

import easysql.ast.expr.*
import easysql.ast.order.*
import easysql.ast.statement.*
import easysql.ast.table.*
import easysql.ast.limit.SqlLimit

import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.input.CharArrayReader.EofCh

class SqlParser extends StandardTokenParsers {
    class SqlLexical extends StdLexical {
        override protected def processIdent(name: String): Token = { 
            val upperCased = name.toUpperCase.nn
            if reserved.contains(upperCased) then Keyword(upperCased) else Identifier(name)
        }

        override def token: Parser[Token] = (
            identChar ~ rep(identChar | digit) ^^ { 
                case first ~ rest => processIdent((first :: rest).mkString("")) 
            } |
            '\"' ~> (identChar ~ rep(identChar | digit)) <~ '\"' ^^ { 
                case first ~ rest => Identifier((first :: rest).mkString("")) 
            } |
            '`' ~> (identChar ~ rep(identChar | digit)) <~ '`' ^^ { 
                case first ~ rest => Identifier((first :: rest).mkString("")) 
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

    override val lexical: SqlLexical = 
        new SqlLexical()

    lexical.reserved += (
        "CAST", "AS", "AND", "XOR", "OR", "OVER", "BY", "PARTITION", "ORDER", "DISTINCT", "NOT",
        "CASE", "WHEN", "THEN", "ELSE", "END", "ASC", "DESC", "TRUE", "FALSE", "NULL",
        "BETWEEN", "IN", "LIKE", "IS",
        "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "LIMIT", "OFFSET",
        "JOIN", "OUTER", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON", "LATERAL",
        "UNION", "EXCEPT", "INTERSECT", "ALL", "COUNT", "SUM", "AVG", "MAX", "MIN"
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
            opt("NOT") ~ "IN" ~ "(" ~ select ~ ")" ^^ { 
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, "in", in)
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
                case (acc, (not: Boolean, "in", in: SqlQueryExpr)) => SqlInExpr(acc, in, not)
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
        union |
        ident ~ opt("." ~> ident) ^^ {
            case id ~ None => SqlIdentExpr(id)
            case table ~ Some(column) => SqlPropertyExpr(table, column)
        } |
        "(" ~> expr <~ ")"

    def function: Parser[SqlExpr] =
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExprFuncExpr(funcName.toUpperCase.nn, args)
        }

    def aggFunc: Parser[String] = 
        ("COUNT" | "SUM" | "AVG" | "MAX" | "MIN")

    def aggFunction: Parser[SqlAggFuncExpr] =
        "COUNT" ~ "(" ~ "*" ~ ")" ^^ (_ => SqlAggFuncExpr("COUNT", SqlAllColumnExpr(None) :: Nil, false, Map(), Nil)) |
        (aggFunc | ident) ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlAggFuncExpr(funcName.toUpperCase.nn, args, false, Map(), Nil)
        } |
        ident ~ ("(" ~ "DISTINCT" ~> expr <~ ")") ^^ {
            case funcName ~ arg =>  SqlAggFuncExpr(funcName.toUpperCase.nn, arg :: Nil, true, Map(), Nil)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderBy])] =
        "(" ~> "PARTITION" ~> "BY" ~> rep1sep(expr, ",") ~ opt("ORDER" ~> "BY" ~> rep1sep(order, ",")) <~ ")" ^^ {
            case partition ~ order => (partition, order.getOrElse(Nil))
        } |
        "(" ~> "ORDER" ~> "BY" ~> rep1sep(order, ",") <~ ")" ^^ {
            case o => (Nil, o)
        }

    def windownFunction: Parser[SqlExpr] =
        aggFunction ~ "OVER" ~ over ^^ {
            case agg ~ _ ~ o => SqlOverExpr(agg, o._1, o._2, None)
        }

    def order: Parser[SqlOrderBy] =
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

    def unionType: Parser[SqlUnionType] =
        "UNION" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Union
            case _ => SqlUnionType.UnionAll
        } |
        "EXCEPT" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Except
            case _ => SqlUnionType.ExceptAll
        } |
        "INTERSECT" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Intersect
            case _ => SqlUnionType.IntersectAll
        }

    def union: Parser[SqlQueryExpr] =
        select ~ rep(unionType ~ select ^^ {
            case t ~ s => (t, s)
        }) ^^ {
            case s ~ unions => unions.foldLeft(s) {
                case (l, r) => SqlQueryExpr(SqlUnion(l.query, r._1, r._2.query))
            }
        }

    def select: Parser[SqlQueryExpr] =
        "SELECT" ~> opt("DISTINCT") ~ selectItems ~ opt(from) ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
            case distinct ~ s ~ f ~ w ~ g ~ o ~ l => {
                val param = 
                    if distinct.isDefined then Some("DISTINCT")
                    else None
                SqlQueryExpr(
                    SqlSelect(param, s, f.getOrElse(Nil), w, g.map(_._1).getOrElse(Nil), o.getOrElse(Nil), false, l, g.map(_._2).getOrElse(None))
                )
            }
        }

    def selectItems: Parser[List[SqlSelectItem]] =
        rep1sep(selectItem, ",")

    def selectItem: Parser[SqlSelectItem] =
        "*" ^^ (_ => SqlSelectItem(SqlAllColumnExpr(None), None)) |
        ident ~ "." ~ "*" ^^ { 
            case e ~ _ ~ _ => SqlSelectItem(SqlAllColumnExpr(Some(e)), None) 
        } |
        expr ~ opt(opt("AS") ~> ident) ^^ {
            case expr ~ alias => SqlSelectItem(expr, alias)
        }

    def simpleTable: Parser[SqlTable] =
        ident ~ opt(opt("AS") ~> ident) ^^ {
            case table ~ alias => SqlIdentTable(table, alias)
        } |
        opt("LATERAL") ~ ("(" ~> select <~ ")") ~ (opt("AS") ~> ident) ^^ {
            case lateral ~ s ~ alias => SqlSubQueryTable(s.query, lateral.isDefined, Some(alias))
        }

    def joinType: Parser[SqlJoinType] =
        "LEFT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.LeftJoin) |
        "RIGHT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.RightJoin) |
        "FULL" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.FullJoin) |
        "CROSS" ~ "JOIN" ^^ (_ => SqlJoinType.CrossJoin) |
        "INNER" ~ "JOIN" ^^ (_ => SqlJoinType.InnerJoin) |
        "JOIN" ^^ (_ => SqlJoinType.Join)

    def table: Parser[SqlTable] =
        simpleTable |
        "(" ~> joinTable <~ ")"

    def joinTable: Parser[SqlTable] =
        table ~ rep(joinType ~ table ~ opt("ON" ~> expr) ^^ {
            case jt ~ t ~ o => (jt, t, o)
        }) ^^ {
            case t ~ joins => joins.foldLeft(t) {
                case (l, r) => SqlJoinTable(l, r._1, r._2, r._3)
            }
        }

    def from: Parser[List[SqlTable]] =
        "FROM" ~> rep1sep(joinTable, ",")

    def where: Parser[SqlExpr] =
        "WHERE" ~> expr

    def groupBy: Parser[(List[SqlExpr], Option[SqlExpr])] =
        "GROUP" ~> "BY" ~> rep1sep(expr, ",") ~ opt("HAVING" ~> expr) ^^ {
            case g ~ h => (g, h)
        }

    def orderBy: Parser[List[SqlOrderBy]] =
        "ORDER" ~> "BY" ~> rep1sep(order, ",")

    def limit: Parser[SqlLimit] =
        "LIMIT" ~ numericLit ~ opt("OFFSET" ~> numericLit) ^^ {
            case _ ~ limit ~ offset => SqlLimit(limit.toInt, offset.map(_.toInt).getOrElse(0))
        }

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