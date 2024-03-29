package easysql.dsl

import easysql.ast.SqlDataType
import easysql.ast.expr.*
import easysql.macros.*
import easysql.printer.*
import easysql.query.nonselect.*
import easysql.query.select.*
import easysql.parser.SqlParser

import scala.annotation.{experimental, targetName}
import scala.collection.mutable

def value[T <: SqlDataType](v: T): LiteralExpr[T] = 
    LiteralExpr(v)

def col[T <: SqlDataType](text: String): DynamicExpr[T] = 
    DynamicExpr(new SqlParser().parse(text))

def caseWhen[T <: SqlDataType](branches: CaseBranch[T]*): CaseExpr[T] = 
    CaseExpr(branches.toList, NullExpr)

def exists[T <: SqlDataType](query: Select[Tuple1[T], ?]): FuncExpr[Boolean] = 
    FuncExpr("EXISTS", List(SubQueryExpr(query)))

def notExists[T <: SqlDataType](query: Select[Tuple1[T], ?]): FuncExpr[Boolean] = 
    FuncExpr("NOT EXISTS", List(SubQueryExpr(query)))

def all[T <: SqlDataType](query: Select[Tuple1[T], ?]): FuncExpr[T] = 
    FuncExpr("ALL", List(SubQueryExpr(query)))

def any[T <: SqlDataType](query: Select[Tuple1[T], ?]): FuncExpr[T] = 
    FuncExpr("ANY", List(SubQueryExpr(query)))

def some[T <: SqlDataType](query: Select[Tuple1[T], ?]): FuncExpr[T] = 
    FuncExpr("SOME", List(SubQueryExpr(query)))

def cast[T <: SqlDataType](expr: Expr[?], castType: String): CastExpr[T] = 
    CastExpr(expr, castType)

def interval(value: String): IntervalExpr =
    IntervalExpr(value, None)

def interval(value: String, unit: SqlIntervalUnit): IntervalExpr =
    IntervalExpr(value, Some(unit))

def year: SqlIntervalUnit =
    SqlIntervalUnit.Year

def month: SqlIntervalUnit =
    SqlIntervalUnit.Month

def week: SqlIntervalUnit =
    SqlIntervalUnit.Week

def day: SqlIntervalUnit =
    SqlIntervalUnit.Day

def hour: SqlIntervalUnit =
    SqlIntervalUnit.Hour

def minute: SqlIntervalUnit =
    SqlIntervalUnit.Minute

def second: SqlIntervalUnit =
    SqlIntervalUnit.Second

def table(name: String): TableSchema[Nothing] = 
    TableSchema(name, None, Nil)

def unboundedPreceding: SqlOverBetweenType =
    SqlOverBetweenType.UnboundedPreceding

def unboundedFollowing: SqlOverBetweenType =
    SqlOverBetweenType.UnboundedFollowing

def currentRow: SqlOverBetweenType =
    SqlOverBetweenType.CurrentRow

extension (n: Int) {
    infix def preceding: SqlOverBetweenType =
        SqlOverBetweenType.Preceding(SqlNumberExpr(n))

    infix def following: SqlOverBetweenType =
        SqlOverBetweenType.Following(SqlNumberExpr(n))
}

transparent inline def asTable[T <: Product]: Any = 
    tableInfo[T]

extension [T <: SqlDataType] (expr: ColumnExpr[T, ?]) {
    def :=(value: T): (ColumnExpr[T, ?], Expr[?]) =
        (expr, LiteralExpr(value))

    def :=(value: Expr[T]): (ColumnExpr[T, ?], Expr[?]) =
        (expr, value)

    def :=(value: Option[T]): (ColumnExpr[T, ?], Expr[?]) = {
        val updateExpr = value match {
            case None => NullExpr
            case Some(value) => LiteralExpr(value)
        }

        (expr, updateExpr)
    }
}

object AllColumn {
    @targetName("allColumn")
    def * : Expr[Nothing] =
        AllColumnExpr(None)
}

def select[U <: Tuple](items: U): Select[InverseMap[U], AliasNames[U]] =
    Select().select(items)

def select[I <: SqlDataType, E <: Expr[I]](item: E): Select[Tuple1[I], AliasNames[Tuple1[E]]] =
    Select().select(item)

def select[I <: SqlDataType, N <: String](item: AliasExpr[I, N]): Select[Tuple1[I], Tuple1[N]] = 
    Select().select(item)

def select[P <: Product](table: TableSchema[P]): Select[Tuple1[P], EmptyTuple] = 
    Select().select(table)

def dynamicSelect(columns: Expr[?]*): Select[EmptyTuple, EmptyTuple] =
    Select().dynamicsSelect(columns*)

def from[P <: Product](table: TableSchema[P]): Select[Tuple1[P], EmptyTuple] =
    Select().select(table).from(table)

inline def selectByPk[T <: Product](pk: SqlDataType | Tuple): Select[Tuple1[T], EmptyTuple] = {
    val (_, cols) = fetchPk[T, pk.type]
    val table = asTable[T].asInstanceOf[TableSchema[T]]
    val select = Select().select(table).from(table)

    val conditions: List[Expr[Boolean]] = inline pk match {
        case t: Tuple => t.toArray.toList.zip(cols).map { (p, c) =>
            p match {
                case d: SqlDataType => BinaryExpr[Boolean](col(c), SqlBinaryOperator.Eq, LiteralExpr(d))
            }
        }
        case d: SqlDataType => List(BinaryExpr[Boolean](col(cols.head), SqlBinaryOperator.Eq, LiteralExpr(d)))
    }
    val where = conditions.reduce((x, y) => x && y)
    
    select.where(where)
}

def cte[T <: Tuple](withQuery: InWithQuery ?=> With[T]) = {
    given InWithQuery = In
    withQuery
}

def commonTable(query: AliasQuery[?, ?]*): With[EmptyTuple] =
    With().commonTable(query*)

def monadicQuery[T <: Product](table: TableSchema[T]): MonadicQuery[Tuple1[T], table.type] =
    MonadicQuery(table)

class InsertToken {
    infix def into[T <: Tuple](table: (TableSchema[?], T)): Insert[InverseMap[T], Nothing] = 
        Insert().insertInto(table._1)(table._2)
}

def insert: InsertToken =
    new InsertToken

inline def insert[T <: Product](entities: T*): Insert[EmptyTuple, InsertEntity] = 
    Insert().insert(entities*)

inline def insert[T <: Product](entityList: List[T]) =
    Insert().insert(entityList*)

inline def save[T <: Product](entity: T): Save = 
    Save().save(entity)

class UpdateToken {
    infix def table(table: TableSchema[?]): Update =
        Update().update(table)
}

def update: UpdateToken =
    new UpdateToken

inline def update[T <: Product](entity: T, skipNone: Boolean): Update = 
    Update().update(entity, skipNone)

class DeleteToken {
    infix def from(table: TableSchema[?]): Delete =
        Delete().deleteFrom(table)
}

def delete: DeleteToken =
    new DeleteToken

inline def delete[T <: Product](pk: SqlDataType | Tuple): Delete = 
    Delete().delete[T](pk)

def truncate(table: TableSchema[?]): Truncate = 
    Truncate().truncate(table)

@experimental
def jpa[E <: Product](table: TableSchema[E]): JPA[E] =
    JPA(table)

extension (s: Select[?, ?]) {
    @experimental
    def toEsDsl = {
        val visitor = new ESPrinter()
        visitor.printSelect(s.ast)
        visitor.dslBuilder.toString()
    }

    @experimental
    def toMongoDsl = {
        val visitor = new MongoPrinter()
        visitor.printSelect(s.ast)
        visitor.dslBuilder.toString
    }
}

extension (s: StringContext) {
    def sql(args: (SqlDataType | List[SqlDataType])*): NativeSql = {
        val pit = s.parts.iterator
        val builder = mutable.StringBuilder(pit.next())
        val sqlArgs = mutable.ArrayBuffer[SqlDataType]()
        args.foreach { arg =>
            arg match {
                case s: SqlDataType => {
                    builder.append("?")
                    sqlArgs.append(s)
                }
                case l: List[SqlDataType] => {
                    builder.append("(")
                    val list = l.map(_ => "?").mkString("", ", ", "")
                    builder.append(list)
                    builder.append(")")
                    sqlArgs.addAll(l)
                }
            }
            builder.append(pit.next)
        }

        new NativeSql(builder.toString, sqlArgs.toArray)
    }
}
