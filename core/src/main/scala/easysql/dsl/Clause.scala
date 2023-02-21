package easysql.dsl

import easysql.ast.SqlDataType
import easysql.ast.expr.SqlBinaryOperator
import easysql.query.select.*
import easysql.query.nonselect.*
import easysql.macros.*
import easysql.util.*
import easysql.printer.*
import easysql.database.DB

import scala.annotation.experimental
import scala.collection.mutable

def value[T <: SqlDataType](v: T): LiteralExpr[T] = 
    LiteralExpr(v)

def col[T <: SqlDataType](columnName: String): IdentExpr[T] = 
    IdentExpr(columnName)

def caseWhen[T <: SqlDataType](branches: CaseBranch[T]*): CaseExpr[T] = 
    CaseExpr(branches.toList, NullExpr)

def exists[T <: SqlDataType](query: Select[Tuple1[T], _]): FuncExpr[Boolean] = 
    FuncExpr("EXISTS", List(SubQueryExpr(query)))

def notExists[T <: SqlDataType](query: Select[Tuple1[T], _]): FuncExpr[Boolean] = 
    FuncExpr("NOT EXISTS", List(SubQueryExpr(query)))

def all[T <: SqlDataType](query: Select[Tuple1[T], _]): FuncExpr[Boolean] = 
    FuncExpr("ALL", List(SubQueryExpr(query)))

def any[T <: SqlDataType](query: Select[Tuple1[T], _]): FuncExpr[Boolean] = 
    FuncExpr("ANY", List(SubQueryExpr(query)))

def some[T <: SqlDataType](query: Select[Tuple1[T], _]): FuncExpr[Boolean] = 
    FuncExpr("SOME", List(SubQueryExpr(query)))

def cast[T <: SqlDataType](expr: Expr[_], castType: String): CastExpr[T] = 
    CastExpr(expr, castType)

def table(name: String): TableSchema[Nothing] = 
    TableSchema(name, None, Nil)

inline def asTable[T <: Product]: TableSchema[T] = {
    val tableName = fetchTableName[T]
    TableSchema(tableName, None, fieldNames[T].zip(identNames[T]).map((n, i) => ColumnExpr(tableName, n, i)))
}

extension [T <: SqlDataType] (expr: ColumnExpr[T, _] | IdentExpr[T]) {
    infix def to(value: Expr[T]): (ColumnExpr[T, _] | IdentExpr[T], Expr[T]) = 
        (expr, value)

    infix def to(value: T): (ColumnExpr[T, _] | IdentExpr[T], Expr[T]) = 
        (expr, LiteralExpr(value))
}

object AllColumn {
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

def dynamicSelect(columns: Expr[_]*): Select[EmptyTuple, EmptyTuple] =
    Select().dynamicsSelect(columns*)

def from[P <: Product](table: TableSchema[P]): Select[Tuple1[P], EmptyTuple] =
    Select().select(table).from(table)

inline def find[T <: Product](pk: SqlDataType | Tuple): Select[Tuple1[T], EmptyTuple] = {
    val (tableName, cols) = fetchPk[T, pk.type]
    val table = asTable[T]
    val select = Select().select(table).from(table)

    val conditions: List[Expr[Boolean]] = inline pk match {
        case t: Tuple => t.toArray.toList.zip(cols).map { (p, c) =>
            p match {
                case d: SqlDataType => BinaryExpr[Boolean](IdentExpr(c), SqlBinaryOperator.EQ, LiteralExpr(d))
            }
        }
        case d: SqlDataType => List(BinaryExpr[Boolean](IdentExpr(cols.head), SqlBinaryOperator.EQ, LiteralExpr(d)))
    }
    val where = conditions.reduce((x, y) => x && y)
    
    select.where(where)
}

def withQuery(query: AliasQuery[_, _]*): With[EmptyTuple] =
    With().addTable(query*)

def insertInto(table: TableSchema[_])(columns: Tuple): Insert[InverseMap[Tuple], Nothing] = 
    Insert().insertInto(table)(columns)

inline def insert[T <: Product](entities: T*): Insert[EmptyTuple, InsertEntity] = 
    Insert().insert(entities: _*)

inline def insert[T <: Product](eneityList: List[T]) = 
    Insert().insert(eneityList*)

inline def save[T <: Product](entity: T): Save = 
    Save().save(entity)

def update(table: TableSchema[_]): Update = 
    Update().update(table)

inline def update[T <: Product](entity: T, skipNone: Boolean): Update = 
    Update().update(entity, skipNone)

def deleteFrom(table: TableSchema[_]): Delete = 
    Delete().deleteFrom(table)

inline def delete[T <: Product](pk: SqlDataType | Tuple): Delete = 
    Delete().delete[T](pk)

def truncate(table: TableSchema[_]): Truncate = 
    Truncate().truncate(table)

extension (s: Select[_, _]) {
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
    def sql(args: (SqlDataType | List[SqlDataType])*): String = {
        val pit = s.parts.iterator
        val builder = mutable.StringBuilder(pit.next())
        args.foreach { arg =>
            val printer = fetchPrinter(DB.MYSQL)
            val expr = arg match {
                case s: SqlDataType => LiteralExpr(s)
                case l: List[SqlDataType] => ListExpr(l.map(LiteralExpr(_)))
            }
            val sqlExpr = exprToSqlExpr(expr)
            printer.printExpr(sqlExpr)
            builder.append(printer.sql)
            builder.append(pit.next())
        }
        builder.toString
    }
}
