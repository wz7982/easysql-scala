package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.table.SqlTable.*
import easysql.ast.SqlDataType
import easysql.ast.expr.SqlBinaryOperator
import easysql.ast.expr.SqlExpr.*
import easysql.query.ToSql
import easysql.dsl.*
import easysql.macros.*
import easysql.util.*
import easysql.query.select.Query
import easysql.database.DB

class Insert[T <: Tuple, S <: InsertState](val ast: SqlStatement.SqlInsert) {
    inline def insert[T <: Product, SS >: S <: InsertEntity](entities: T*): Insert[EmptyTuple, InsertEntity] = {
        val metaData = insertMetaData[T]
        val table = Some(new SqlIdentTable(metaData._1, None))
        val insertList = entities.toList.map { entity =>
            metaData._2.map { i =>
                val value = i._2.apply(entity) match {
                    case d: SqlDataType => LiteralExpr(d)
                    case o: Option[SqlDataType] => o.map(LiteralExpr(_)).getOrElse(NullExpr)
                }
                
                exprToSqlExpr(value)
            }
        }
        val columns = metaData._2.map(e => exprToSqlExpr(IdentExpr(e._1)))

        new Insert(ast.copy(table = table, values = insertList, columns = columns))
    }

    def insertInto[C <: Tuple](table: TableSchema[_])(columns: C): Insert[InverseMap[C], Nothing] = {
        val insertTable = Some(new SqlIdentTable(table.__tableName, None))
        val insertColumns = columns.toList.filter {
            case e: IdentExpr[_] => true
            case e: ColumnExpr[_, _] => true
            case e: PrimaryKeyExpr[_, _] => true
            case _ => false
        }.map {
            case e: IdentExpr[_] => SqlIdentExpr(e.column)
            case e: ColumnExpr[_, _] => SqlIdentExpr(e.columnName)
            case e: PrimaryKeyExpr[_, _] => SqlIdentExpr(e.columnName)
        }

        new Insert(ast.copy(table = insertTable, columns = insertColumns))
    }

    infix def values[SS >: S <: InsertValues](values: T*): Insert[T, InsertValues] = {
        val insertInfo = values.toList.map { v =>
            v.toList.map {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
            }
        }

        new Insert(ast.copy(values = insertInfo))
    }

    infix def select[SS >: S <: InsertSelect](query: Query[T, _]): Insert[T, InsertSelect] =
        new Insert(ast.copy(query = Some(query.ast)))
}

object Insert {
    def apply(): Insert[EmptyTuple, Nothing] = 
        new Insert(SqlStatement.SqlInsert(None, Nil, Nil, None))

    given insertNonSelect: NonSelect[Insert[_, _]] with {
        extension (x: Insert[_, _]) {
            def ast: SqlStatement =
                x.ast
        }
    }

    given insertToSql: ToSql[Insert[_, _]] with {
        extension (x: Insert[_, _]) {
            def sql(db: DB): String =
                statementToString(x.ast, db)        
        }
    }
}

sealed trait InsertState

final class InsertEntity extends InsertState

final class InsertValues extends InsertState

final class InsertSelect extends InsertState