package easysql.query.nonselect

import easysql.ast.SqlDataType
import easysql.ast.expr.*
import easysql.ast.statement.{SqlInsert, SqlStatement}
import easysql.ast.table.*
import easysql.database.DB
import easysql.dsl.*
import easysql.macros.*
import easysql.query.ToSql
import easysql.query.select.*
import easysql.util.*

class Insert[T <: Tuple, S <: InsertState](private val ast: SqlInsert) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    inline def insert[P <: Product, SS >: S <: InsertEntity](entities: P*): Insert[EmptyTuple, InsertEntity] = {
        val tableName = fetchTableName[P]    
        val table = Some(SqlIdentTable(tableName, None))

        val metaDataList = entities.toList.map { entity =>
            val metaData = insertMetaData[P](entity)
            metaData.map { (name, value) => 
                val valueExpr = value match {
                    case d: SqlDataType => LiteralExpr(d)
                    case o: Option[_] => o.map(v => LiteralExpr(v.asInstanceOf[SqlDataType])).getOrElse(NullExpr)
                    case _ => NullExpr
                }

                name -> exprToSqlExpr(valueExpr)
            }
        }

        val columns = metaDataList.head.map { (name, _) =>
            exprToSqlExpr(IdentExpr(name))
        }
        val insertList = metaDataList.map(_.map(_._2))

        new Insert(ast.copy(table = table, values = insertList, columns = columns))
    }

    def insertInto[C <: Tuple](table: TableSchema[_])(columns: C): Insert[InverseMap[C], Nothing] = {
        val insertTable = Some(SqlIdentTable(table.__tableName, None))
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

    infix def values[SS >: S <: InsertValues](values: InsertType[T]*): Insert[T, InsertValues] = {
        val insertInfo = values.toList.map { v =>
            v.toList.map {
                case d: SqlDataType => exprToSqlExpr(LiteralExpr(d))
                case o: Option[_] => o match {
                    case Some(value) => exprToSqlExpr(LiteralExpr(value.asInstanceOf[SqlDataType]))
                    case _ => exprToSqlExpr(NullExpr)
                }
            }
        }

        new Insert(ast.copy(values = insertInfo))
    }

    infix def select[SS >: S <: InsertSelect, A <: Tuple](query: Query[T, A]): Insert[T, InsertSelect] =
        new Insert(ast.copy(query = Some(query.getAst)))
}

object Insert {
    def apply(): Insert[EmptyTuple, Nothing] = 
        new Insert(SqlInsert(None, Nil, Nil, None))
}

sealed trait InsertState

final class InsertEntity extends InsertState

final class InsertValues extends InsertState

final class InsertSelect extends InsertState