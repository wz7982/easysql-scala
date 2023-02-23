package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.table.SqlTable.SqlIdentTable
import easysql.dsl.TableSchema
import easysql.query.ToSql
import easysql.database.DB
import easysql.util.statementToString

class Truncate(private val ast: SqlStatement.SqlTruncate) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    def truncate(table: TableSchema[_]): Truncate = {
        val truncateTable = Some(new SqlIdentTable(table.__tableName, None))

        new Truncate(ast.copy(table = truncateTable))
    }
}

object Truncate {
    def apply(): Truncate = 
        new Truncate(SqlStatement.SqlTruncate(None))
}
