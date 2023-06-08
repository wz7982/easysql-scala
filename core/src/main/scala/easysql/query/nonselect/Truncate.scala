package easysql.query.nonselect

import easysql.ast.statement.{SqlStatement, SqlTruncate}
import easysql.ast.table.SqlIdentTable
import easysql.dsl.TableSchema

class Truncate(private val ast: SqlTruncate) extends NonSelect {
    override def getAst: SqlStatement =
        ast

    def truncate(table: TableSchema[_]): Truncate = {
        val truncateTable = Some(SqlIdentTable(table.__tableName, None))

        new Truncate(ast.copy(table = truncateTable))
    }
}

object Truncate {
    def apply(): Truncate = 
        new Truncate(SqlTruncate(None))
}
