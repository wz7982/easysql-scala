package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.table.SqlTable.SqlIdentTable
import easysql.dsl.TableSchema

class Truncate(
    private[easysql] override val ast: SqlStatement.SqlTruncate
) extends NonSelect(ast) {
    def truncate(table: TableSchema[_]): Truncate = {
        val truncateTable = Some(new SqlIdentTable(table.__tableName, None))

        new Truncate(ast.copy(table = truncateTable))
    }
}

object Truncate {
    def apply(): Truncate = new Truncate(SqlStatement.SqlTruncate(None))
}
