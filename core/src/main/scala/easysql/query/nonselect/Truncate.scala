package easysql.query.nonselect

import easysql.ast.statement.SqlStatement
import easysql.ast.table.SqlTable.SqlIdentTable
import easysql.dsl.TableSchema
import easysql.query.ToSql
import easysql.database.DB
import easysql.util.statementToString

class Truncate(private val ast: SqlStatement.SqlTruncate) {
    def truncate(table: TableSchema[_]): Truncate = {
        val truncateTable = Some(new SqlIdentTable(table.__tableName, None))

        new Truncate(ast.copy(table = truncateTable))
    }
}

object Truncate {
    def apply(): Truncate = 
        new Truncate(SqlStatement.SqlTruncate(None))

    given truncateNonSelect: NonSelect[Truncate] with {}

    given saveToSql: ToSql[Truncate] with {
        extension (x: Truncate) {
            def sql(db: DB): String =
                statementToString(x.ast, db)        
        }
    }
}
