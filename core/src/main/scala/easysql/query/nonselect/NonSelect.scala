package easysql.query.nonselect

import easysql.query.ToSql
import easysql.ast.statement.SqlStatement
import easysql.database.DB
import easysql.util.statementToString

trait NonSelect {
    def getAst: SqlStatement
}

object NonSelect {
    given nonSelectToSql: ToSql[NonSelect] with {
        extension (x: NonSelect) {
            def sql(db: DB): String =
                statementToString(x.getAst, db)        
        }
    }
}