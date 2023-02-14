package easysql.query.nonselect

import easysql.query.ToSql
import easysql.ast.statement.SqlStatement
import easysql.database.DB
import easysql.util.statementToString

trait NonSelect[T]