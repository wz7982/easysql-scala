package org.easysql.query.select

import org.easysql.ast.expr.{SqlAllColumnExpr, SqlIdentifierExpr}
import org.easysql.ast.limit.SqlLimit
import org.easysql.ast.order.SqlOrderBy
import org.easysql.ast.statement.select.{SqlSelect, SqlSelectItem, SqlSelectQuery}
import org.easysql.ast.table.*
import org.easysql.ast.SqlSingleConstType
import org.easysql.database.DB
import org.easysql.dsl.{JoinTableSchema, TableSchema, *}
import org.easysql.util.toSqlString
import org.easysql.visitor.*

import java.sql.Connection
import scala.collection.mutable.ListBuffer
import scala.compiletime.ops.any.==
import scala.language.dynamics

class Select[T <: Tuple, Table <: TableSchema | Tuple] extends SelectQueryImpl[T] with Dynamic {
    private val sqlSelect = SqlSelect(selectList = ListBuffer(SqlSelectItem(SqlAllColumnExpr())))

    private var joinLeft: SqlTableSource = SqlIdentifierTableSource("")

    private var aliasName: Option[String] = None

    infix def from(table: TableSchema): Select[T, table.type] = {
        val tableName = table.tableName

        val from = Some(SqlIdentifierTableSource(tableName))
        joinLeft = from.get
        sqlSelect.from = from

        this.asInstanceOf[Select[T, table.type]]
    }

    infix def from(table: AliasedTableSchema | String) = {
        val tableName = table match {
            case aliasedTableSchema: AliasedTableSchema => aliasedTableSchema.tableName
            case string: String => string
        }

        val from = Some(SqlIdentifierTableSource(tableName))
        if (table.isInstanceOf[AliasedTableSchema]) {
            from.get.alias = Some(table.asInstanceOf[AliasedTableSchema].aliasName)
        }
        joinLeft = from.get
        sqlSelect.from = from

        this
    }

    infix def from(table: SelectQuery[_]) = {
        val from = Some(SqlSubQueryTableSource(table.getSelect))
        joinLeft = from.get
        sqlSelect.from = from
        this
    }

    infix def from(table: Select[_, _]) = {
        val from = SqlSubQueryTableSource(table.getSelect)
        if (table.aliasName.nonEmpty) {
            from.alias = table.aliasName
        }
        joinLeft = from
        sqlSelect.from = Some(from)
        this
    }

    infix def fromLateral(query: SelectQuery[_]) = {
        val from = Some(SqlSubQueryTableSource(query.getSelect, true))
        joinLeft = from.get
        sqlSelect.from = from
        this
    }

    infix def fromLateral(table: Select[_, _]) = {
        val from = SqlSubQueryTableSource(table.getSelect, true)
        if (table.aliasName.nonEmpty) {
            from.alias = table.aliasName
        }
        joinLeft = from
        sqlSelect.from = Some(from)
        this
    }

    infix def as(name: String)(using NonEmpty[name.type] =:= Any) = {
        this.aliasName = Some(name)
        this
    }

    infix def unsafeAs(name: String) = {
        this.aliasName = Some(name)
        this
    }

    infix def select[U <: Tuple](items: U): Select[Tuple.Concat[T, RecursiveInverseMap[U, Expr]], Table] = {
        if (this.sqlSelect.selectList.size == 1 && this.sqlSelect.selectList.head.expr.isInstanceOf[SqlAllColumnExpr]) {
            this.sqlSelect.selectList.clear()
        }

        def addItem(column: Expr[_, _]): Unit = {
            if (column.alias.isEmpty) {
                sqlSelect.addSelectItem(visitExpr(column))
            } else {
                sqlSelect.addSelectItem(visitExpr(column), column.alias)
            }
        }

        def spread(items: Tuple): Unit = {
            items.toArray.foreach {
                case t: Tuple => spread(t)
                case expr: Expr[_, _] => addItem(expr)
            }
        }

        spread(items)
        this.asInstanceOf[Select[Tuple.Concat[T, RecursiveInverseMap[U, Expr]], Table]]
    }

    infix def select[I <: SqlSingleConstType | Null, ST <: TableSchema | Tuple](item: Expr[I, ST])(using TableContains[Table, ST] =:= Any): Select[Tuple.Concat[T, InverseMap[Tuple1[Expr[I, ST]], Expr]], Table] = {
        if (this.sqlSelect.selectList.size == 1 && this.sqlSelect.selectList.head.expr.isInstanceOf[SqlAllColumnExpr]) {
            this.sqlSelect.selectList.clear()
        }

        if (item.alias.isEmpty) {
            sqlSelect.addSelectItem(visitExpr(item))
        } else {
            sqlSelect.addSelectItem(visitExpr(item), item.alias)
        }
        this.asInstanceOf[Select[Tuple.Concat[T, InverseMap[Tuple1[Expr[I, ST]], Expr]], Table]]
    }

    infix def dynamicSelect(columns: Expr[_, _]*): Select[Tuple1[Nothing], Table] = {
        if (this.sqlSelect.selectList.size == 1 && this.sqlSelect.selectList.head.expr.isInstanceOf[SqlAllColumnExpr]) {
            this.sqlSelect.selectList.clear()
        }

        columns.foreach { item =>
            if (item.alias.isEmpty) {
                sqlSelect.addSelectItem(visitExpr(item))
            } else {
                sqlSelect.addSelectItem(visitExpr(item), item.alias)
            }
        }

        this.asInstanceOf[Select[Tuple1[Nothing], Table]]
    }

    def distinct = {
        sqlSelect.distinct = true
        this
    }

    infix def where[ET <: TableSchema | Tuple](condition: Expr[_, ET])(using TableContains[Table, ET] == true) = {
        sqlSelect.addCondition(getExpr(condition))
        this
    }

    def unsafeWhere(condition: Expr[_, _]) = {
        sqlSelect.addCondition(getExpr(condition))
        this
    }

    def where[ET <: TableSchema | Tuple](test: () => Boolean, condition: Expr[_, ET])(using TableContains[Table, ET] == true) = {
        if (test()) {
            sqlSelect.addCondition(getExpr(condition))
        }
        this
    }

    def where[ET <: TableSchema | Tuple](test: Boolean, condition: Expr[_, ET])(using TableContains[Table, ET] == true) = {
        if (test) {
            sqlSelect.addCondition(getExpr(condition))
        }
        this
    }

    infix def having[ET <: TableSchema | Tuple](condition: Expr[_, ET])(using TableContains[Table, ET] == true) = {
        sqlSelect.addHaving(getExpr(condition))
        this
    }

    //    infix def orderBy(items: OrderBy*) = {
    //        items.foreach { it =>
    //            val sqlOrderBy = SqlOrderBy(visitExpr(it.query), it.order)
    //            this.sqlSelect.orderBy.append(sqlOrderBy)
    //        }
    //        this
    //    }

    infix def limit(count: Int) = {
        if (this.sqlSelect.limit.isEmpty) {
            this.sqlSelect.limit = Some(SqlLimit(count, 0))
        } else {
            this.sqlSelect.limit.get.limit = count
        }
        this
    }

    infix def offset(offset: Int) = {
        if (this.sqlSelect.limit.isEmpty) {
            this.sqlSelect.limit = Some(SqlLimit(1, offset))
        } else {
            this.sqlSelect.limit.get.offset = offset
        }
        this
    }

    //    infix def groupBy(items: Expr[_]*) = {
    //        items.foreach { it =>
    //            this.sqlSelect.groupBy.append(visitExpr(it))
    //        }
    //        this
    //    }

    private def joinClause(table: String | TableSchema | AliasedTableSchema, joinType: SqlJoinType) = {
        val tableName = table match {
            case string: String => string
            case tableSchema: TableSchema => tableSchema.tableName
            case aliasedTableSchema: AliasedTableSchema => aliasedTableSchema.tableName
        }
        val joinTable = SqlIdentifierTableSource(tableName)
        if (table.isInstanceOf[AliasedTableSchema]) {
            joinTable.alias = Some(table.asInstanceOf[AliasedTableSchema].aliasName)
        }

        val join = SqlJoinTableSource(joinLeft, joinType, joinTable)
        sqlSelect.from = Some(join)
        joinLeft = join

        table match {
            case tableSchema: TableSchema => this.asInstanceOf[Select[T, TableConcat[Table, tableSchema.type]]]
            case _ => this
        }
    }

    private def joinClause(table: SelectQuery[_], joinType: SqlJoinType, isLateral: Boolean = false) = {
        val join = SqlJoinTableSource(joinLeft, joinType, SqlSubQueryTableSource(table.getSelect, isLateral = isLateral))
        if (table.isInstanceOf[Select[_, _]]) {
            join.alias = table.asInstanceOf[Select[_, _]].aliasName
        }
        sqlSelect.from = Some(join)
        joinLeft = join
        this
    }

    private def joinClause(table: JoinTableSchema, joinType: SqlJoinType) = {
        def unapplyTable(t: TableSchema | JoinTableSchema | AliasedTableSchema): SqlTableSource = {
            t match {
                case table: TableSchema => SqlIdentifierTableSource(table.tableName)
                case a: AliasedTableSchema => {
                    val ts = SqlIdentifierTableSource(a.tableName)
                    ts.alias = Some(a.aliasName)
                    ts
                }
                case j: JoinTableSchema => SqlJoinTableSource(unapplyTable(j.left), j.joinType, unapplyTable(j.right), j.onCondition.map(getExpr))
            }
        }

        val joinTableSource = SqlJoinTableSource(unapplyTable(table.left), table.joinType, unapplyTable(table.right), table.onCondition.map(getExpr))
        val join = SqlJoinTableSource(joinLeft, joinType, joinTableSource)

        sqlSelect.from = Some(join)
        joinLeft = join
        this
    }

    infix def on[ET <: TableSchema | Tuple](onCondition: Expr[_, ET])(using TableContains[Table, ET] == true) = {
        val from = this.sqlSelect.from.get
        from match {
            case table: SqlJoinTableSource => table.on = Some(visitExpr(onCondition))
            case _ =>
        }
        this
    }

    infix def join(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.JOIN)
    }

    infix def join(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.JOIN)
    }

    infix def join(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.JOIN)
    }

    infix def joinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.JOIN, true)
    }

    infix def leftJoin(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.LEFT_JOIN)
    }

    infix def leftJoin(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.LEFT_JOIN)
    }

    infix def leftJoin(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.LEFT_JOIN)
    }

    infix def leftJoinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.LEFT_JOIN, true)
    }

    infix def rightJoin(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.RIGHT_JOIN)
    }

    infix def rightJoin(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.RIGHT_JOIN)
    }

    infix def rightJoin(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.RIGHT_JOIN)
    }

    infix def rightJoinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.RIGHT_JOIN, true)
    }

    infix def innerJoin(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.INNER_JOIN)
    }

    infix def innerJoin(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.INNER_JOIN)
    }

    infix def innerJoin(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.INNER_JOIN)
    }

    infix def innerJoinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.INNER_JOIN, true)
    }

    infix def crossJoin(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.CROSS_JOIN)
    }

    infix def crossJoin(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.CROSS_JOIN)
    }

    infix def crossJoin(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.CROSS_JOIN)
    }

    infix def crossJoinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.CROSS_JOIN, true)
    }

    infix def fullJoin(table: String | TableSchema | AliasedTableSchema) = {
        joinClause(table, SqlJoinType.FULL_JOIN)
    }

    infix def fullJoin(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.FULL_JOIN)
    }

    infix def fullJoin(table: JoinTableSchema) = {
        joinClause(table, SqlJoinType.FULL_JOIN)
    }

    infix def fullJoinLateral(query: SelectQuery[_]) = {
        joinClause(query, SqlJoinType.FULL_JOIN, true)
    }

    def forUpdate = {
        this.sqlSelect.forUpdate = true
        this
    }

    override def getSelect: SqlSelectQuery = sqlSelect

    override def sql(db: DB): String = toSqlString(sqlSelect, db)

    override def toSql(using db: DB): String = toSqlString(sqlSelect, db)

    def fetchCountSql(db: DB): String = {
        val selectCopy = this.sqlSelect.copy(selectList = ListBuffer(), limit = None, orderBy = ListBuffer())
        selectCopy.selectList.clear()
        selectCopy.orderBy.clear()
        selectCopy.addSelectItem(visitExpr(count()), Some("count"))

        toSqlString(selectCopy, db)
    }

    def toFetchCountSql(using db: DB): String = fetchCountSql(db)

    def pageSql(pageSize: Int, pageNumber: Int)(db: DB): String = {
        val offset = if (pageNumber <= 1) {
            0
        } else {
            pageSize * (pageNumber - 1)
        }
        val limit = SqlLimit(pageSize, offset)

        val selectCopy = this.sqlSelect.copy(limit = Some(limit))

        toSqlString(selectCopy, db)
    }

    def toPageSql(pageSize: Int, pageNumber: Int)(using db: DB): String = pageSql(pageSize, pageNumber)(db)

    def selectDynamic(name: String): Expr[Nothing, NothingTable] = TableColumnExpr[Nothing, NothingTable](aliasName.get, name)
}

object Select {
    def apply(): Select[EmptyTuple, NothingTable] = new Select()
}