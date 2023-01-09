package org.easysql.query.select

import org.easysql.ast.expr.{SqlAllColumnExpr, SqlIdentExpr}
import org.easysql.ast.limit.SqlLimit
import org.easysql.ast.order.{SqlOrderBy, SqlOrderByOption}
import org.easysql.ast.statement.select.{SqlSelect, SqlSelectItem, SqlSelectQuery}
import org.easysql.ast.table.*
import org.easysql.ast.SqlDataType
import org.easysql.database.DB
import org.easysql.dsl.{JoinTableSchema, TableSchema, *}
import org.easysql.util.toSqlString
import org.easysql.visitor.*

import java.sql.Connection
import scala.collection.mutable.ListBuffer

class Select[T <: Tuple, AliasNames <: Tuple] extends SelectQuery[T, AliasNames] {
    private val sqlSelect = SqlSelect()

    private var joinLeft: Option[SqlTable] = None

    infix def from[Table <: TableSchema[_]](table: Table): Select[T, AliasNames]  = {
        val from = SqlIdentTable(table._tableName)
        from.alias = table._aliasName

        joinLeft = Some(from)
        sqlSelect.from = Some(from)

        this
    }

    infix def from(table: SelectQuery[_, _]): Select[T, AliasNames]  = {
        val from = SqlSubQueryTable(table.getSelect)
        from.alias = table.aliasName

        joinLeft = Some(from)
        sqlSelect.from = Some(from)

        this
    }

    infix def fromLateral(table: SelectQuery[_, _]): Select[T, AliasNames]  = {
        val from = SqlSubQueryTable(table.getSelect, true)
        from.alias = table.aliasName

        joinLeft = Some(from)
        sqlSelect.from = Some(from)

        this
    }

    infix def select[U <: Tuple](items: U): Select[Tuple.Concat[T, RecursiveInverseMap[U]], Tuple.Concat[AliasNames, ExtractAliasNames[U]]] = {
        def addExpr(column: Expr[_]): Unit = {
            sqlSelect.addSelectItem(visitExpr(column))
            column match {
                case TableColumnExpr(_, column, ident, _) => selectItems.addOne(ident, column)
                case PrimaryKeyColumnExpr(_, column, ident, _, _) => selectItems.addOne(ident, column)
                case _ =>
            }
        }

        def addItem(item: AliasExpr[_, _]): Unit = {
            sqlSelect.addSelectItem(visitExpr(item.expr), Some(item.name))
            selectItems.addOne(item.name, item.name)
        }

        def spread(items: Tuple): Unit = {
            items.toArray.foreach {
                case t: Tuple => spread(t)
                case expr: Expr[_] => addExpr(expr)
                case item: AliasExpr[_, _] => addItem(item)
                case t: TableSchema[_] => {
                    t._cols.foreach { c =>
                        sqlSelect.addSelectItem(visitExpr(c))
                    }
                }
            }
        }

        spread(items)

        this.asInstanceOf[Select[Tuple.Concat[T, RecursiveInverseMap[U]], Tuple.Concat[AliasNames, ExtractAliasNames[U]]]]
    }
    
    infix def select[I <: SqlDataType, N <: String](item: TableColumnExpr[I, N]): Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]] = {
        sqlSelect.addSelectItem(visitExpr(item))
        selectItems.addOne(item.ident, item.column)
        
        this.asInstanceOf[Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]]]
    }
    
    infix def select[I <: SqlDataType, N <: String](item: PrimaryKeyColumnExpr[I, N]): Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]] = {
        sqlSelect.addSelectItem(visitExpr(item))
        selectItems.addOne(item.ident, item.column)
        
        this.asInstanceOf[Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]]]
    }

    infix def select[I <: SqlDataType](item: Expr[I]): Select[Tuple.Concat[T, Tuple1[I]], AliasNames] = {
        sqlSelect.addSelectItem(visitExpr(item))

        this.asInstanceOf[Select[Tuple.Concat[T, Tuple1[I]], AliasNames]]
    }

    infix def select[I <: SqlDataType, N <: String](item: AliasExpr[I, N]): Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]] = {
        sqlSelect.addSelectItem(visitExpr(item.expr), Some(item.name))
        selectItems.addOne(item.name, item.name)

        this.asInstanceOf[Select[Tuple.Concat[T, Tuple1[I]], Tuple.Concat[AliasNames, Tuple1[N]]]]
    }

    infix def select[P <: Product](table: TableSchema[P]): Select[Tuple.Concat[T, Tuple1[P]], AliasNames] = {
        table._cols.foreach { c =>
            sqlSelect.addSelectItem(visitExpr(c))
        }

        this.asInstanceOf[Select[Tuple.Concat[T, Tuple1[P]], AliasNames]]
    }

    infix def dynamicSelect(columns: Expr[_] | AliasExpr[_, _]*): Select[T, AliasNames] = {
        columns.foreach {
            case e: Expr[_] => {
                sqlSelect.addSelectItem(visitExpr(e))
                e match {
                    case TableColumnExpr(_, column, ident, _) => selectItems.addOne(ident, column)
                    case PrimaryKeyColumnExpr(_, column, ident, _, _) => selectItems.addOne(ident, column)
                    case _ =>
                }
            }
            case s: AliasExpr[_, _] => {
                sqlSelect.addSelectItem(visitExpr(s.expr), Some(s.name))
                selectItems.addOne(s.name, s.name)
            }
        }

        this
    }

    def distinct: Select[T, AliasNames] = {
        sqlSelect.distinct = true
        this
    }

    infix def where(condition: Expr[_]): Select[T, AliasNames] = {
        sqlSelect.addCondition(getExpr(condition))

        this
    }

    def where(test: () => Boolean, condition: Expr[_]): Select[T, AliasNames] = {
        if (test()) {
            sqlSelect.addCondition(getExpr(condition))
        }

        this
    }

    def where(test: Boolean, condition: Expr[_]): Select[T, AliasNames] = {
        if (test) {
            sqlSelect.addCondition(getExpr(condition))
        }

        this
    }

    infix def having(condition: Expr[_]): Select[T, AliasNames] = {
        sqlSelect.addHaving(getExpr(condition))

        this
    }

    infix def orderBy(item: OrderBy*): Select[T, AliasNames] = {
        val sqlOrderBy = item.map(o => SqlOrderBy(visitExpr(o.query), o.order))
        this.sqlSelect.orderBy.addAll(sqlOrderBy)

        this
    }

    infix def limit(count: Int): Select[T, AliasNames] = {
        if (this.sqlSelect.limit.isEmpty) {
            this.sqlSelect.limit = Some(SqlLimit(count, 0))
        } else {
            this.sqlSelect.limit.get.limit = count
        }
        this
    }

    infix def offset(offset: Int): Select[T, AliasNames] = {
        if (this.sqlSelect.limit.isEmpty) {
            this.sqlSelect.limit = Some(SqlLimit(1, offset))
        } else {
            this.sqlSelect.limit.get.offset = offset
        }
        this
    }

    infix def groupBy(item: Expr[_]*): Select[T, AliasNames] = {
        this.sqlSelect.groupBy.appendAll(item.map(i => visitExpr(i)))

        this
    }

    private def joinClause(table: TableSchema[_], joinType: SqlJoinType): Select[T, AliasNames] = {
        val joinTable = SqlIdentTable(table._tableName)
        joinTable.alias = table._aliasName

        val tableSource = joinLeft match {
            case None => joinTable
            case Some(value) => SqlJoinTable(value, joinType, joinTable)
        }
        sqlSelect.from = Some(tableSource)
        joinLeft = Some(tableSource)

        this
    }

    private def joinClause(table: SelectQuery[_, _], joinType: SqlJoinType, isLateral: Boolean = false): Select[T, AliasNames] = {
        val tableSource = joinLeft match {
            case None => SqlSubQueryTable(table.getSelect, isLateral = isLateral)
            case Some(value) => SqlJoinTable(value, joinType, SqlSubQueryTable(table.getSelect, isLateral = isLateral))
        }
            
        tableSource.alias = table.aliasName

        sqlSelect.from = Some(tableSource)
        joinLeft = Some(tableSource)

        this
    }

    private def joinClause(table: JoinTableSchema, joinType: SqlJoinType): Select[T, AliasNames] = {
        def unapplyTable(t: AnyTable): SqlTable = {
            t match {
                case table: TableSchema[_] =>
                    val ts = SqlIdentTable(table._tableName)
                    ts.alias = table._aliasName
                    ts
                case j: JoinTableSchema => SqlJoinTable(unapplyTable(j.left), j.joinType, unapplyTable(j.right), j.onCondition.map(getExpr))
            }
        }

        val joinTableSource = SqlJoinTable(unapplyTable(table.left), table.joinType, unapplyTable(table.right), table.onCondition.map(getExpr))
        val tableSource = joinLeft match {
            case None => joinTableSource
            case Some(value) => SqlJoinTable(value, joinType, joinTableSource)
        }

        sqlSelect.from = Some(tableSource)
        joinLeft = Some(tableSource)

        this
    }

    infix def on(onCondition: Expr[_]): Select[T, AliasNames] = {
        val from = this.sqlSelect.from.get
        from match {
            case table: SqlJoinTable => table.on = Some(visitExpr(onCondition))
            case _ =>
        }

        this
    }

    infix def join(table: TableSchema[_]): Select[T, AliasNames] = joinClause(table, SqlJoinType.JOIN)

    infix def join(query: SelectQuery[_, _]): Select[T, AliasNames] = joinClause(query, SqlJoinType.JOIN)

    infix def join(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.JOIN)

    infix def joinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] = joinClause(query, SqlJoinType.JOIN, true)

    infix def leftJoin(table: TableSchema[_]): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.LEFT_JOIN)

    infix def leftJoin(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.LEFT_JOIN)

    infix def leftJoin(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.LEFT_JOIN)

    infix def leftJoinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.LEFT_JOIN, true)

    infix def rightJoin(table: TableSchema[_]): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.RIGHT_JOIN)

    infix def rightJoin(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.RIGHT_JOIN)

    infix def rightJoin(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.RIGHT_JOIN)

    infix def rightJoinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.RIGHT_JOIN, true)

    infix def innerJoin(table: TableSchema[_]): Select[T, AliasNames] = joinClause(table, SqlJoinType.INNER_JOIN)

    infix def innerJoin(query: SelectQuery[_, _]): Select[T, AliasNames] = joinClause(query, SqlJoinType.INNER_JOIN)

    infix def innerJoin(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.INNER_JOIN)

    infix def innerJoinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] = joinClause(query, SqlJoinType.INNER_JOIN, true)

    infix def crossJoin(table: TableSchema[_]): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.CROSS_JOIN)

    infix def crossJoin(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.CROSS_JOIN)

    infix def crossJoin(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.CROSS_JOIN)

    infix def crossJoinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.CROSS_JOIN, true)

    infix def fullJoin(table: TableSchema[_]): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.FULL_JOIN)

    infix def fullJoin(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.FULL_JOIN)

    infix def fullJoin(table: JoinTableSchema): Select[T, AliasNames] =
        joinClause(table, SqlJoinType.FULL_JOIN)

    infix def fullJoinLateral(query: SelectQuery[_, _]): Select[T, AliasNames] =
        joinClause(query, SqlJoinType.FULL_JOIN, true)

    def forUpdate: Select[T, AliasNames] = {
        this.sqlSelect.forUpdate = true
        this
    }

    override def getSelect: SqlSelect = sqlSelect

    override def sql(db: DB): String = toSqlString(sqlSelect, db)

    override def toSql(using db: DB): String = toSqlString(sqlSelect, db)

    def countSql(db: DB): String = {
        val selectCopy = this.sqlSelect.copy(selectList = ListBuffer(), limit = None, orderBy = ListBuffer())
        selectCopy.selectList.clear()
        selectCopy.orderBy.clear()
        selectCopy.addSelectItem(visitExpr(count()), Some("count"))

        toSqlString(selectCopy, db)
    }

    def toCountSql(using db: DB): String = countSql(db)

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

    private[select] def clear: Unit = this.sqlSelect.selectList.clear
}

object Select {
    def apply(): Select[EmptyTuple, EmptyTuple] = new Select()
}