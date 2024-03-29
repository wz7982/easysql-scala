package easysql.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.unsafeNulls

def jdbcQuery(conn: Connection, sql: String): List[Map[String, Any]] = {
    var stmt: Statement = null
    var rs: ResultSet = null
    val result = ListBuffer[Map[String, Any]]()

    try {
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        val metadata = rs.getMetaData

        while (rs.next()) {
            val rowMap = (1 to metadata.getColumnCount).map { it =>
                metadata.getColumnLabel(it) -> rs.getObject(it)
            }.toMap
            result.addOne(rowMap)
        }
    } catch {
        case e: Exception => throw e
    } finally {
        if (stmt != null) {
            stmt.close()
        }
        if (rs != null) {
            rs.close()
        }
    }

    result.toList
}

def jdbcQueryToArray(conn: Connection, sql: String, args: Array[Any]): List[Array[Any]] = {
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    val result = ListBuffer[Array[Any]]()

    try {
        stmt = conn.prepareStatement(sql)
        for (i <- 1 to args.length) {
            val arg = args(i - 1)
            arg match {
                case n: BigDecimal => stmt.setBigDecimal(i, n.bigDecimal)
                case n: Int => stmt.setInt(i, n)
                case n: Long => stmt.setLong(i, n)
                case n: Float => stmt.setFloat(i, n)
                case n: Double => stmt.setDouble(i, n)
                case b: Boolean => stmt.setBoolean(i, b)
                case s: String => stmt.setString(i, s)
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
            }
        }
        rs = stmt.executeQuery()
        val metadata = rs.getMetaData

        while (rs.next()) {
            val rowList = (1 to metadata.getColumnCount).toArray.map { it =>
                val data: Any = rs.getObject(it) match {
                    case null => null
                    case b: java.math.BigDecimal => BigDecimal(b)
                    case l: java.math.BigInteger => l.longValue()
                    case l: java.time.LocalDateTime => Date.from(l.atZone(java.time.ZoneId.systemDefault()).toInstant)
                    case l: java.time.LocalDate => Date.from(l.atStartOfDay().atZone(java.time.ZoneId.systemDefault()).toInstant)
                    case i: java.lang.Integer => i
                    case l: java.lang.Long => l
                    case f: java.lang.Float => f
                    case d: java.lang.Double => d
                    case b: java.lang.Boolean => b
                    case d: java.util.Date => d
                    case d @ _ => d.toString
                }
                data
            }
            result.addOne(rowList)
        }
    } catch {
        case e: Exception => throw e
    } finally {
        if (stmt != null) {
            stmt.close()
        }
        if (rs != null) {
            rs.close()
        }
    }

    result.toList
}

def jdbcExec(conn: Connection, sql: String, args: Array[Any]): Int = {
    var stmt: PreparedStatement = null
    var result = 0

    try {
        stmt = conn.prepareStatement(sql)
        for (i <- 1 to args.length) {
            val arg = args(i - 1)
            arg match {
                case n: BigDecimal => stmt.setBigDecimal(i, n.bigDecimal)
                case n: Int => stmt.setInt(i, n)
                case n: Long => stmt.setLong(i, n)
                case n: Float => stmt.setFloat(i, n)
                case n: Double => stmt.setDouble(i, n)
                case b: Boolean => stmt.setBoolean(i, b)
                case s: String => stmt.setString(i, s)
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
            }
        }
        result = stmt.executeUpdate()
    } catch {
        case e: Exception => throw e
    } finally {
        if (stmt != null) {
            stmt.close()
        }
    }

    result
}

def jdbcExecReturnKey(conn: Connection, sql: String, args: Array[Any]): List[Long] = {
    var stmt: PreparedStatement = null
    val result = ListBuffer[Long]()

    try {
        stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        for (i <- 1 to args.length) {
            val arg = args(i - 1)
            arg match {
                case n: BigDecimal => stmt.setBigDecimal(i, n.bigDecimal)
                case n: Int => stmt.setInt(i, n)
                case n: Long => stmt.setLong(i, n)
                case n: Float => stmt.setFloat(i, n)
                case n: Double => stmt.setDouble(i, n)
                case b: Boolean => stmt.setBoolean(i, b)
                case s: String => stmt.setString(i, s)
                case d: Date => stmt.setDate(i, java.sql.Date(d.getTime))
                case _ => stmt.setObject(i, arg)
            }
        }
        stmt.executeUpdate()
        val resultSet = stmt.getGeneratedKeys
        while (resultSet.next()) {
            result += resultSet.getLong(1)
        }
    } catch {
        case e: Exception => throw e
    } finally {
        if (stmt != null) {
            stmt.close()
        }
    }

    result.toList
}