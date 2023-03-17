package easysql.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcOperator {
    public List<Object[]> jdbcQuery(Connection conn, String sql, Object[] args) throws SQLException {
        List<Object[]> result = new ArrayList<>();

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= args.length; i++) {
                stmt.setObject(i, args[i - 1]);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();

                while (rs.next()) {
                    List<Object> rowList = new ArrayList<>();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        Object object = rs.getObject(i);
                        Object data = null;

                        if (object != null) {
                            if (object instanceof java.math.BigDecimal) {
                                data = new scala.math.BigDecimal((java.math.BigDecimal) object);
                            } else if (object instanceof java.math.BigInteger) {
                                data = ((java.math.BigInteger) object).longValue();
                            } else if (object instanceof java.time.LocalDateTime) {
                                data = Date.from(((java.time.LocalDateTime) object).atZone(java.time.ZoneId.systemDefault()).toInstant());
                            } else if (object instanceof java.time.LocalDate) {
                                data = Date.from(((java.time.LocalDate) object).atStartOfDay().atZone(java.time.ZoneId.systemDefault()).toInstant());
                            } else if (object instanceof java.lang.Integer) {
                                data = object;
                            } else if (object instanceof java.lang.Long) {
                                data = object;
                            } else if (object instanceof java.lang.Float) {
                                data = object;
                            } else if (object instanceof java.lang.Double) {
                                data = object;
                            } else if (object instanceof java.lang.Boolean) {
                                data = object;
                            } else {
                                data = object.toString();
                            }
                        }

                        rowList.add(data);
                    }
                    result.add(rowList.toArray());
                }
            }
        }

        return result;
    }

    public int jdbcExec(Connection conn, String sql, Object[] args) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= args.length; i++) {
                stmt.setObject(i, args[i - 1]);
            }

            return stmt.executeUpdate();
        }
    }
    
    public List<Long> jdbcExecReturnKey(Connection conn, String sql, Object[] args) throws SQLException {
        List<Long> result = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 1; i <= args.length; i++) {
                stmt.setObject(i, args[i - 1]);
            }
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
            }
        }
        
        return result;
    }
}