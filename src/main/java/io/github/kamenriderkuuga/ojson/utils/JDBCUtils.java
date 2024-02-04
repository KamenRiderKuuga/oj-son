package io.github.kamenriderkuuga.ojson.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.*;

/**
 * JDBC工具类
 *
 * @author guohao
 * @date 2023/2/13
 */
public class JDBCUtils {

    public static Boolean executeSQL(String sql, Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.execute(sql);
    }

    /**
     * 获取数据库连接
     *
     * @param sqlEngine SQL引擎
     * @return 数据库连接
     */
    public static Connection getConnection(String sqlEngine) throws SQLException {
        switch (sqlEngine.toLowerCase()) {
            case "h2":
                return DriverManager.getConnection("jdbc:h2:mem:;DATABASE_TO_UPPER=false");
            default:
                return DriverManager.getConnection("jdbc:sqlite::memory:");
        }
    }

    public static Boolean insert(String table, List<Object> values, List<String> jsonColumns, Connection connection) throws SQLException {
        var sql = "INSERT INTO " + table + " VALUES(" + StringUtils.repeat("?", ",", values.size() + 1) + ")";
        var statement = connection.prepareStatement(sql);
        for (int i = 0; i < values.size(); i++) {
            statement.setObject(i + 1, values.get(i));
        }
        statement.setObject(values.size() + 1, JsonUtils.toJson(jsonColumns));
        return statement.execute();
    }

    public static Map<String, Object> select(String sql, Connection connection) throws SQLException {
        ResultSet data = null;
        Set<String> nodeSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        try {
            var statement = connection.createStatement();
            data = statement.executeQuery(sql);
            var md = data.getMetaData();
            int columns = md.getColumnCount();
            while (data.next()) {
                HashMap<String, Object> row = new LinkedHashMap<>(columns);
                var meta = data.getString("___metadata___");
                List jsonColumns = JsonUtils.fromJson(meta, List.class);
                for (int i = 1; i <= columns - 1; ++i) {
                    if (md.getColumnLabel(i).contains("___metadata___")) {
                        continue;
                    }
                    if (nodeSet.contains(md.getColumnLabel(i))) {
                        throw new SQLException("存在重复的列名：" + md.getColumnLabel(i));
                    }
                    nodeSet.add(md.getColumnLabel(i));
                    if (jsonColumns.contains(md.getColumnLabel(i).toLowerCase())) {
                        try {
                            row.put(md.getColumnLabel(i), JsonUtils.fromJson(data.getString(i), Object.class));
                        } catch (Exception e) {
                            e.printStackTrace();
                            if (md.getColumnType(i) == Types.BOOLEAN) {
                                row.put(md.getColumnLabel(i), data.getBoolean(i));
                            } else {
                                row.put(md.getColumnLabel(i), data.getObject(i));
                            }
                        }
                    } else if (md.getColumnType(i) == Types.BOOLEAN) {
                        row.put(md.getColumnLabel(i), data.getBoolean(i));
                    } else {
                        row.put(md.getColumnLabel(i), data.getObject(i));
                    }
                }
                return row;
            }
            return new HashMap<>();
        } finally {
            if (data != null) {
                data.close();
            }
        }
    }

    public static void dropTable(String table, Connection connection) throws SQLException {
        var sql = "DROP TABLE " + table;
        try {
            var statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                System.err.println(e.getMessage());
            }
        }
    }
}
