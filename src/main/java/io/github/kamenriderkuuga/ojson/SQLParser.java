package io.github.kamenriderkuuga.ojson;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import io.github.kamenriderkuuga.ojson.utils.JDBCUtils;
import io.github.kamenriderkuuga.ojson.utils.JsonUtils;
import io.github.kamenriderkuuga.ojson.utils.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * 基于SQL的数据解析类
 *
 * @author guohao
 * @date 2023/2/8
 */
public class SQLParser {

    /**
     * 基于SQL解析数据，并返回解析结果
     *
     * @param jsonData  JSON数据
     * @param sqlSelect SQL查询字段
     * @param sqlWhere  SQL查询条件
     * @return 解析结果
     */
    public static ParseResult parse(String jsonData, String sqlSelect, String sqlWhere) {
        var table = RandomUtils.nextRandomRandomAlphabetic(10);
        var result = new ParseResult();
        JsonNode root = null;
        try {
            root = JsonUtils.readTree(jsonData);
        } catch (Exception e) {
            result.setMessage(e.getMessage());
            return result;
        }
        var sql = "SELECT " + sqlSelect + " FROM " + table + (StringUtils.isEmpty(sqlWhere) ? "" : " WHERE " + sqlWhere);

        String dbType = JdbcConstants.H2.toString();
        SQLStatement stmt = SQLUtils.parseSingleStatement(sql, dbType);

        var selectList = ((SQLSelectQueryBlock) (((SQLSelectStatement) stmt).getSelect()).getQuery()).getSelectList();
        var where = ((SQLSelectQueryBlock) (((SQLSelectStatement) stmt).getSelect()).getQuery()).getWhere();

        JsonNode finalRoot = root;
        var fieldsNameMap = new HashMap<String, HashSet<String>>();
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(table).append(" (").append(System.getProperty("line.separator"));
        var tableNames = new HashSet<String>();
        var values = new ArrayList<Object>();
        var jsonColumns = new ArrayList<String>();

        for (var selectItem : selectList) {
            if (!(selectItem.getExpr() instanceof SQLIdentifierExpr) && !(selectItem.getExpr() instanceof SQLPropertyExpr)) {
                continue;
            }
            if (StringUtils.isEmpty(selectItem.getAlias()) || StringUtils.isEmpty(selectItem.getExpr().toString())) {
                continue;
            }
            if (!fieldsNameMap.containsKey(selectItem.getExpr().toString().toLowerCase())) {
                fieldsNameMap.put(selectItem.getExpr().toString().toLowerCase(), new HashSet<>());
            }
            fieldsNameMap.get(selectItem.getExpr().toString().toLowerCase()).add(selectItem.getAlias().toLowerCase());
        }

        appendMetaDataFromSQLExpr(where, finalRoot, createTableSQL, tableNames, values, jsonColumns, fieldsNameMap);

        for (var selectItem : selectList) {
            appendMetaDataFromSQLExpr(selectItem.getExpr(), finalRoot, createTableSQL, tableNames, values, jsonColumns, fieldsNameMap);
        }

        var fields = finalRoot.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            var columnName = entry.getKey();
            columnName = StringUtils.strip(columnName, "`");
            columnName = StringUtils.strip(columnName, "[");
            columnName = StringUtils.strip(columnName, "]");

            var lowerColumnName = columnName.toLowerCase();
            if (tableNames.contains(lowerColumnName)) {
                continue;
            }

            var node = entry.getValue();
            appendCreateTableSQL(createTableSQL, columnName, node, values, jsonColumns, fieldsNameMap);
            tableNames.add(lowerColumnName);
        }

        createTableSQL.append("___metadata___ TEXT").append(System.lineSeparator());
        createTableSQL.append(");");
        selectList.add(new SQLSelectItem(new SQLIdentifierExpr("___metadata___")));
        sql = stmt.toString();

        Connection connection = null;

        try {
            connection = JDBCUtils.getConnection("sqlite");
            JDBCUtils.executeSQL(createTableSQL.toString(), connection);
            JDBCUtils.insert(table, values, jsonColumns, connection);
            result.setSuccess(true);
            result.setData(JDBCUtils.select(sql, connection));
            return result;
        } catch (SQLException e) {
            result.setSuccess(false);
            result.setMessage(e.getMessage());
            return result;
        } finally {
            try {
                JDBCUtils.dropTable(table, connection);
            } catch (SQLException e) {
            }
        }
    }

    private static void appendMetaDataFromSQLExpr(SQLExpr sqlExpr, JsonNode root, StringBuilder createTableSQL,
                                                  HashSet<String> tableNames, ArrayList<Object> values, ArrayList<String> jsonColumns,
                                                  HashMap<String, HashSet<String>> fieldsNameMap) {
        if (sqlExpr == null) {
            return;
        }
        if ((sqlExpr instanceof SQLIdentifierExpr)) {
            var selectItem = (SQLIdentifierExpr) sqlExpr;
            var columnName = selectItem.toString();
            columnName = StringUtils.strip(columnName, "`");
            columnName = StringUtils.strip(columnName, "[");
            columnName = StringUtils.strip(columnName, "]");
            if (StringUtils.isEmpty(columnName)) {
                return;
            }
            selectItem.setName("`" + columnName + "`");

            var lowerColumnName = columnName.toLowerCase();
            if (tableNames.contains(lowerColumnName)) {
                return;
            }

            JsonNode node = findJsonNodeCaseInsensitive(root, lowerColumnName);
            appendCreateTableSQL(createTableSQL, columnName, node, values, jsonColumns, fieldsNameMap);

            tableNames.add(lowerColumnName);
        }

        if ((sqlExpr instanceof SQLPropertyExpr)) {
            var selectItem = (SQLPropertyExpr) sqlExpr;
            var columnName = selectItem.toString();
            columnName = StringUtils.strip(columnName, "`");
            columnName = StringUtils.strip(columnName, "[");
            columnName = StringUtils.strip(columnName, "]");
            selectItem.setName("`" + columnName + "`");
            selectItem.setOwner("");
            selectItem.setSplitString("");

            var lowerColumnName = columnName.toLowerCase();
            if (tableNames.contains(lowerColumnName)) {
                return;
            }

            JsonNode node = findJsonNodeCaseInsensitive(root, lowerColumnName);
            appendCreateTableSQL(createTableSQL, columnName, node, values, jsonColumns, fieldsNameMap);

            tableNames.add(lowerColumnName);
        }

        var children = sqlExpr.getChildren();
        for (var item : children) {
            if (item instanceof SQLPropertyExpr) {
                var selectItem = (SQLPropertyExpr) item;
                var columnName = selectItem.toString();
                columnName = StringUtils.strip(columnName, "`");
                columnName = StringUtils.strip(columnName, "[");
                columnName = StringUtils.strip(columnName, "]");
                selectItem.setName("`" + columnName + "`");
                selectItem.setOwner("");
                selectItem.setSplitString("");

                var lowerColumnName = columnName.toLowerCase();
                if (tableNames.contains(lowerColumnName)) {
                    continue;
                }

                JsonNode node = findJsonNodeCaseInsensitive(root, lowerColumnName);
                appendCreateTableSQL(createTableSQL, columnName, node, values, jsonColumns, fieldsNameMap);

                tableNames.add(lowerColumnName);
            } else {
                appendMetaDataFromSQLExpr((SQLExpr) item, root, createTableSQL, tableNames, values, jsonColumns, fieldsNameMap);
            }
        }
    }

    private static JsonNode findJsonNodeCaseInsensitive(JsonNode node, String lowerColumnName) {
        var fieldsName = lowerColumnName.split("\\.");

        for (var fieldName : fieldsName) {
            var fields = node.fields();
            var currentFind = false;
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                if (entry.getKey().toLowerCase().equals(fieldName)) {
                    node = entry.getValue();
                    currentFind = true;
                    break;
                }
            }
            if (!currentFind) {
                return MissingNode.getInstance();
            }
        }

        return node;
    }

    private static void appendCreateTableSQL(StringBuilder createTableSQL, String columnName, JsonNode node, ArrayList<Object> values, ArrayList<String> jsonColumns, HashMap<String, HashSet<String>> fieldsNameMap) {
        if (node instanceof MissingNode) {
            createTableSQL.append("`").append(columnName).append("`").append(" TEXT,");
            values.add(null);
        } else if (node.isTextual()) {
            createTableSQL.append("`").append(columnName).append("`").append(" TEXT,");
            values.add(node.asText());
        } else if (node.isNumber()) {
            createTableSQL.append("`").append(columnName).append("`").append(" DECIMAL,");
            values.add(node.asDouble());
        } else if (node.isBoolean()) {
            createTableSQL.append("`").append(columnName).append("`").append(" BOOLEAN,");
            values.add(node.asBoolean());
        } else if (node.isObject()) {
            createTableSQL.append("`").append(columnName).append("`").append(" TEXT,");
            jsonColumns.add(columnName.toLowerCase());
            if (fieldsNameMap.containsKey(columnName.toLowerCase())) {
                jsonColumns.addAll(fieldsNameMap.get(columnName.toLowerCase()));
            }
            values.add(node.toString());
        } else if (node.isArray()) {
            createTableSQL.append("`").append(columnName).append("`").append(" TEXT,");
            jsonColumns.add(columnName.toLowerCase());
            if (fieldsNameMap.containsKey(columnName.toLowerCase())) {
                jsonColumns.addAll(fieldsNameMap.get(columnName.toLowerCase()));
            }
            values.add(node.toString());
        } else if (node.isNull()) {
            createTableSQL.append("`").append(columnName).append("`").append(" TEXT,");
            values.add(null);
        }

        createTableSQL.append(System.getProperty("line.separator"));
    }
}
