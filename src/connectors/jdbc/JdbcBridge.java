import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Minimal JDBC bridge for Node.js  JDBC communication via stdin/stdout JSON.
 *
 * Usage:
 *   java -cp driver.jar JdbcBridge <driverClass> <jdbcUrl> <username> <password> [maxConnections] [timeoutMs]
 *
 * Supported stdin commands (one JSON object per line):
 *   {"action":"test"}
 *   {"action":"query","sql":"SELECT ...","params":["val1",2],"readonly":true,"maxRows":100}
 *   {"action":"schemas"}
 *   {"action":"tables","schema":"public"}
 *   {"action":"views","schema":"public"}
 *   {"action":"columns","schema":"public","table":"users"}
 *   {"action":"indexes","schema":"public","table":"users"}
 *   {"action":"procedures","schema":"public"}
 *   {"action":"procedure_detail","schema":"public","name":"myproc"}
 *   {"action":"table_exists","schema":"public","table":"users"}
 *   {"action":"table_row_count","schema":"public","table":"users"}
 *   {"action":"table_comment","schema":"public","table":"users"}
 *   {"action":"disconnect"}
 *
 * stdout responses (one JSON object per line):
 *   {"type":"ok"}
 *   {"type":"result","rows":[...],"rowCount":N}
 *   {"type":"error","message":"..."}
 *   {"type":"schemas","schemas":["public",...]}
 *   {"type":"tables","tables":["users",...]}
 *   {"type":"views","views":["user_view",...]}
 *   {"type":"columns","columns":[{"column_name":"id","data_type":"int4",...},...]}
 *   {"type":"indexes","indexes":[{"index_name":"pk_users","column_names":["id"],...},...]}
 *   {"type":"procedures","procedures":["myfunc",...]}
 *   {"type":"procedure_detail","detail":{...}}
 *   {"type":"table_exists","exists":true}
 *   {"type":"table_row_count","count":1234}
 *   {"type":"table_comment","comment":"User table"}
 */
public class JdbcBridge {
    private static final PrintWriter stdout = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
    private static final BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    private static Connection conn;
    private static DatabaseMetaData meta;
    private static int maxConnections = 5;
    private static int timeoutMs = 30000;
    private static final int DEFAULT_MAX_ROWS = 100;
    private static final Set<String> BLOCKED_COMMANDS = new HashSet<>(Arrays.asList(
        "DELETE", "DROP", "TRUNCATE", "ALTER", "INSERT", "UPDATE",
        "CREATE", "REPLACE", "GRANT", "REVOKE", "RENAME", "MERGE"
    ));
    private static final Map<String, String> escaped = new HashMap<>();

    public static void main(String[] args) {
        if (args.length < 4) {
            sendError("Usage: JdbcBridge <driverClass> <jdbcUrl> <username> <password> [maxConnections] [timeoutMs]");
            System.exit(1);
        }

        String driverClass = args[0];
        String jdbcUrl = args[1];
        String username = args[2];
        String password = args[3];
        if (args.length >= 5) maxConnections = Integer.parseInt(args[4]);
        if (args.length >= 6) timeoutMs = Integer.parseInt(args[5]);

        try {
            // Load driver from classpath (JAR must be on -cp)
            Class.forName(driverClass);

            DriverManager.setLoginTimeout(timeoutMs / 1000);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            conn.setReadOnly(false); // read-only applied per-query via executeOptions
            meta = conn.getMetaData();

            sendOk();
        } catch (Exception e) {
            sendError("Failed to connect: " + e.getMessage());
            System.exit(1);
            return;
        }

        // Process commands from stdin
        try {
            String line;
            while ((line = stdin.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                try {
                    processCommand(line);
                } catch (Exception e) {
                    sendError("Command error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            // stdin closed, exit gracefully
        } finally {
            disconnect();
        }
    }

    private static void processCommand(String line) {
        Map<String, Object> cmd = parseSimpleJson(line);
        String action = (String) cmd.get("action");
        if (action == null) {
            sendError("Missing 'action' field");
            return;
        }

        try {
            switch (action) {
                case "test":
                    handleTest();
                    break;
                case "query":
                    handleQuery(cmd);
                    break;
                case "schemas":
                    handleSchemas();
                    break;
                case "tables":
                    handleTables(cmd);
                    break;
                case "views":
                    handleViews(cmd);
                    break;
                case "columns":
                    handleColumns(cmd);
                    break;
                case "indexes":
                    handleIndexes(cmd);
                    break;
                case "procedures":
                    handleProcedures(cmd);
                    break;
                case "procedure_detail":
                    handleProcedureDetail(cmd);
                    break;
                case "table_exists":
                    handleTableExists(cmd);
                    break;
                case "table_row_count":
                    handleTableRowCount(cmd);
                    break;
                case "table_comment":
                    handleTableComment(cmd);
                    break;
                case "disconnect":
                    disconnect();
                    sendOk();
                    System.exit(0);
                    break;
                default:
                    sendError("Unknown action: " + action);
            }
        } catch (SQLException e) {
            sendError("SQL error: " + e.getMessage());
        } catch (Exception e) {
            sendError("Error: " + e.getMessage());
        }
    }

    private static void handleTest() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(timeoutMs / 1000);
            stmt.execute("SELECT 1");
        }
        sendOk();
    }

    private static void handleQuery(Map<String, Object> cmd) throws SQLException {
        String sql = (String) cmd.get("sql");
        if (sql == null) {
            sendError("Missing 'sql' field");
            return;
        }
        @SuppressWarnings("unchecked")
        List<Object> params = (List<Object>) cmd.get("params");
        Boolean readonly = (Boolean) cmd.get("readonly");
        Number maxRows = (Number) cmd.get("maxRows");

        // Resolve effective maxRows: user-specified > default 100
        int effectiveMaxRows = (maxRows != null && maxRows.intValue() > 0)
            ? maxRows.intValue()
            : DEFAULT_MAX_ROWS;

        // Split statements and execute
        String[] statements = sql.split(";");
        List<Map<String, Object>> allRows = new ArrayList<>();
        int totalRowCount = 0;

        boolean wasReadOnly = false;
        if (readonly != null && readonly) {
            wasReadOnly = conn.isReadOnly();
            conn.setReadOnly(true);
        }

        try {
            for (String stmt : statements) {
                String trimmed = stmt.trim();
                if (trimmed.isEmpty()) continue;

                // Safety check: block dangerous SQL commands
                String firstWord = getFirstKeyword(trimmed);
                if (BLOCKED_COMMANDS.contains(firstWord)) {
                    sendError("Blocked dangerous operation: " + firstWord
                        + ". Only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH/CALL queries are allowed.");
                    return;
                }

                // Apply LIMIT for SELECT statements
                String processedSql = trimmed;
                String upper = trimmed.toUpperCase();
                boolean isSelect = upper.startsWith("SELECT") || upper.startsWith("WITH")
                    || upper.startsWith("SHOW") || upper.startsWith("DESCRIBE")
                    || upper.startsWith("EXPLAIN") || upper.startsWith("CALL");

                if (isSelect && !upper.contains("LIMIT") && !upper.contains("FETCH FIRST") && !upper.contains("FETCH NEXT")) {
                    processedSql = trimmed + " LIMIT " + effectiveMaxRows;
                }

                try (PreparedStatement ps = conn.prepareStatement(processedSql)) {
                    ps.setQueryTimeout(timeoutMs / 1000);

                    // Set parameters if provided
                    if (params != null) {
                        for (int i = 0; i < params.size(); i++) {
                            setParameter(ps, i + 1, params.get(i));
                        }
                    }

                    boolean isResultSet = ps.execute();
                    if (isResultSet) {
                        try (ResultSet rs = ps.getResultSet()) {
                            ResultSetMetaData rsmd = rs.getMetaData();
                            int colCount = rsmd.getColumnCount();
                            while (rs.next()) {
                                Map<String, Object> row = new LinkedHashMap<>();
                                for (int i = 1; i <= colCount; i++) {
                                    row.put(rsmd.getColumnLabel(i), rs.getObject(i));
                                }
                                allRows.add(row);
                            }
                        }
                    }
                    int updateCount = ps.getUpdateCount();
                    if (updateCount >= 0) {
                        totalRowCount += updateCount;
                    }
                }
            }
        } finally {
            if (readonly != null && readonly) {
                conn.setReadOnly(wasReadOnly);
            }
        }

        // If there are rows from SELECT, rowCount = number of rows; otherwise use totalRowCount
        int finalRowCount = allRows.isEmpty() ? totalRowCount : allRows.size();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "result");
        response.put("rows", allRows);
        response.put("rowCount", finalRowCount);
        stdout.println(toJson(response));
    }

    /**
     * Extracts the first SQL keyword from a statement, skipping comments.
     */
    private static String getFirstKeyword(String sql) {
        sql = sql.trim();
        // Skip single-line comments (-- ...)
        while (sql.startsWith("--")) {
            int nl = sql.indexOf('\n');
            if (nl == -1) return "";
            sql = sql.substring(nl + 1).trim();
        }
        // Skip block comments (/* ... */)
        while (sql.startsWith("/*")) {
            int end = sql.indexOf("*/");
            if (end == -1) return "";
            sql = sql.substring(end + 2).trim();
        }
        // Get first word (until space, tab, newline, or '(')
        StringBuilder first = new StringBuilder();
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c) || c == '(' || c == ';') break;
            first.append(Character.toUpperCase(c));
        }
        return first.toString();
    }

    private static void handleSchemas() throws SQLException {
        List<String> schemas = new ArrayList<>();
        try (ResultSet rs = meta.getSchemas()) {
            while (rs.next()) {
                schemas.add(rs.getString("TABLE_SCHEM"));
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "schemas");
        response.put("schemas", schemas);
        stdout.println(toJson(response));
    }

    private static void handleTables(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"TABLE"})) {
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "tables");
        response.put("tables", tables);
        stdout.println(toJson(response));
    }

    private static void handleViews(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        List<String> views = new ArrayList<>();
        try (ResultSet rs = meta.getTables(null, schema, "%", new String[]{"VIEW"})) {
            while (rs.next()) {
                views.add(rs.getString("TABLE_NAME"));
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "views");
        response.put("views", views);
        stdout.println(toJson(response));
    }

    private static void handleColumns(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        String table = (String) cmd.get("table");
        if (table == null) {
            sendError("Missing 'table' field");
            return;
        }
        List<Map<String, Object>> columns = new ArrayList<>();
        try (ResultSet rs = meta.getColumns(null, schema, table, "%")) {
            while (rs.next()) {
                Map<String, Object> col = new LinkedHashMap<>();
                col.put("column_name", rs.getString("COLUMN_NAME"));
                col.put("data_type", rs.getString("TYPE_NAME"));
                col.put("is_nullable", rs.getString("IS_NULLABLE"));
                col.put("column_default", rs.getString("COLUMN_DEF"));
                col.put("description", rs.getString("REMARKS"));
                columns.add(col);
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "columns");
        response.put("columns", columns);
        stdout.println(toJson(response));
    }

    private static void handleIndexes(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        String table = (String) cmd.get("table");
        if (table == null) {
            sendError("Missing 'table' field");
            return;
        }

        // Collect indexes grouped by name
        Map<String, Map<String, Object>> indexMap = new LinkedHashMap<>();
        try (ResultSet rs = meta.getIndexInfo(null, schema, table, false, false)) {
            while (rs.next()) {
                String idxName = rs.getString("INDEX_NAME");
                if (idxName == null) continue; // skip table stats entries

                Map<String, Object> idx = indexMap.get(idxName);
                if (idx == null) {
                    idx = new LinkedHashMap<>();
                    idx.put("index_name", idxName);
                    idx.put("column_names", new ArrayList<String>());
                    idx.put("is_unique", !rs.getBoolean("NON_UNIQUE"));
                    idx.put("is_primary", idxName.equalsIgnoreCase(rs.getString("TABLE_NAME") + "_pkey") ||
                            idxName.toUpperCase().startsWith("PK_") ||
                            idxName.equalsIgnoreCase("PRIMARY"));
                    indexMap.put(idxName, idx);
                }
                @SuppressWarnings("unchecked")
                List<String> colNames = (List<String>) idx.get("column_names");
                String colName = rs.getString("COLUMN_NAME");
                if (colName != null) {
                    colNames.add(colName);
                }
            }
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "indexes");
        response.put("indexes", new ArrayList<>(indexMap.values()));
        stdout.println(toJson(response));
    }

    private static void handleProcedures(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        List<String> procedures = new ArrayList<>();
        try (ResultSet rs = meta.getProcedures(null, schema, "%")) {
            while (rs.next()) {
                procedures.add(rs.getString("PROCEDURE_NAME"));
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "procedures");
        response.put("procedures", procedures);
        stdout.println(toJson(response));
    }

    private static void handleProcedureDetail(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        String name = (String) cmd.get("name");
        if (name == null) {
            sendError("Missing 'name' field");
            return;
        }

        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("procedure_name", name);
        detail.put("procedure_type", "procedure");

        // Get procedure columns (parameters)
        List<String> params = new ArrayList<>();
        try (ResultSet rs = meta.getProcedureColumns(null, schema, name, "%")) {
            while (rs.next()) {
                short colType = rs.getShort("COLUMN_TYPE");
                String paramName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                String prefix = colType == DatabaseMetaData.procedureColumnIn ? "IN " :
                               colType == DatabaseMetaData.procedureColumnOut ? "OUT " :
                               colType == DatabaseMetaData.procedureColumnInOut ? "INOUT " : "";
                params.add(prefix + paramName + " " + typeName);
            }
        }
        // If no params found via getProcedureColumns, try getFunctions
        if (params.isEmpty()) {
            try (ResultSet rs = meta.getFunctionColumns(null, schema, name, "%")) {
                while (rs.next()) {
                    short colType = rs.getShort("COLUMN_TYPE");
                    String paramName = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    String prefix = colType == DatabaseMetaData.functionColumnIn ? "IN " :
                                   colType == DatabaseMetaData.functionReturn ? "RETURN " :
                                   colType == DatabaseMetaData.functionColumnOut ? "OUT " : "";
                    params.add(prefix + paramName + " " + typeName);
                }
            } catch (Exception ignored) {}
        }

        detail.put("parameter_list", String.join(", ", params));
        detail.put("language", "SQL");

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "procedure_detail");
        response.put("detail", detail);
        stdout.println(toJson(response));
    }

    private static void handleTableExists(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        String table = (String) cmd.get("table");
        if (table == null) {
            sendError("Missing 'table' field");
            return;
        }
        boolean exists = false;
        try (ResultSet rs = meta.getTables(null, schema, table, null)) {
            exists = rs.next();
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "table_exists");
        response.put("exists", exists);
        stdout.println(toJson(response));
    }

    private static void handleTableRowCount(Map<String, Object> cmd) {
        String schema = (String) cmd.get("schema");
        String table = (String) cmd.get("table");
        if (table == null) {
            sendError("Missing 'table' field");
            return;
        }
        try {
            String fullName = schema != null ? quoteIdentifier(schema) + "." + quoteIdentifier(table) : quoteIdentifier(table);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fullName)) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("type", "table_row_count");
                    response.put("count", count);
                    stdout.println(toJson(response));
                }
            }
        } catch (Exception e) {
            // Fallback: return null
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("type", "table_row_count");
            response.put("count", null);
            stdout.println(toJson(response));
        }
    }

    private static void handleTableComment(Map<String, Object> cmd) throws SQLException {
        String schema = (String) cmd.get("schema");
        String table = (String) cmd.get("table");
        if (table == null) {
            sendError("Missing 'table' field");
            return;
        }
        String comment = null;
        try (ResultSet rs = meta.getTables(null, schema, table, null)) {
            if (rs.next()) {
                comment = rs.getString("REMARKS");
            }
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("type", "table_comment");
        response.put("comment", comment);
        stdout.println(toJson(response));
    }

    private static void setParameter(PreparedStatement ps, int index, Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            ps.setString(index, (String) value);
        } else if (value instanceof Integer) {
            ps.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            ps.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            ps.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            ps.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            ps.setBoolean(index, (Boolean) value);
        } else if (value instanceof BigDecimal) {
            ps.setBigDecimal(index, (BigDecimal) value);
        } else if (value instanceof java.sql.Date) {
            ps.setDate(index, (java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
            ps.setTimestamp(index, (java.sql.Timestamp) value);
        } else {
            ps.setObject(index, value);
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static void disconnect() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException ignored) {}
    }

    private static void sendOk() {
        stdout.println("{\"type\":\"ok\"}");
    }

    private static void sendError(String message) {
        Map<String, Object> err = new LinkedHashMap<>();
        err.put("type", "error");
        err.put("message", message);
        stdout.println(toJson(err));
    }

    // ---- Minimal JSON parser (avoids extra dependencies) ----

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseSimpleJson(String json) {
        Map<String, Object> result = new LinkedHashMap<>();
        json = json.trim();
        if (!json.startsWith("{") || !json.endsWith("}")) return result;
        json = json.substring(1, json.length() - 1).trim();
        if (json.isEmpty()) return result;

        // Split by commas, respecting strings and nesting
        List<String> pairs = splitJsonPairs(json);
        for (String pair : pairs) {
            int colon = findUnquotedColon(pair);
            if (colon == -1) continue;
            String key = unquote(pair.substring(0, colon).trim());
            String rawValue = pair.substring(colon + 1).trim();
            result.put(key, parseJsonValue(rawValue));
        }
        return result;
    }

    private static List<String> splitJsonPairs(String json) {
        List<String> pairs = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (inString) {
                current.append(c);
                if (c == '"' && json.charAt(i - 1) != '\\') inString = false;
            } else {
                if (c == '"') inString = true;
                else if (c == '{' || c == '[') depth++;
                else if (c == '}' || c == ']') depth--;
                if (c == ',' && depth == 0) {
                    pairs.add(current.toString());
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            }
        }
        if (current.length() > 0) pairs.add(current.toString());
        return pairs;
    }

    private static int findUnquotedColon(String s) {
        boolean inString = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) inString = !inString;
            if (c == ':' && !inString) return i;
        }
        return -1;
    }

    private static Object parseJsonValue(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        raw = raw.trim();
        if (raw.equals("null")) return null;
        if (raw.equals("true")) return true;
        if (raw.equals("false")) return false;
        if (raw.startsWith("\"")) return unquote(raw);
        if (raw.startsWith("[")) {
            List<Object> list = new ArrayList<>();
            String inner = raw.substring(1, raw.length() - 1).trim();
            if (!inner.isEmpty()) {
                for (String item : splitJsonPairs(inner)) {
                    list.add(parseJsonValue(item));
                }
            }
            return list;
        }
        if (raw.startsWith("{")) return parseSimpleJson(raw);
        // Try number
        try {
            if (raw.contains(".")) return Double.parseDouble(raw);
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return raw;
        }
    }

    private static String unquote(String s) {
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
            s = s.replace("\\\"", "\"").replace("\\\\", "\\")
                 .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }
        return s;
    }

    // ---- Minimal JSON serializer ----

    private static String toJson(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof String) return "\"" + escapeJson((String) obj) + "\"";
        if (obj instanceof Boolean || obj instanceof Number) return obj.toString();
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
                sb.append(toJson(entry.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }
        if (obj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) obj;
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(toJson(list.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
        return "\"" + escapeJson(obj.toString()) + "\"";
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }
}
