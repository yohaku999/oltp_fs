package dev.yohaku.dbfs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

class DbfsStatementHandler extends DbfsProxyHandler {
    protected final DbfsConnectionState state;
    private ResultSet currentResultSet;
    private SQLWarning warning;
    private boolean closed;
    private int queryTimeoutSeconds;
    private int maxRows;
    private boolean closeOnCompletion;
    private boolean poolable;
    private long updateCount = -1;

    DbfsStatementHandler(DbfsConnectionState state) {
        this.state = state;
    }

    @FunctionalInterface
    protected interface SqlExecution<T> {
        T run() throws SQLException;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("unwrap")) {
            return unwrapStatement(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy);
        }
        if (name.equals("close")) {
            close();
            return null;
        }
        if (name.equals("isClosed")) {
            return closed;
        }
        if (name.equals("getConnection")) {
            ensureOpen();
            return state.connectionProxy();
        }
        if (name.equals("executeQuery") && args.length == 1) {
            ensureOpen();
            String sql = normalizedSql(args[0]);
            QueryResult result = executeWithTrace("QUERY", sql, () -> sql,
                    () -> state.client().executeQuery(sql, List.of()));
            currentResultSet = DbfsJdbcProxyFactory.newResultSet(result);
            updateCount = -1;
            return currentResultSet;
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate")) && args.length >= 1) {
            ensureOpen();
            String sql = normalizedSql(args[0]);
            int updated = executeWithTrace("UPDATE", sql, () -> sql,
                    () -> state.client().executeUpdate(sql, List.of()));
            currentResultSet = null;
            updateCount = updated;
            if (name.equals("executeLargeUpdate")) {
                return (long) updated;
            }
            return updated;
        }
        if (name.equals("execute") && args.length >= 1) {
            ensureOpen();
            String sql = normalizedSql(args[0]);
            if (looksLikeQuery(sql)) {
                QueryResult result = executeWithTrace("QUERY", sql, () -> sql,
                        () -> state.client().executeQuery(sql, List.of()));
                currentResultSet = DbfsJdbcProxyFactory.newResultSet(result);
                updateCount = -1;
                return true;
            }

            updateCount = executeWithTrace("UPDATE", sql, () -> sql,
                    () -> state.client().executeUpdate(sql, List.of()));
            currentResultSet = null;
            return false;
        }
        if (name.equals("getResultSet")) {
            ensureOpen();
            return currentResultSet;
        }
        if (name.equals("getUpdateCount")) {
            ensureOpen();
            return updateCount > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) updateCount;
        }
        if (name.equals("getLargeUpdateCount")) {
            ensureOpen();
            return updateCount;
        }
        if (name.equals("getMoreResults")) {
            ensureOpen();
            currentResultSet = null;
            updateCount = -1;
            return false;
        }
        if (name.equals("closeOnCompletion")) {
            closeOnCompletion = true;
            return null;
        }
        if (name.equals("isCloseOnCompletion")) {
            return closeOnCompletion;
        }
        if (name.equals("setPoolable")) {
            poolable = (Boolean) args[0];
            return null;
        }
        if (name.equals("isPoolable")) {
            return poolable;
        }
        if (name.equals("setQueryTimeout")) {
            queryTimeoutSeconds = (Integer) args[0];
            return null;
        }
        if (name.equals("getQueryTimeout")) {
            return queryTimeoutSeconds;
        }
        if (name.equals("setMaxRows")) {
            maxRows = (Integer) args[0];
            return null;
        }
        if (name.equals("getMaxRows")) {
            return maxRows;
        }
        if (name.equals("clearWarnings")) {
            warning = null;
            return null;
        }
        if (name.equals("getWarnings")) {
            return warning;
        }
        if (name.equals("cancel")) {
            return null;
        }
        if (name.equals("setFetchDirection")) {
            return null;
        }
        if (name.equals("getFetchDirection")) {
            return ResultSet.FETCH_FORWARD;
        }
        if (name.equals("setFetchSize")) {
            return null;
        }
        if (name.equals("getFetchSize")) {
            return 0;
        }
        if (name.equals("getResultSetConcurrency")) {
            return ResultSet.CONCUR_READ_ONLY;
        }
        if (name.equals("getResultSetType")) {
            return ResultSet.TYPE_FORWARD_ONLY;
        }
        if (name.equals("getResultSetHoldability")) {
            return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        }
        if (name.equals("clearBatch")) {
            return null;
        }
        if (name.equals("executeBatch")) {
            ensureOpen();
            return new int[0];
        }
        if (name.equals("executeLargeBatch")) {
            ensureOpen();
            return new long[0];
        }
        if (name.equals("addBatch")) {
            return null;
        }

        throw unsupported(method);
    }

    protected List<Object> orderedParameters(Map<Integer, Object> parameters) {
        if (parameters.isEmpty()) {
            return List.of();
        }

        int maxIndex = Collections.max(parameters.keySet());
        List<Object> ordered = new ArrayList<>(Collections.nCopies(maxIndex, null));
        for (Map.Entry<Integer, Object> entry : parameters.entrySet()) {
            ordered.set(entry.getKey() - 1, entry.getValue());
        }
        return Collections.unmodifiableList(new ArrayList<>(ordered));
    }

    protected void close() {
        closed = true;
        currentResultSet = null;
    }

    protected void setCurrentResultSet(ResultSet resultSet) {
        this.currentResultSet = resultSet;
    }

    protected void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw closed("Statement");
        }
        if (state.closed()) {
            throw closed("Connection");
        }
    }

    protected <T> T executeWithTrace(
            String kind,
            String shapeKey,
            Supplier<String> renderedSqlSupplier,
            SqlExecution<T> execution) throws SQLException {
        SqlTraceSampler.record(kind, shapeKey, renderedSqlSupplier);
        long startedAtNanos = System.nanoTime();
        try {
            T result = execution.run();
            SqlTraceSampler.recordTiming(
                    kind,
                    shapeKey,
                    renderedSqlSupplier,
                    elapsedMicros(startedAtNanos),
                    true,
                    null);
            return result;
        } catch (SQLException | RuntimeException exception) {
            SqlTraceSampler.recordTiming(
                    kind,
                    shapeKey,
                    renderedSqlSupplier,
                    elapsedMicros(startedAtNanos),
                    false,
                    exception.getClass().getSimpleName());
            throw exception;
        }
    }

    private long elapsedMicros(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1000;
    }

    private Object unwrapStatement(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap Statement to " + targetType.getName());
    }
}

final class DbfsPreparedStatementHandler extends DbfsStatementHandler {
    private final String sql;
    private final Map<Integer, Object> parameters = new LinkedHashMap<>();
    private final List<List<Object>> batchParameters = new ArrayList<>();

    DbfsPreparedStatementHandler(DbfsConnectionState state, String sql) {
        super(state);
        this.sql = sql;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("executeQuery") && args.length == 0) {
            ensureOpen();
            List<Object> ordered = orderedParameters(parameters);
            QueryResult result = executeWithTrace(
                    "QUERY",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> state.client().executeQuery(sql, ordered));
            ResultSet resultSet = DbfsJdbcProxyFactory.newResultSet(result);
            setCurrentResultSet(resultSet);
            setUpdateCount(-1);
            return resultSet;
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate")) && args.length == 0) {
            ensureOpen();
            List<Object> ordered = orderedParameters(parameters);
            int updated = executeWithTrace(
                    "UPDATE",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> state.client().executeUpdate(sql, ordered));
            setCurrentResultSet(null);
            setUpdateCount(updated);
            if (name.equals("executeLargeUpdate")) {
                return (long) updated;
            }
            return updated;
        }
        if (name.equals("execute") && args.length == 0) {
            ensureOpen();
            List<Object> ordered = orderedParameters(parameters);
            if (looksLikeQuery(sql)) {
                QueryResult result = executeWithTrace(
                        "QUERY",
                        sql,
                        () -> SqlLiteralRenderer.render(sql, ordered),
                        () -> state.client().executeQuery(sql, ordered));
                setCurrentResultSet(DbfsJdbcProxyFactory.newResultSet(result));
                setUpdateCount(-1);
                return true;
            }

            setUpdateCount(executeWithTrace(
                    "UPDATE",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> state.client().executeUpdate(sql, ordered)));
            setCurrentResultSet(null);
            return false;
        }
        if (name.equals("clearParameters")) {
            parameters.clear();
            return null;
        }
        if (name.startsWith("set") && args.length >= 2 && args[0] instanceof Integer) {
            Integer index = (Integer) args[0];
            parameters.put(index, normalizeParameterValue(name, args));
            return null;
        }
        if (name.equals("addBatch") && args.length == 0) {
            batchParameters.add(orderedParameters(parameters));
            return null;
        }
        if (name.equals("clearBatch")) {
            batchParameters.clear();
            return null;
        }
        if (name.equals("executeBatch")) {
            ensureOpen();
            int[] updateCounts = new int[batchParameters.size()];
            for (int index = 0; index < batchParameters.size(); index += 1) {
                List<Object> batch = batchParameters.get(index);
                updateCounts[index] = executeWithTrace(
                        "UPDATE",
                        sql,
                        () -> SqlLiteralRenderer.render(sql, batch),
                        () -> state.client().executeUpdate(sql, batch));
            }
            batchParameters.clear();
            setCurrentResultSet(null);
            setUpdateCount(updateCounts.length == 0 ? 0 : updateCounts[updateCounts.length - 1]);
            return updateCounts;
        }
        if (name.equals("executeLargeBatch")) {
            ensureOpen();
            long[] updateCounts = new long[batchParameters.size()];
            for (int index = 0; index < batchParameters.size(); index += 1) {
                List<Object> batch = batchParameters.get(index);
                updateCounts[index] = executeWithTrace(
                        "UPDATE",
                        sql,
                        () -> SqlLiteralRenderer.render(sql, batch),
                        () -> state.client().executeUpdate(sql, batch));
            }
            batchParameters.clear();
            setCurrentResultSet(null);
            setUpdateCount(updateCounts.length == 0 ? 0 : updateCounts[updateCounts.length - 1]);
            return updateCounts;
        }
        if (name.equals("addBatch") && args.length == 0) {
            return null;
        }

        return super.invokeJdbc(proxy, method, args);
    }

    private Object normalizeParameterValue(String methodName, Object[] args) throws SQLException {
        if (methodName.equals("setNull")) {
            return null;
        }
        if (methodName.equals("setObject") && args.length == 3 && args[2] instanceof Integer) {
            Integer targetSqlType = (Integer) args[2];
            return coerceSqlType(args[1], targetSqlType);
        }
        return args[1];
    }

    private Object coerceSqlType(Object value, int targetSqlType) throws SQLException {
        if (value == null) {
            return null;
        }

        switch (targetSqlType) {
            case Types.INTEGER:
                return ((Number) value).intValue();
            case Types.BIGINT:
                return ((Number) value).longValue();
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return ((Number) value).doubleValue();
            case Types.BOOLEAN:
            case Types.BIT:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return value.toString();
            default:
                throw new SQLFeatureNotSupportedException("Unsupported target SQL type: " + targetSqlType);
        }
    }
}