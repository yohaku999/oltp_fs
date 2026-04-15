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
            QueryResult result = state.client().executeQuery(normalizedSql(args[0]), List.of());
            currentResultSet = DbfsJdbcProxyFactory.newResultSet(result);
            updateCount = -1;
            return currentResultSet;
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate")) && args.length >= 1) {
            ensureOpen();
            int updated = state.client().executeUpdate(normalizedSql(args[0]), List.of());
            currentResultSet = null;
            updateCount = updated;
            return name.equals("executeLargeUpdate") ? (long) updated : updated;
        }
        if (name.equals("execute") && args.length >= 1) {
            ensureOpen();
            String sql = normalizedSql(args[0]);
            if (looksLikeQuery(sql)) {
                currentResultSet = DbfsJdbcProxyFactory.newResultSet(state.client().executeQuery(sql, List.of()));
                updateCount = -1;
                return true;
            }

            updateCount = state.client().executeUpdate(sql, List.of());
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

    DbfsPreparedStatementHandler(DbfsConnectionState state, String sql) {
        super(state);
        this.sql = sql;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("executeQuery") && args.length == 0) {
            ensureOpen();
            ResultSet resultSet = DbfsJdbcProxyFactory.newResultSet(state.client().executeQuery(sql, orderedParameters(parameters)));
            setCurrentResultSet(resultSet);
            setUpdateCount(-1);
            return resultSet;
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate")) && args.length == 0) {
            ensureOpen();
            int updated = state.client().executeUpdate(sql, orderedParameters(parameters));
            setCurrentResultSet(null);
            setUpdateCount(updated);
            return name.equals("executeLargeUpdate") ? (long) updated : updated;
        }
        if (name.equals("execute") && args.length == 0) {
            ensureOpen();
            if (looksLikeQuery(sql)) {
                setCurrentResultSet(DbfsJdbcProxyFactory.newResultSet(state.client().executeQuery(sql, orderedParameters(parameters))));
                setUpdateCount(-1);
                return true;
            }

            setUpdateCount(state.client().executeUpdate(sql, orderedParameters(parameters)));
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