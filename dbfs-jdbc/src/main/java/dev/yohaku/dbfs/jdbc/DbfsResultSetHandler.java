package dev.yohaku.dbfs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

final class DbfsResultSetHandler extends DbfsProxyHandler {
    private final QueryResult result;
    private boolean closed;
    private int rowIndex = -1;
    private Object lastValue;

    DbfsResultSetHandler(QueryResult result) {
        this.result = result;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("unwrap")) {
            return unwrapResultSet(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy);
        }
        if (name.equals("close")) {
            closed = true;
            return null;
        }
        if (name.equals("isClosed")) {
            return closed;
        }
        if (name.equals("next")) {
            ensureOpen();
            if (rowIndex + 1 >= result.rows().size()) {
                rowIndex = result.rows().size();
                return false;
            }
            rowIndex += 1;
            return true;
        }
        if (name.equals("beforeFirst")) {
            ensureOpen();
            rowIndex = -1;
            lastValue = null;
            return null;
        }
        if (name.equals("wasNull")) {
            return lastValue == null;
        }
        if (name.equals("getMetaData")) {
            ensureOpen();
            return DbfsJdbcProxyFactory.newResultSetMetaData(result);
        }
        if (name.equals("findColumn")) {
            ensureOpen();
            return findColumn((String) args[0]) + 1;
        }
        if (name.equals("getString")) {
            Object value = value(args[0]);
            return value == null ? null : String.valueOf(value);
        }
        if (name.equals("getInt")) {
            Object value = value(args[0]);
            return value == null ? 0 : ((Number) value).intValue();
        }
        if (name.equals("getShort")) {
            Object value = value(args[0]);
            return value == null ? (short) 0 : ((Number) value).shortValue();
        }
        if (name.equals("getLong")) {
            Object value = value(args[0]);
            return value == null ? 0L : ((Number) value).longValue();
        }
        if (name.equals("getFloat")) {
            Object value = value(args[0]);
            return value == null ? 0.0f : ((Number) value).floatValue();
        }
        if (name.equals("getDouble")) {
            Object value = value(args[0]);
            return value == null ? 0.0d : ((Number) value).doubleValue();
        }
        if (name.equals("getTimestamp")) {
            Object value = value(args[0]);
            return value == null ? null : java.sql.Timestamp.class.cast(value);
        }
        if (name.equals("getBoolean")) {
            Object value = value(args[0]);
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        }
        if (name.equals("getObject") && args.length == 1) {
            return value(args[0]);
        }
        if (name.equals("getObject") && args.length == 2 && args[1] instanceof Class<?>) {
            Class<?> targetType = (Class<?>) args[1];
            Object rawValue = value(args[0]);
            if (rawValue == null) {
                return null;
            }
            return targetType.cast(rawValue);
        }
        if (name.equals("getFetchDirection")) {
            return ResultSet.FETCH_FORWARD;
        }
        if (name.equals("getType")) {
            return ResultSet.TYPE_FORWARD_ONLY;
        }
        if (name.equals("getConcurrency")) {
            return ResultSet.CONCUR_READ_ONLY;
        }
        if (name.equals("rowUpdated") || name.equals("rowInserted") || name.equals("rowDeleted")) {
            return false;
        }

        throw unsupported(method);
    }

    private Object unwrapResultSet(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap ResultSet to " + targetType.getName());
    }

    private Object value(Object columnSpecifier) throws SQLException {
        ensureOpen();
        List<Object> row = currentRow();

        int columnIndex;
        if (columnSpecifier instanceof Integer) {
            Integer integerIndex = (Integer) columnSpecifier;
            columnIndex = integerIndex - 1;
        } else {
            columnIndex = findColumn((String) columnSpecifier);
        }

        if (columnIndex < 0 || columnIndex >= row.size()) {
            throw new SQLException("Column index out of range: " + (columnIndex + 1));
        }

        lastValue = row.get(columnIndex);
        return lastValue;
    }

    private int findColumn(String label) throws SQLException {
        for (int index = 0; index < result.columns().size(); index += 1) {
            if (result.columns().get(index).equalsIgnoreCase(label)) {
                return index;
            }
        }
        throw new SQLException("Unknown column label: " + label);
    }

    private List<Object> currentRow() throws SQLException {
        if (rowIndex < 0 || rowIndex >= result.rows().size()) {
            throw new SQLException("ResultSet cursor is not positioned on a row");
        }
        return result.rows().get(rowIndex);
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw closed("ResultSet");
        }
    }
}