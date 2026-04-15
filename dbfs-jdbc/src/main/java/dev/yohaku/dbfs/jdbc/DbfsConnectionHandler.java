package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Properties;

final class DbfsConnectionHandler extends DbfsProxyHandler {
    private final DbfsConnectionState state;
    private SQLWarning warning;

    DbfsConnectionHandler(DbfsConnectionState state) {
        this.state = state;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("unwrap")) {
            return unwrapConnection(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy);
        }
        if (name.equals("close")) {
            state.close();
            return null;
        }
        if (name.equals("isClosed")) {
            return state.closed();
        }
        if (name.equals("isValid")) {
            return !state.closed();
        }
        if (name.equals("createStatement")) {
            ensureOpen();
            return DbfsJdbcProxyFactory.newStatement(state);
        }
        if (name.equals("prepareStatement") && args.length >= 1 && args[0] instanceof String) {
            ensureOpen();
            return DbfsJdbcProxyFactory.newPreparedStatement(state, normalizedSql(args[0]));
        }
        if (name.equals("prepareCall")) {
            throw unsupported(method);
        }
        if (name.equals("nativeSQL")) {
            ensureOpen();
            return normalizedSql(args[0]);
        }
        if (name.equals("setAutoCommit")) {
            ensureOpen();
            state.setAutoCommit((Boolean) args[0]);
            return null;
        }
        if (name.equals("getAutoCommit")) {
            ensureOpen();
            return state.autoCommit();
        }
        if (name.equals("commit") || name.equals("rollback")) {
            ensureOpen();
            return null;
        }
        if (name.equals("setReadOnly")) {
            ensureOpen();
            state.setReadOnly((Boolean) args[0]);
            return null;
        }
        if (name.equals("isReadOnly")) {
            ensureOpen();
            return state.readOnly();
        }
        if (name.equals("setSchema")) {
            ensureOpen();
            state.setSchema((String) args[0]);
            return null;
        }
        if (name.equals("getSchema")) {
            ensureOpen();
            return state.schema();
        }
        if (name.equals("setCatalog")) {
            ensureOpen();
            state.setCatalog((String) args[0]);
            return null;
        }
        if (name.equals("getCatalog")) {
            ensureOpen();
            return state.catalog();
        }
        if (name.equals("clearWarnings")) {
            warning = null;
            return null;
        }
        if (name.equals("getWarnings")) {
            return warning;
        }
        if (name.equals("getTransactionIsolation")) {
            return Connection.TRANSACTION_NONE;
        }
        if (name.equals("setTransactionIsolation")) {
            return null;
        }
        if (name.equals("setNetworkTimeout")) {
            ensureOpen();
            state.setNetworkTimeoutMillis((Integer) args[1]);
            return null;
        }
        if (name.equals("getNetworkTimeout")) {
            ensureOpen();
            return state.networkTimeoutMillis();
        }
        if (name.equals("abort")) {
            state.close();
            return null;
        }
        if (name.equals("setClientInfo")) {
            setClientInfo(args);
            return null;
        }
        if (name.equals("getClientInfo") && args.length == 0) {
            Properties copy = new Properties();
            copy.putAll(state.properties());
            return copy;
        }
        if (name.equals("getClientInfo") && args.length == 1) {
            return state.properties().getProperty((String) args[0]);
        }
        if (name.equals("getMetaData")) {
            ensureOpen();
            return DbfsJdbcProxyFactory.newDatabaseMetaData(state);
        }
        if (name.equals("getHoldability")) {
            return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        }
        if (name.equals("setHoldability")) {
            return null;
        }
        if (name.equals("getTypeMap")) {
            return new HashMap<String, Class<?>>();
        }
        if (name.equals("setTypeMap")) {
            return null;
        }

        throw unsupported(method);
    }

    private Object unwrapConnection(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap Connection to " + targetType.getName());
    }

    private void setClientInfo(Object[] args) throws SQLClientInfoException {
        if (args.length == 2 && args[0] instanceof String) {
            String key = (String) args[0];
            if (args[1] == null) {
                state.properties().remove(key);
            } else {
                state.properties().setProperty(key, String.valueOf(args[1]));
            }
            return;
        }

        if (args.length == 1 && args[0] instanceof Properties) {
            Properties properties = (Properties) args[0];
            state.properties().clear();
            state.properties().putAll(properties);
            return;
        }

        throw new SQLClientInfoException();
    }

    private void ensureOpen() throws SQLException {
        if (state.closed()) {
            throw closed("Connection");
        }
    }
}