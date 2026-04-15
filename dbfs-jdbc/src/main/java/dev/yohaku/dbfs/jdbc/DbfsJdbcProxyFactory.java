package dev.yohaku.dbfs.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

final class DbfsJdbcProxyFactory {
    private DbfsJdbcProxyFactory() {
    }

    static Connection newConnection(DbfsConnectionState state) {
        return newProxy(Connection.class, new DbfsConnectionHandler(state));
    }

    static Statement newStatement(DbfsConnectionState state) {
        return newProxy(Statement.class, new DbfsStatementHandler(state));
    }

    static PreparedStatement newPreparedStatement(DbfsConnectionState state, String sql) {
        return newProxy(PreparedStatement.class, new DbfsPreparedStatementHandler(state, sql));
    }

    static ResultSet newResultSet(QueryResult result) {
        return newProxy(ResultSet.class, new DbfsResultSetHandler(result));
    }

    static DatabaseMetaData newDatabaseMetaData(DbfsConnectionState state) {
        return newProxy(DatabaseMetaData.class, new DbfsDatabaseMetaDataHandler(state));
    }

    static ResultSetMetaData newResultSetMetaData(QueryResult result) {
        return newProxy(ResultSetMetaData.class, new DbfsResultSetMetaDataHandler(result));
    }

    private static <T> T newProxy(Class<T> iface, InvocationHandler handler) {
        Object proxy = Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[]{iface}, handler);
        return iface.cast(proxy);
    }
}