package dev.yohaku.dbfs.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Locale;
import java.util.Objects;

abstract class DbfsProxyHandler implements InvocationHandler {
    @Override
    public final Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return handleObjectMethod(proxy, method, args);
        }

        try {
            return invokeJdbc(proxy, method, args == null ? new Object[0] : args);
        } catch (InvocationTargetException exception) {
            throw exception.getTargetException();
        }
    }

    protected abstract Object invokeJdbc(Object proxy, Method method, Object[] args) throws Throwable;

    protected final Object handleObjectMethod(Object proxy, Method method, Object[] args) {
        switch (method.getName()) {
            case "toString":
                return proxy.getClass().getInterfaces()[0].getSimpleName() + "[dbfs]";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new IllegalStateException("Unexpected Object method: " + method.getName());
        }
    }

    protected final SQLFeatureNotSupportedException unsupported(Method method) {
        return new SQLFeatureNotSupportedException(method.getName() + " is not implemented yet");
    }

    protected final SQLException closed(String type) {
        return new SQLException(type + " is closed");
    }

    protected final String normalizedSql(Object sqlArgument) {
        return Objects.requireNonNull((String) sqlArgument, "sql").trim();
    }

    protected final boolean looksLikeQuery(String sql) {
        String normalized = sql.stripLeading().toLowerCase(Locale.ROOT);
        return normalized.startsWith("select") || normalized.startsWith("with");
    }
}