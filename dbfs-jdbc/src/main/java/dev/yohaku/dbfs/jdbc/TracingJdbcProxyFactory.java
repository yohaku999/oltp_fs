package dev.yohaku.dbfs.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

final class TracingJdbcProxyFactory {
    private TracingJdbcProxyFactory() {
    }

    static Connection newConnection(Connection delegate) {
        TracingConnectionHandler handler = new TracingConnectionHandler(delegate);
        Connection proxy = newProxy(Connection.class, handler);
        handler.setConnectionProxy(proxy);
        return proxy;
    }

    static Statement newStatement(Connection connectionProxy, Statement delegate) {
        return newProxy(Statement.class, new TracingStatementHandler(connectionProxy, delegate));
    }

    static PreparedStatement newPreparedStatement(Connection connectionProxy, PreparedStatement delegate, String sql) {
        return newProxy(PreparedStatement.class,
                new TracingPreparedStatementHandler(connectionProxy, delegate, sql));
    }

    private static <T> T newProxy(Class<T> iface, InvocationHandler handler) {
        Object proxy = Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[]{iface}, handler);
        return iface.cast(proxy);
    }
}

abstract class TracingJdbcProxyHandler implements InvocationHandler {
    private final Object delegate;

    TracingJdbcProxyHandler(Object delegate) {
        this.delegate = delegate;
    }

    @Override
    public final Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) {
            return handleObjectMethod(proxy, method, args == null ? new Object[0] : args);
        }

        try {
            return invokeJdbc(proxy, method, args == null ? new Object[0] : args);
        } catch (InvocationTargetException exception) {
            throw exception.getTargetException();
        }
    }

    protected abstract Object invokeJdbc(Object proxy, Method method, Object[] args) throws Throwable;

    protected final Object invokeDelegate(Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(delegate, args);
        } catch (InvocationTargetException exception) {
            throw exception.getTargetException();
        }
    }

    protected final Object delegate() {
        return delegate;
    }

    protected final Object handleObjectMethod(Object proxy, Method method, Object[] args) {
        switch (method.getName()) {
            case "toString":
                return delegate + "[tracing]";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new IllegalStateException("Unexpected Object method: " + method.getName());
        }
    }

    protected final String normalizedSql(Object sqlArgument) {
        return Objects.requireNonNull((String) sqlArgument, "sql").trim();
    }

    protected final boolean looksLikeQuery(String sql) {
        String normalized = sql.stripLeading().toLowerCase(Locale.ROOT);
        return normalized.startsWith("select") || normalized.startsWith("with");
    }
}

final class TracingConnectionHandler extends TracingJdbcProxyHandler {
    private final Connection delegate;
    private Connection connectionProxy;

    TracingConnectionHandler(Connection delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    void setConnectionProxy(Connection connectionProxy) {
        this.connectionProxy = connectionProxy;
    }

    @Override
    protected Object invokeJdbc(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("unwrap")) {
            return unwrapConnection(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy) || ((Class<?>) args[0]).isInstance(delegate);
        }
        if (name.startsWith("createStatement")) {
            Statement statement = (Statement) invokeDelegate(method, args);
            return TracingJdbcProxyFactory.newStatement(connectionProxy, statement);
        }
        if (name.startsWith("prepareStatement") && args.length >= 1 && args[0] instanceof String) {
            PreparedStatement statement = (PreparedStatement) invokeDelegate(method, args);
            return TracingJdbcProxyFactory.newPreparedStatement(connectionProxy, statement, normalizedSql(args[0]));
        }

        return invokeDelegate(method, args);
    }

    private Object unwrapConnection(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        if (targetType.isInstance(delegate)) {
            return delegate;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap Connection to " + targetType.getName());
    }
}

class TracingStatementHandler extends TracingJdbcProxyHandler {
    @FunctionalInterface
    protected interface SqlExecution<T> {
        T run() throws Throwable;
    }

    private final Connection connectionProxy;
    private final Statement delegate;

    TracingStatementHandler(Connection connectionProxy, Statement delegate) {
        super(delegate);
        this.connectionProxy = connectionProxy;
        this.delegate = delegate;
    }

    @Override
    protected Object invokeJdbc(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("unwrap")) {
            return unwrapStatement(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy) || ((Class<?>) args[0]).isInstance(delegate);
        }
        if (name.equals("getConnection")) {
            return connectionProxy;
        }
        if (name.equals("executeQuery") && args.length >= 1 && args[0] instanceof String) {
            String sql = normalizedSql(args[0]);
            return executeWithTrace("QUERY", sql, () -> sql, () -> invokeDelegate(method, args));
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate"))
                && args.length >= 1 && args[0] instanceof String) {
            String sql = normalizedSql(args[0]);
            return executeWithTrace("UPDATE", sql, () -> sql, () -> invokeDelegate(method, args));
        }
        if (name.equals("execute") && args.length >= 1 && args[0] instanceof String) {
            String sql = normalizedSql(args[0]);
            return executeWithTrace(looksLikeQuery(sql) ? "QUERY" : "UPDATE", sql, () -> sql,
                    () -> invokeDelegate(method, args));
        }

        return invokeDelegate(method, args);
    }

    protected <T> T executeWithTrace(
            String kind,
            String shapeKey,
            Supplier<String> renderedSqlSupplier,
            SqlExecution<T> execution) throws Throwable {
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
        } catch (Throwable throwable) {
            SqlTraceSampler.recordTiming(
                    kind,
                    shapeKey,
                    renderedSqlSupplier,
                    elapsedMicros(startedAtNanos),
                    false,
                    throwable.getClass().getSimpleName());
            throw throwable;
        }
    }

    protected <T> T executeBatchWithTrace(
            String kind,
            String shapeKey,
            Supplier<String> renderedSqlSupplier,
            long executionCount,
            SqlExecution<T> execution) throws Throwable {
        SqlTraceSampler.record(kind, shapeKey, renderedSqlSupplier);
        long startedAtNanos = System.nanoTime();
        try {
            T result = execution.run();
            SqlTraceSampler.recordTiming(
                    kind,
                    shapeKey,
                    renderedSqlSupplier,
                    elapsedMicros(startedAtNanos),
                    executionCount,
                    true,
                    null);
            return result;
        } catch (Throwable throwable) {
            SqlTraceSampler.recordTiming(
                    kind,
                    shapeKey,
                    renderedSqlSupplier,
                    elapsedMicros(startedAtNanos),
                    executionCount,
                    false,
                    throwable.getClass().getSimpleName());
            throw throwable;
        }
    }

    private long elapsedMicros(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1000;
    }

    private Object unwrapStatement(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        if (targetType.isInstance(delegate)) {
            return delegate;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap Statement to " + targetType.getName());
    }
}

final class TracingPreparedStatementHandler extends TracingStatementHandler {
    private final String sql;
    private final Map<Integer, Object> parameters = new LinkedHashMap<>();
    private final List<List<Object>> batchParameters = new ArrayList<>();

    TracingPreparedStatementHandler(Connection connectionProxy, PreparedStatement delegate, String sql) {
        super(connectionProxy, delegate);
        this.sql = sql;
    }

    @Override
    protected Object invokeJdbc(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();

        if (name.equals("executeQuery") && args.length == 0) {
            List<Object> ordered = orderedParameters(parameters);
            return executeWithTrace(
                    "QUERY",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> invokeDelegate(method, args));
        }
        if ((name.equals("executeUpdate") || name.equals("executeLargeUpdate")) && args.length == 0) {
            List<Object> ordered = orderedParameters(parameters);
            return executeWithTrace(
                    "UPDATE",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> invokeDelegate(method, args));
        }
        if (name.equals("execute") && args.length == 0) {
            List<Object> ordered = orderedParameters(parameters);
            return executeWithTrace(
                    looksLikeQuery(sql) ? "QUERY" : "UPDATE",
                    sql,
                    () -> SqlLiteralRenderer.render(sql, ordered),
                    () -> invokeDelegate(method, args));
        }
        if (name.equals("clearParameters") && args.length == 0) {
            parameters.clear();
            return invokeDelegate(method, args);
        }
        if (name.startsWith("set") && args.length >= 2 && args[0] instanceof Integer) {
            parameters.put((Integer) args[0], normalizeParameterValue(name, args));
            return invokeDelegate(method, args);
        }
        if (name.equals("addBatch") && args.length == 0) {
            batchParameters.add(orderedParameters(parameters));
            return invokeDelegate(method, args);
        }
        if (name.equals("clearBatch") && args.length == 0) {
            batchParameters.clear();
            return invokeDelegate(method, args);
        }
        if ((name.equals("executeBatch") || name.equals("executeLargeBatch")) && args.length == 0) {
            if (batchParameters.isEmpty()) {
                return invokeDelegate(method, args);
            }
            List<Object> sampleBatch = batchParameters.get(0);
            long executionCount = batchParameters.size();
            try {
                return executeBatchWithTrace(
                        "UPDATE",
                        sql,
                        () -> SqlLiteralRenderer.render(sql, sampleBatch),
                        executionCount,
                        () -> invokeDelegate(method, args));
            } finally {
                batchParameters.clear();
            }
        }

        return super.invokeJdbc(proxy, method, args);
    }

    private List<Object> orderedParameters(Map<Integer, Object> currentParameters) {
        if (currentParameters.isEmpty()) {
            return List.of();
        }

        int maxIndex = Collections.max(currentParameters.keySet());
        List<Object> ordered = new ArrayList<>(Collections.nCopies(maxIndex, null));
        for (Map.Entry<Integer, Object> entry : currentParameters.entrySet()) {
            ordered.set(entry.getKey() - 1, entry.getValue());
        }
        return Collections.unmodifiableList(new ArrayList<>(ordered));
    }

    private Object normalizeParameterValue(String methodName, Object[] args) throws SQLException {
        if (methodName.equals("setNull")) {
            return null;
        }
        if (methodName.equals("setObject") && args.length == 3 && args[2] instanceof Integer) {
            return coerceSqlType(args[1], (Integer) args[2]);
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