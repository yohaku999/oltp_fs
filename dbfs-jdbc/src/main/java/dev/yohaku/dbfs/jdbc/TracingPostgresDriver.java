package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public final class TracingPostgresDriver implements Driver {
    public static final String URL_PREFIX = "jdbc:trace:postgresql:";
    private static final String DELEGATE_URL_PREFIX = "jdbc:postgresql:";
    private static final String DELEGATE_DRIVER_CLASS = "org.postgresql.Driver";
    private static final Logger LOGGER = Logger.getLogger(TracingPostgresDriver.class.getName());

    @FunctionalInterface
    interface DriverLoader {
        Driver load() throws SQLException;
    }

    static {
        try {
            DriverManager.registerDriver(new TracingPostgresDriver());
        } catch (SQLException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private final DriverLoader driverLoader;

    public TracingPostgresDriver() {
        this(TracingPostgresDriver::loadDelegateDriver);
    }

    TracingPostgresDriver(DriverLoader driverLoader) {
        this.driverLoader = driverLoader;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        Driver delegate = driverLoader.load();
        Connection connection = delegate.connect(delegateUrl(url), info);
        if (connection == null) {
            return null;
        }
        return TracingJdbcProxyFactory.newConnection(connection);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driverLoader.load().getPropertyInfo(acceptsURL(url) ? delegateUrl(url) : url, info);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return LOGGER;
    }

    static String delegateUrl(String url) {
        return DELEGATE_URL_PREFIX + url.substring(URL_PREFIX.length());
    }

    private static Driver loadDelegateDriver() throws SQLException {
        try {
            Class<?> driverClass = Class.forName(DELEGATE_DRIVER_CLASS);
            Object instance = driverClass.getDeclaredConstructor().newInstance();
            if (!(instance instanceof Driver)) {
                throw new SQLException(DELEGATE_DRIVER_CLASS + " is not a java.sql.Driver");
            }
            return (Driver) instance;
        } catch (SQLException exception) {
            throw exception;
        } catch (ReflectiveOperationException exception) {
            throw new SQLException("Failed to load PostgreSQL JDBC driver", exception);
        }
    }
}