package dev.yohaku.dbfs.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;

class TracingPostgresDriverTest {
    @Test
    void driverAcceptsTracingPostgresUrls() {
        TracingPostgresDriver driver = new TracingPostgresDriver(() -> new FakeDriver());

        assertTrue(driver.acceptsURL("jdbc:trace:postgresql://localhost/test"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/test"));
    }

    @Test
    void driverDelegatesToPostgresDriverAndWrapsStatements() throws SQLException {
        FakeDriver fakeDriver = new FakeDriver();
        TracingPostgresDriver driver = new TracingPostgresDriver(() -> fakeDriver);

        try (Connection connection = driver.connect("jdbc:trace:postgresql://localhost/test", new Properties())) {
            assertEquals("jdbc:postgresql://localhost/test", fakeDriver.lastUrl.get());
            assertNotSame(fakeDriver.connection, connection);

            try (Statement statement = connection.createStatement()) {
                assertDoesNotThrow(() -> statement.executeQuery("SELECT 1"));
                assertEquals(1, fakeDriver.statementExecuteQueryCount.get());
                assertEquals(connection, statement.getConnection());
            }

            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM orders WHERE id = ?")) {
                statement.setInt(1, 42);
                try (ResultSet ignored = statement.executeQuery()) {
                    assertEquals(42, fakeDriver.lastPreparedParameter.get());
                    assertEquals(1, fakeDriver.preparedExecuteQueryCount.get());
                }
            }
        }
    }

    private static final class FakeDriver implements Driver {
        private final AtomicReference<String> lastUrl = new AtomicReference<>();
        private final AtomicInteger statementExecuteQueryCount = new AtomicInteger();
        private final AtomicInteger preparedExecuteQueryCount = new AtomicInteger();
        private final AtomicReference<Object> lastPreparedParameter = new AtomicReference<>();
        private final Connection connection = newConnectionProxy();

        @Override
        public Connection connect(String url, Properties info) {
            lastUrl.set(url);
            return connection;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith("jdbc:postgresql:");
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        private Connection newConnectionProxy() {
            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("createStatement")) {
                    return newStatementProxy();
                }
                if (name.equals("prepareStatement")) {
                    return newPreparedStatementProxy();
                }
                if (name.equals("close")) {
                    return null;
                }
                if (name.equals("isClosed")) {
                    return false;
                }
                if (name.equals("unwrap")) {
                    return proxy;
                }
                if (name.equals("isWrapperFor")) {
                    return false;
                }
                if (name.equals("toString")) {
                    return "FakeConnection";
                }
                throw new UnsupportedOperationException(name);
            };
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    handler);
        }

        private Statement newStatementProxy() {
            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("executeQuery")) {
                    statementExecuteQueryCount.incrementAndGet();
                    return emptyResultSet();
                }
                if (name.equals("close")) {
                    return null;
                }
                if (name.equals("getConnection")) {
                    return connection;
                }
                if (name.equals("toString")) {
                    return "FakeStatement";
                }
                if (name.equals("unwrap")) {
                    return proxy;
                }
                if (name.equals("isWrapperFor")) {
                    return false;
                }
                throw new UnsupportedOperationException(name);
            };
            return (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(),
                    new Class<?>[]{Statement.class},
                    handler);
        }

        private PreparedStatement newPreparedStatementProxy() {
            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("setInt")) {
                    lastPreparedParameter.set(args[1]);
                    return null;
                }
                if (name.equals("executeQuery")) {
                    preparedExecuteQueryCount.incrementAndGet();
                    return emptyResultSet();
                }
                if (name.equals("close")) {
                    return null;
                }
                if (name.equals("getConnection")) {
                    return connection;
                }
                if (name.equals("toString")) {
                    return "FakePreparedStatement";
                }
                if (name.equals("unwrap")) {
                    return proxy;
                }
                if (name.equals("isWrapperFor")) {
                    return false;
                }
                throw new UnsupportedOperationException(name);
            };
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    handler);
        }

        private ResultSet emptyResultSet() {
            InvocationHandler handler = (proxy, method, args) -> {
                String name = method.getName();
                if (name.equals("next")) {
                    return false;
                }
                if (name.equals("close")) {
                    return null;
                }
                if (name.equals("toString")) {
                    return "FakeResultSet";
                }
                throw new UnsupportedOperationException(name);
            };
            return (ResultSet) Proxy.newProxyInstance(
                    ResultSet.class.getClassLoader(),
                    new Class<?>[]{ResultSet.class},
                    handler);
        }
    }
}