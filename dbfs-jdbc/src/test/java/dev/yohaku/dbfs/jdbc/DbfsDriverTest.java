package dev.yohaku.dbfs.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

class DbfsDriverTest {
    @Test
    void driverAcceptsDbfsUrls() {
        DbfsDriver driver = new DbfsDriver();

        assertTrue(driver.acceptsURL("jdbc:dbfs:local"));
        assertTrue(driver.acceptsURL("jdbc:dbfs://localhost/test"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/test"));
    }

    @Test
    void driverRegistersWithDriverManager() throws Exception {
        Class.forName(DbfsDriver.class.getName());

        Driver driver = DriverManager.getDriver("jdbc:dbfs:local");
        assertInstanceOf(DbfsDriver.class, driver);
    }

    @Test
    void connectionAndStatementSmokeTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs:local")) {
            assertFalse(connection.isClosed());
            assertTrue(connection.isValid(1));
            assertEquals("jdbc:dbfs:local", connection.getMetaData().getURL());

            try (Statement statement = connection.createStatement()) {
                assertFalse(statement.isClosed());
                assertEquals(connection, statement.getConnection());

                try (ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                    assertNotNull(resultSet.getMetaData());
                    assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    void preparedStatementSupportsParameterBinding() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs:local");
             PreparedStatement statement = connection.prepareStatement("SELECT * FROM orders WHERE id = ? AND name = ?")) {
            statement.setInt(1, 42);
            statement.setString(2, "alice");

            assertDoesNotThrow(() -> {
                try (ResultSet ignored = statement.executeQuery()) {
                    assertNotNull(ignored);
                }
            });

            statement.clearParameters();
            statement.setObject(1, 42L);
            statement.setNull(2, java.sql.Types.VARCHAR);
            assertDoesNotThrow(() -> {
                boolean ignored = statement.execute();
                assertTrue(ignored);
            });
        }
    }

    @Test
    void unsupportedOperationsFailExplicitly() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs:local")) {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL run_tpcc()"));
        }
    }
}