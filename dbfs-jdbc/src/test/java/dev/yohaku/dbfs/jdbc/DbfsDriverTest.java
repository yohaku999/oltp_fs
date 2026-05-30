package dev.yohaku.dbfs.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class DbfsDriverTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    void statementHandlesVersionQueryLocally() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs://localhost:1/metadata");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT version();")) {
            assertTrue(resultSet.next());
            assertEquals("dbfs-jdbc 0.0.0", resultSet.getString("version"));
            assertFalse(resultSet.next());
        }
    }

    @Test
    void preparedStatementHandlesVersionQueryLocally() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs://localhost:1/metadata");
             PreparedStatement statement = connection.prepareStatement("  select   version()  ");
             ResultSet resultSet = statement.executeQuery()) {
            assertTrue(resultSet.next());
            assertEquals("dbfs-jdbc 0.0.0", resultSet.getString(1));
            assertFalse(resultSet.next());
        }
    }

    @Test
    void statementHandlesShowAllLocally() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs://localhost:1/metadata");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SHOW ALL;")) {
            assertTrue(resultSet.next());
            assertEquals("server_version", resultSet.getString("name"));
            assertEquals("dbfs-jdbc 0.0.0", resultSet.getString("setting"));
            assertEquals("DBFS JDBC compatibility value", resultSet.getString("description"));
        }
    }

    @Test
    void statementHandlesPostgresMetricViewsLocally() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs://localhost:1/metadata");
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_stat_database")) {
            assertEquals(0, resultSet.getMetaData().getColumnCount());
            assertFalse(resultSet.next());
        }
    }

    @Test
    void sqlLiteralRendererRendersRepresentativePreparedStatementSql() {
        String rendered = SqlLiteralRenderer.render(
                "INSERT INTO order_line VALUES (?, ?, ?, ?)",
            Arrays.asList(1, "O'Brian", null, Timestamp.valueOf("2026-04-18 00:16:32.005")));

        assertEquals(
                "INSERT INTO order_line VALUES (1, 'O''Brian', NULL, '2026-04-18 00:16:32.005')",
                rendered);
    }

    @Test
    void unsupportedOperationsFailExplicitly() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:dbfs:local")) {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL run_tpcc()"));
        }
    }

    @Test
    void resultSetCoercesStringTimestamps() throws SQLException {
        Timestamp expected = Timestamp.valueOf("2026-05-06 03:10:14.123");
        QueryResult result = new QueryResult(
                List.of("C_SINCE"),
                List.of(List.of("2026-05-06 03:10:14.123")));

        try (ResultSet resultSet = DbfsJdbcProxyFactory.newResultSet(result)) {
            assertTrue(resultSet.next());
            assertEquals(expected, resultSet.getTimestamp("C_SINCE"));
            assertEquals(expected, resultSet.getObject(1, Timestamp.class));
        }
    }

    @Test
    void resultSetCoercesEpochMillisTimestamps() throws SQLException {
        long epochMillis = 1_778_785_571_223L;
        Timestamp expected = new Timestamp(epochMillis);
        QueryResult result = new QueryResult(
                List.of("C_SINCE", "H_DATE"),
                List.of(List.of(String.valueOf(epochMillis), epochMillis)));

        try (ResultSet resultSet = DbfsJdbcProxyFactory.newResultSet(result)) {
            assertTrue(resultSet.next());
            assertEquals(expected, resultSet.getTimestamp("C_SINCE"));
            assertEquals(expected, resultSet.getTimestamp("H_DATE"));
            assertEquals(expected, resultSet.getObject(1, Timestamp.class));
            assertEquals(expected, resultSet.getObject(2, Timestamp.class));
        }
    }

    @Test
    void remoteRequestSerializesTimestampParametersAsStrings() throws Exception {
        Timestamp timestamp = Timestamp.valueOf("2026-05-15 04:12:33.456");

        byte[] encoded = ImplDbfsClient.encodeRequest(
                "update",
                "benchbase",
                "INSERT INTO customer VALUES (?)",
            Arrays.asList(timestamp, null));

        JsonNode root = OBJECT_MAPPER.readTree(new String(encoded, StandardCharsets.UTF_8));
        assertEquals("update", root.get("operation").asText());
        assertEquals("benchbase", root.get("database").asText());
        assertEquals("INSERT INTO customer VALUES (?)", root.get("sql").asText());
        assertEquals("2026-05-15 04:12:33.456", root.get("parameters").get(0).asText());
        assertTrue(root.get("parameters").get(0).isTextual());
        assertNull(root.get("parameters").get(1).textValue());
        assertTrue(root.get("parameters").get(1).isNull());
    }
}
