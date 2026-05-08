package dev.yohaku.dbfs.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

final class ImplDbfsClient implements DbfsClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int DEFAULT_READ_TIMEOUT_MILLIS = 30_000;
    private static final int MAX_RESPONSE_BYTES = 64 * 1024 * 1024;

    private final String host;
    private final int port;
    private final String database;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;

    ImplDbfsClient(String url) {
        URI endpoint = parseJdbcUri(url);
        this.host = endpoint.getHost();
        this.port = endpoint.getPort();
        this.database = endpoint.getPath() == null || endpoint.getPath().isBlank()
                ? ""
                : endpoint.getPath().replaceFirst("^/", "");
        this.connectTimeoutMillis = Integer.getInteger("dbfs.jdbc.connectTimeoutMillis", DEFAULT_CONNECT_TIMEOUT_MILLIS);
        this.readTimeoutMillis = Integer.getInteger("dbfs.jdbc.readTimeoutMillis", DEFAULT_READ_TIMEOUT_MILLIS);
    }

    @Override
    public QueryResult executeQuery(String sql, List<Object> parameters) throws SQLException {
        RemoteResponse response = send("query", sql, parameters);
        ensureSuccess(response);
        return new QueryResult(
                response.columns == null ? List.of() : response.columns,
                response.rows == null ? List.of() : response.rows);
    }

    @Override
    public int executeUpdate(String sql, List<Object> parameters) throws SQLException {
        RemoteResponse response = send("update", sql, parameters);
        ensureSuccess(response);
        if (response.updateCount == null) {
            throw new SQLException("dbfs server response did not include updateCount");
        }
        return response.updateCount;
    }

    @Override
    public void close() {
    }

    private RemoteResponse send(String operation, String sql, List<Object> parameters) throws SQLException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);

            try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                 DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
                Map<String, Object> request = new LinkedHashMap<>();
                request.put("operation", operation);
                request.put("database", database);
                request.put("sql", sql);
                request.put("parameters", parameters == null ? List.of() : parameters);

                byte[] requestBytes = OBJECT_MAPPER.writeValueAsBytes(request);
                output.writeInt(requestBytes.length);
                output.write(requestBytes);
                output.flush();

                int responseLength = input.readInt();
                if (responseLength < 0 || responseLength > MAX_RESPONSE_BYTES) {
                    throw new SQLException("Invalid dbfs server response length: " + responseLength);
                }

                byte[] responseBytes = new byte[responseLength];
                input.readFully(responseBytes);
                return OBJECT_MAPPER.readValue(responseBytes, RemoteResponse.class);
            }
        } catch (EOFException exception) {
            throw new SQLException("dbfs server closed the connection unexpectedly", exception);
        } catch (IOException exception) {
            throw new SQLException("Failed to communicate with dbfs server at " + host + ":" + port, exception);
        }
    }

    private void ensureSuccess(RemoteResponse response) throws SQLException {
        if (response == null) {
            throw new SQLException("dbfs server returned an empty response");
        }
        if (!response.ok) {
            throw new SQLException(
                    response.errorMessage == null ? "dbfs server execution failed" : response.errorMessage,
                    response.sqlState);
        }
    }

    private static URI parseJdbcUri(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("dbfs JDBC URL must not be empty");
        }

        URI uri = URI.create(url.startsWith("jdbc:") ? url.substring("jdbc:".length()) : url);
        if (uri.getHost() == null || uri.getPort() < 0) {
            throw new IllegalArgumentException("dbfs JDBC URL must include host and port: " + url);
        }
        return uri;
    }

    static final class RemoteResponse {
        public boolean ok = true;
        public String sqlState;
        public String errorMessage;
        public List<String> columns;
        public List<List<Object>> rows;
        public Integer updateCount;
    }

}