package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

final class QueryResult {
    private final List<String> columns;
    private final List<List<Object>> rows;

    QueryResult(List<String> columns, List<List<Object>> rows) {
        this.columns = List.copyOf(columns);
        List<List<Object>> immutableRows = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            immutableRows.add(java.util.Collections.unmodifiableList(new ArrayList<>(row)));
        }
        this.rows = List.copyOf(immutableRows);
    }

    static QueryResult empty() {
        return new QueryResult(List.of(), List.of());
    }

    List<String> columns() {
        return columns;
    }

    List<List<Object>> rows() {
        return rows;
    }
}

interface DbfsClient extends AutoCloseable {
    QueryResult executeQuery(String sql, List<Object> parameters) throws SQLException;

    int executeUpdate(String sql, List<Object> parameters) throws SQLException;

    int[] executeBatchUpdate(String sql, List<List<Object>> parameterSets) throws SQLException;

    @Override
    void close() throws SQLException;
}

final class NoopDbfsClient implements DbfsClient {
    @Override
    public QueryResult executeQuery(String sql, List<Object> parameters) {
        return QueryResult.empty();
    }

    @Override
    public int executeUpdate(String sql, List<Object> parameters) {
        return 0;
    }

    @Override
    public int[] executeBatchUpdate(String sql, List<List<Object>> parameterSets) {
        return new int[parameterSets == null ? 0 : parameterSets.size()];
    }

    @Override
    public void close() {
    }
}

final class DbfsConnectionState {
    private final String url;
    private final Properties properties;
    private final DbfsClient client;
    private Connection connectionProxy;
    private boolean closed;
    private boolean autoCommit = true;
    private boolean readOnly;
    private int networkTimeoutMillis;
    private String schema;
    private String catalog;

    private DbfsConnectionState(String url, Properties properties, DbfsClient client) {
        this.url = url;
        this.properties = properties;
        this.client = client;
    }

    static DbfsConnectionState open(String url, Properties info) {
        Properties properties = new Properties();
        if (info != null) {
            properties.putAll(info);
        }

        DbfsClient client = isRemoteUrl(url) ? new ImplDbfsClient(url) : new InMemoryDbfsClient(url);
        return new DbfsConnectionState(url, properties, client);
    }

    private static boolean isRemoteUrl(String url) {
        if (url == null || !url.startsWith("jdbc:dbfs://")) {
            return false;
        }

        URI uri = URI.create(url.substring("jdbc:".length()));
        return uri.getHost() != null && uri.getPort() >= 0;
    }

    String url() {
        return url;
    }

    Properties properties() {
        return properties;
    }

    DbfsClient client() {
        return client;
    }

    Connection connectionProxy() {
        return connectionProxy;
    }

    void setConnectionProxy(Connection connectionProxy) {
        this.connectionProxy = connectionProxy;
    }

    boolean closed() {
        return closed;
    }

    void close() throws SQLException {
        if (closed) {
            return;
        }

        closed = true;
        client.close();
    }

    boolean autoCommit() {
        return autoCommit;
    }

    void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    boolean readOnly() {
        return readOnly;
    }

    void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    int networkTimeoutMillis() {
        return networkTimeoutMillis;
    }

    void setNetworkTimeoutMillis(int networkTimeoutMillis) {
        this.networkTimeoutMillis = networkTimeoutMillis;
    }

    String schema() {
        return schema;
    }

    void setSchema(String schema) {
        this.schema = schema;
    }

    String catalog() {
        return catalog;
    }

    void setCatalog(String catalog) {
        this.catalog = catalog;
    }
}
