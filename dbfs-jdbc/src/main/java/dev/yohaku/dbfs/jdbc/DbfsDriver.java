package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public final class DbfsDriver implements Driver {
    public static final String URL_PREFIX = "jdbc:dbfs:";
    private static final Logger LOGGER = Logger.getLogger(DbfsDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new DbfsDriver());
        } catch (SQLException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        DbfsConnectionState state = DbfsConnectionState.open(url, info);
        Connection connection = DbfsJdbcProxyFactory.newConnection(state);
        state.setConnectionProxy(connection);
        return connection;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo host = new DriverPropertyInfo("host", info == null ? null : info.getProperty("host"));
        host.description = "Optional dbfs host name";

        DriverPropertyInfo port = new DriverPropertyInfo("port", info == null ? null : info.getProperty("port"));
        port.description = "Optional dbfs port";

        DriverPropertyInfo database = new DriverPropertyInfo("database", info == null ? null : info.getProperty("database"));
        database.description = "Optional dbfs database/catalog name";

        return new DriverPropertyInfo[]{host, port, database};
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
}