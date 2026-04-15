package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;

final class DbfsDatabaseMetaDataHandler extends DbfsProxyHandler {
    private final DbfsConnectionState state;

    DbfsDatabaseMetaDataHandler(DbfsConnectionState state) {
        this.state = state;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();
        if (name.equals("unwrap")) {
            return unwrapMetadata(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy);
        }
        if (name.equals("getURL")) {
            return state.url();
        }
        if (name.equals("getConnection")) {
            return state.connectionProxy();
        }
        if (name.equals("getDriverName")) {
            return "dbfs JDBC";
        }
        if (name.equals("getDriverVersion")) {
            return "0.1.0-SNAPSHOT";
        }
        if (name.equals("getDatabaseProductName")) {
            return "dbfs";
        }
        if (name.equals("getDatabaseProductVersion")) {
            return "dev";
        }
        if (name.equals("getIdentifierQuoteString")) {
            return "\"";
        }
        if (name.equals("getDefaultTransactionIsolation")) {
            return Connection.TRANSACTION_NONE;
        }
        if (name.equals("supportsTransactions")) {
            return false;
        }
        if (name.equals("supportsResultSetType")) {
            return ((Integer) args[0]) == ResultSet.TYPE_FORWARD_ONLY;
        }
        if (name.equals("supportsResultSetConcurrency")) {
            return ((Integer) args[0]) == ResultSet.TYPE_FORWARD_ONLY
                    && ((Integer) args[1]) == ResultSet.CONCUR_READ_ONLY;
        }
        if (name.equals("allProceduresAreCallable")) {
            return false;
        }
        if (name.equals("allTablesAreSelectable")) {
            return true;
        }
        if (isFalseMetadataCapability(name)) {
            return false;
        }
        if (isEmptyMetadataString(name)) {
            return "";
        }

        throw unsupported(method);
    }

    private boolean isFalseMetadataCapability(String name) {
        return name.equals("nullsAreSortedHigh")
                || name.equals("nullsAreSortedLow")
                || name.equals("nullsAreSortedAtStart")
                || name.equals("nullsAreSortedAtEnd")
                || name.equals("usesLocalFiles")
                || name.equals("usesLocalFilePerTable")
                || name.equals("supportsMixedCaseIdentifiers")
                || name.equals("storesUpperCaseIdentifiers")
                || name.equals("storesLowerCaseIdentifiers")
                || name.equals("storesMixedCaseIdentifiers")
                || name.equals("supportsMixedCaseQuotedIdentifiers")
                || name.equals("storesUpperCaseQuotedIdentifiers")
                || name.equals("storesLowerCaseQuotedIdentifiers")
                || name.equals("storesMixedCaseQuotedIdentifiers")
                || name.equals("supportsAlterTableWithAddColumn")
                || name.equals("supportsAlterTableWithDropColumn")
                || name.equals("supportsColumnAliasing")
                || name.equals("nullPlusNonNullIsNull")
                || name.equals("supportsConvert")
                || name.equals("supportsTableCorrelationNames")
                || name.equals("supportsDifferentTableCorrelationNames")
                || name.equals("supportsExpressionsInOrderBy")
                || name.equals("supportsOrderByUnrelated")
                || name.equals("supportsGroupBy")
                || name.equals("supportsGroupByUnrelated")
                || name.equals("supportsGroupByBeyondSelect")
                || name.equals("supportsLikeEscapeClause")
                || name.equals("supportsMultipleResultSets")
                || name.equals("supportsMultipleTransactions")
                || name.equals("supportsNonNullableColumns")
                || name.equals("supportsMinimumSQLGrammar")
                || name.equals("supportsCoreSQLGrammar")
                || name.equals("supportsExtendedSQLGrammar")
                || name.equals("supportsANSI92EntryLevelSQL")
                || name.equals("supportsANSI92IntermediateSQL")
                || name.equals("supportsANSI92FullSQL")
                || name.equals("supportsIntegrityEnhancementFacility")
                || name.equals("supportsOuterJoins")
                || name.equals("supportsFullOuterJoins")
                || name.equals("supportsLimitedOuterJoins")
                || name.equals("supportsSchemasInDataManipulation")
                || name.equals("supportsSchemasInProcedureCalls")
                || name.equals("supportsSchemasInTableDefinitions")
                || name.equals("supportsSchemasInIndexDefinitions")
                || name.equals("supportsSchemasInPrivilegeDefinitions")
                || name.equals("supportsCatalogsInDataManipulation")
                || name.equals("supportsCatalogsInProcedureCalls")
                || name.equals("supportsCatalogsInTableDefinitions")
                || name.equals("supportsCatalogsInIndexDefinitions")
                || name.equals("supportsCatalogsInPrivilegeDefinitions")
                || name.equals("supportsPositionedDelete")
                || name.equals("supportsPositionedUpdate")
                || name.equals("supportsSelectForUpdate")
                || name.equals("supportsStoredProcedures")
                || name.equals("supportsSubqueriesInComparisons")
                || name.equals("supportsSubqueriesInExists")
                || name.equals("supportsSubqueriesInIns")
                || name.equals("supportsSubqueriesInQuantifieds")
                || name.equals("supportsCorrelatedSubqueries")
                || name.equals("supportsUnion")
                || name.equals("supportsUnionAll")
                || name.equals("supportsOpenCursorsAcrossCommit")
                || name.equals("supportsOpenCursorsAcrossRollback")
                || name.equals("supportsOpenStatementsAcrossCommit")
                || name.equals("supportsOpenStatementsAcrossRollback")
                || name.equals("generatedKeyAlwaysReturned");
    }

    private boolean isEmptyMetadataString(String name) {
        return name.equals("getSQLKeywords")
                || name.equals("getNumericFunctions")
                || name.equals("getStringFunctions")
                || name.equals("getSystemFunctions")
                || name.equals("getTimeDateFunctions")
                || name.equals("getSearchStringEscape")
                || name.equals("getExtraNameCharacters")
                || name.equals("getSchemaTerm")
                || name.equals("getProcedureTerm")
                || name.equals("getCatalogTerm")
                || name.equals("getCatalogSeparator");
    }

    private Object unwrapMetadata(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap DatabaseMetaData to " + targetType.getName());
    }
}

final class DbfsResultSetMetaDataHandler extends DbfsProxyHandler {
    private final QueryResult result;

    DbfsResultSetMetaDataHandler(QueryResult result) {
        this.result = result;
    }

    @Override
    protected Object invokeJdbc(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
        String name = method.getName();
        if (name.equals("unwrap")) {
            return unwrapMetaData(proxy, (Class<?>) args[0]);
        }
        if (name.equals("isWrapperFor")) {
            return ((Class<?>) args[0]).isInstance(proxy);
        }
        if (name.equals("getColumnCount")) {
            return result.columns().size();
        }
        if (name.equals("getColumnLabel") || name.equals("getColumnName")) {
            return result.columns().get((Integer) args[0] - 1);
        }
        if (name.equals("getColumnType")) {
            Object sample = sampleValue((Integer) args[0] - 1);
            return inferSqlType(sample);
        }
        if (name.equals("getColumnTypeName")) {
            int sqlType = inferSqlType(sampleValue((Integer) args[0] - 1));
            if (sqlType == Types.INTEGER) {
                return "INTEGER";
            }
            if (sqlType == Types.BIGINT) {
                return "BIGINT";
            }
            if (sqlType == Types.BOOLEAN) {
                return "BOOLEAN";
            }
            if (sqlType == Types.DOUBLE) {
                return "DOUBLE";
            }
            if (sqlType == Types.VARCHAR) {
                return "VARCHAR";
            }
            return "JAVA_OBJECT";
        }
        if (name.equals("isNullable")) {
            return ResultSetMetaData.columnNullableUnknown;
        }
        if (name.equals("isAutoIncrement")) {
            return false;
        }
        if (name.equals("isCaseSensitive")) {
            return true;
        }
        if (name.equals("isSearchable")) {
            return true;
        }
        if (name.equals("isCurrency")) {
            return false;
        }
        if (name.equals("isSigned")) {
            Object sample = sampleValue((Integer) args[0] - 1);
            return sample instanceof Number;
        }
        if (name.equals("getColumnDisplaySize")) {
            return 64;
        }
        if (name.equals("getSchemaName") || name.equals("getTableName") || name.equals("getCatalogName")) {
            return "";
        }
        if (name.equals("getPrecision") || name.equals("getScale")) {
            return 0;
        }
        if (name.equals("isReadOnly")) {
            return true;
        }
        if (name.equals("isWritable") || name.equals("isDefinitelyWritable")) {
            return false;
        }
        if (name.equals("getColumnClassName")) {
            Object sample = sampleValue((Integer) args[0] - 1);
            return sample == null ? Object.class.getName() : sample.getClass().getName();
        }

        throw unsupported(method);
    }

    private Object unwrapMetaData(Object proxy, Class<?> targetType) throws SQLException {
        if (targetType.isInstance(proxy)) {
            return proxy;
        }
        throw new SQLFeatureNotSupportedException("Cannot unwrap ResultSetMetaData to " + targetType.getName());
    }

    private Object sampleValue(int columnIndex) {
        for (List<Object> row : result.rows()) {
            if (columnIndex < row.size() && row.get(columnIndex) != null) {
                return row.get(columnIndex);
            }
        }
        return null;
    }

    private int inferSqlType(Object value) {
        if (value instanceof Integer) {
            return Types.INTEGER;
        }
        if (value instanceof Long) {
            return Types.BIGINT;
        }
        if (value instanceof Float || value instanceof Double) {
            return Types.DOUBLE;
        }
        if (value instanceof Boolean) {
            return Types.BOOLEAN;
        }
        if (value instanceof String || value == null) {
            return Types.VARCHAR;
        }
        return Types.JAVA_OBJECT;
    }
}