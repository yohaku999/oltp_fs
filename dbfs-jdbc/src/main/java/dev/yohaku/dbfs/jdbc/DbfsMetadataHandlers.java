package dev.yohaku.dbfs.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
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
        if (name.equals("getTables")) {
            return DbfsJdbcProxyFactory.newResultSet(tableMetadataResult());
        }
        if (name.equals("getColumns")) {
            return DbfsJdbcProxyFactory.newResultSet(columnMetadataResult((String) args[2], (String) args[3]));
        }
        if (name.equals("getIndexInfo")) {
            return DbfsJdbcProxyFactory.newResultSet(indexMetadataResult((String) args[2]));
        }
        if (name.equals("getImportedKeys")) {
            return DbfsJdbcProxyFactory.newResultSet(importedKeysMetadataResult((String) args[2]));
        }
        if (name.equals("getTableTypes")) {
            return DbfsJdbcProxyFactory.newResultSet(tableTypesMetadataResult());
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

    private QueryResult tableMetadataResult() {
        List<String> columns = List.of(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION");

        String catalog = catalogName();
        List<List<Object>> rows = new ArrayList<>();
    for (TableDefinition table : tpccTables()) {
            rows.add(List.<Object>of(
                    catalog,
                    "",
            table.name,
                    "TABLE",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""));
        }
        return new QueryResult(columns, rows);
    }

    private QueryResult columnMetadataResult(String tableNamePattern, String columnNamePattern) {
        List<String> columns = List.of(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN");

        String catalog = catalogName();
        List<List<Object>> rows = new ArrayList<>();
        for (TableDefinition table : tpccTables()) {
            if (!matchesPattern(table.name, tableNamePattern)) {
                continue;
            }
            for (ColumnDefinition column : table.columns) {
                if (!matchesPattern(column.name, columnNamePattern)) {
                    continue;
                }
                rows.add(List.<Object>of(
                        catalog,
                        "",
                        table.name,
                        column.name,
                        column.sqlType,
                        column.typeName,
                        column.columnSize,
                        0,
                        column.decimalDigits,
                        column.numPrecRadix,
                        column.nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls,
                        "",
                        "",
                        0,
                        0,
                        column.charOctetLength,
                        column.ordinalPosition,
                        column.nullable ? "YES" : "NO",
                        "",
                        "",
                        "",
                        0,
                        column.autoIncrement ? "YES" : "NO",
                        column.generated ? "YES" : "NO"));
            }
        }
        return new QueryResult(columns, rows);
    }

    private QueryResult indexMetadataResult(String tableName) {
        List<String> columns = List.of(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "NON_UNIQUE",
                "INDEX_QUALIFIER",
                "INDEX_NAME",
                "TYPE",
                "ORDINAL_POSITION",
                "COLUMN_NAME",
                "ASC_OR_DESC",
                "CARDINALITY",
                "PAGES",
                "FILTER_CONDITION");

        List<List<Object>> rows = new ArrayList<>();
        if (matchesPattern("customer", tableName)) {
            String catalog = catalogName();
            rows.add(List.<Object>of(catalog, "", "customer", false, "", "IDX_CUSTOMER_NAME", DatabaseMetaData.tableIndexOther, 1, "C_W_ID", "A", 0L, 0L, ""));
            rows.add(List.<Object>of(catalog, "", "customer", false, "", "IDX_CUSTOMER_NAME", DatabaseMetaData.tableIndexOther, 2, "C_D_ID", "A", 0L, 0L, ""));
            rows.add(List.<Object>of(catalog, "", "customer", false, "", "IDX_CUSTOMER_NAME", DatabaseMetaData.tableIndexOther, 3, "C_LAST", "A", 0L, 0L, ""));
            rows.add(List.<Object>of(catalog, "", "customer", false, "", "IDX_CUSTOMER_NAME", DatabaseMetaData.tableIndexOther, 4, "C_FIRST", "A", 0L, 0L, ""));
        }
        return new QueryResult(columns, rows);
    }

    private QueryResult importedKeysMetadataResult(String tableName) {
        List<String> columns = List.of(
                "PKTABLE_CAT",
                "PKTABLE_SCHEM",
                "PKTABLE_NAME",
                "PKCOLUMN_NAME",
                "FKTABLE_CAT",
                "FKTABLE_SCHEM",
                "FKTABLE_NAME",
                "FKCOLUMN_NAME",
                "KEY_SEQ",
                "UPDATE_RULE",
                "DELETE_RULE",
                "FK_NAME",
                "PK_NAME",
                "DEFERRABILITY");

        String catalog = catalogName();
        List<List<Object>> rows = new ArrayList<>();
        for (ForeignKeyDefinition foreignKey : tpccForeignKeys()) {
            if (!matchesPattern(foreignKey.fkTableName, tableName)) {
                continue;
            }
            rows.add(List.<Object>of(
                    catalog,
                    "",
                    foreignKey.pkTableName,
                    foreignKey.pkColumnName,
                    catalog,
                    "",
                    foreignKey.fkTableName,
                    foreignKey.fkColumnName,
                    foreignKey.keySeq,
                    DatabaseMetaData.importedKeyNoAction,
                    DatabaseMetaData.importedKeyNoAction,
                    foreignKey.fkName,
                    foreignKey.pkName,
                    DatabaseMetaData.importedKeyNotDeferrable));
        }
        return new QueryResult(columns, rows);
    }

    private QueryResult tableTypesMetadataResult() {
        return new QueryResult(List.of("TABLE_TYPE"), List.of(List.of("TABLE")));
    }

    private String catalogName() {
        if (state.catalog() != null && !state.catalog().isEmpty()) {
            return state.catalog();
        }

        String url = state.url();
        int slashIndex = url == null ? -1 : url.lastIndexOf('/');
        if (slashIndex < 0 || slashIndex + 1 >= url.length()) {
            return "";
        }

        String catalog = url.substring(slashIndex + 1);
        int queryIndex = catalog.indexOf('?');
        if (queryIndex >= 0) {
            catalog = catalog.substring(0, queryIndex);
        }
        return catalog;
    }

    private List<TableDefinition> tpccTables() {
        return List.of(
                new TableDefinition("warehouse", List.of(
                        col("W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("W_YTD", Types.DOUBLE, "DOUBLE", 12, 2, false, 2),
                        col("W_TAX", Types.DOUBLE, "DOUBLE", 4, 4, false, 3),
                        col("W_NAME", Types.VARCHAR, "VARCHAR", 10, 0, false, 4),
                        col("W_STREET_1", Types.VARCHAR, "VARCHAR", 20, 0, false, 5),
                        col("W_STREET_2", Types.VARCHAR, "VARCHAR", 20, 0, false, 6),
                        col("W_CITY", Types.VARCHAR, "VARCHAR", 20, 0, false, 7),
                        col("W_STATE", Types.VARCHAR, "VARCHAR", 2, 0, false, 8),
                        col("W_ZIP", Types.VARCHAR, "VARCHAR", 9, 0, false, 9))),
                new TableDefinition("stock", List.of(
                        col("S_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("S_I_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("S_QUANTITY", Types.INTEGER, "INTEGER", 4, 0, false, 3),
                        col("S_YTD", Types.DOUBLE, "DOUBLE", 8, 2, false, 4),
                        col("S_ORDER_CNT", Types.INTEGER, "INTEGER", 10, 0, false, 5),
                        col("S_REMOTE_CNT", Types.INTEGER, "INTEGER", 10, 0, false, 6),
                        col("S_DATA", Types.VARCHAR, "VARCHAR", 50, 0, false, 7),
                        col("S_DIST_01", Types.VARCHAR, "VARCHAR", 24, 0, false, 8),
                        col("S_DIST_02", Types.VARCHAR, "VARCHAR", 24, 0, false, 9),
                        col("S_DIST_03", Types.VARCHAR, "VARCHAR", 24, 0, false, 10),
                        col("S_DIST_04", Types.VARCHAR, "VARCHAR", 24, 0, false, 11),
                        col("S_DIST_05", Types.VARCHAR, "VARCHAR", 24, 0, false, 12),
                        col("S_DIST_06", Types.VARCHAR, "VARCHAR", 24, 0, false, 13),
                        col("S_DIST_07", Types.VARCHAR, "VARCHAR", 24, 0, false, 14),
                        col("S_DIST_08", Types.VARCHAR, "VARCHAR", 24, 0, false, 15),
                        col("S_DIST_09", Types.VARCHAR, "VARCHAR", 24, 0, false, 16),
                        col("S_DIST_10", Types.VARCHAR, "VARCHAR", 24, 0, false, 17))),
                new TableDefinition("order_line", List.of(
                        col("OL_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("OL_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("OL_O_ID", Types.INTEGER, "INTEGER", 10, 0, false, 3),
                        col("OL_NUMBER", Types.INTEGER, "INTEGER", 10, 0, false, 4),
                        col("OL_I_ID", Types.INTEGER, "INTEGER", 10, 0, false, 5),
                        col("OL_DELIVERY_D", Types.TIMESTAMP, "TIMESTAMP", 29, 0, true, 6),
                        col("OL_AMOUNT", Types.DOUBLE, "DOUBLE", 6, 2, false, 7),
                        col("OL_SUPPLY_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 8),
                        col("OL_QUANTITY", Types.INTEGER, "INTEGER", 2, 0, false, 9),
                        col("OL_DIST_INFO", Types.VARCHAR, "VARCHAR", 24, 0, false, 10))),
                new TableDefinition("oorder", List.of(
                        col("O_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("O_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("O_ID", Types.INTEGER, "INTEGER", 10, 0, false, 3),
                        col("O_C_ID", Types.INTEGER, "INTEGER", 10, 0, false, 4),
                        col("O_CARRIER_ID", Types.INTEGER, "INTEGER", 10, 0, true, 5),
                        col("O_OL_CNT", Types.INTEGER, "INTEGER", 2, 0, false, 6),
                        col("O_ALL_LOCAL", Types.INTEGER, "INTEGER", 1, 0, false, 7),
                        col("O_ENTRY_D", Types.TIMESTAMP, "TIMESTAMP", 29, 0, true, 8))),
                new TableDefinition("new_order", List.of(
                        col("NO_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("NO_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("NO_O_ID", Types.INTEGER, "INTEGER", 10, 0, false, 3))),
                new TableDefinition("item", List.of(
                        col("I_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("I_NAME", Types.VARCHAR, "VARCHAR", 24, 0, false, 2),
                        col("I_PRICE", Types.DOUBLE, "DOUBLE", 5, 2, false, 3),
                        col("I_DATA", Types.VARCHAR, "VARCHAR", 50, 0, false, 4),
                        col("I_IM_ID", Types.INTEGER, "INTEGER", 10, 0, false, 5))),
                new TableDefinition("history", List.of(
                        col("H_C_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("H_C_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("H_C_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 3),
                        col("H_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 4),
                        col("H_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 5),
                        col("H_DATE", Types.TIMESTAMP, "TIMESTAMP", 29, 0, true, 6),
                        col("H_AMOUNT", Types.DOUBLE, "DOUBLE", 6, 2, false, 7),
                        col("H_DATA", Types.VARCHAR, "VARCHAR", 24, 0, false, 8))),
                new TableDefinition("district", List.of(
                        col("D_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("D_YTD", Types.DOUBLE, "DOUBLE", 12, 2, false, 3),
                        col("D_TAX", Types.DOUBLE, "DOUBLE", 4, 4, false, 4),
                        col("D_NEXT_O_ID", Types.INTEGER, "INTEGER", 10, 0, false, 5),
                        col("D_NAME", Types.VARCHAR, "VARCHAR", 10, 0, false, 6),
                        col("D_STREET_1", Types.VARCHAR, "VARCHAR", 20, 0, false, 7),
                        col("D_STREET_2", Types.VARCHAR, "VARCHAR", 20, 0, false, 8),
                        col("D_CITY", Types.VARCHAR, "VARCHAR", 20, 0, false, 9),
                        col("D_STATE", Types.VARCHAR, "VARCHAR", 2, 0, false, 10),
                        col("D_ZIP", Types.VARCHAR, "VARCHAR", 9, 0, false, 11))),
                new TableDefinition("customer", List.of(
                        col("C_W_ID", Types.INTEGER, "INTEGER", 10, 0, false, 1),
                        col("C_D_ID", Types.INTEGER, "INTEGER", 10, 0, false, 2),
                        col("C_ID", Types.INTEGER, "INTEGER", 10, 0, false, 3),
                        col("C_DISCOUNT", Types.DOUBLE, "DOUBLE", 4, 4, false, 4),
                        col("C_CREDIT", Types.VARCHAR, "VARCHAR", 2, 0, false, 5),
                        col("C_LAST", Types.VARCHAR, "VARCHAR", 16, 0, false, 6),
                        col("C_FIRST", Types.VARCHAR, "VARCHAR", 16, 0, false, 7),
                        col("C_CREDIT_LIM", Types.DOUBLE, "DOUBLE", 12, 2, false, 8),
                        col("C_BALANCE", Types.DOUBLE, "DOUBLE", 12, 2, false, 9),
                        col("C_YTD_PAYMENT", Types.DOUBLE, "DOUBLE", 12, 2, false, 10),
                        col("C_PAYMENT_CNT", Types.INTEGER, "INTEGER", 10, 0, false, 11),
                        col("C_DELIVERY_CNT", Types.INTEGER, "INTEGER", 10, 0, false, 12),
                        col("C_STREET_1", Types.VARCHAR, "VARCHAR", 20, 0, false, 13),
                        col("C_STREET_2", Types.VARCHAR, "VARCHAR", 20, 0, false, 14),
                        col("C_CITY", Types.VARCHAR, "VARCHAR", 20, 0, false, 15),
                        col("C_STATE", Types.VARCHAR, "VARCHAR", 2, 0, false, 16),
                        col("C_ZIP", Types.VARCHAR, "VARCHAR", 9, 0, false, 17),
                        col("C_PHONE", Types.VARCHAR, "VARCHAR", 16, 0, false, 18),
                        col("C_SINCE", Types.TIMESTAMP, "TIMESTAMP", 29, 0, true, 19),
                        col("C_MIDDLE", Types.VARCHAR, "VARCHAR", 2, 0, false, 20),
                        col("C_DATA", Types.VARCHAR, "VARCHAR", 500, 0, false, 21))));
    }

    private List<ForeignKeyDefinition> tpccForeignKeys() {
        return List.of(
                fk("FK_STOCK_WAREHOUSE", "PK_WAREHOUSE", "stock", "S_W_ID", "warehouse", "W_ID", 1),
                fk("FK_STOCK_ITEM", "PK_ITEM", "stock", "S_I_ID", "item", "I_ID", 1),
                fk("FK_DISTRICT_WAREHOUSE", "PK_WAREHOUSE", "district", "D_W_ID", "warehouse", "W_ID", 1),
                fk("FK_CUSTOMER_DISTRICT", "PK_DISTRICT", "customer", "C_W_ID", "district", "D_W_ID", 1),
                fk("FK_CUSTOMER_DISTRICT", "PK_DISTRICT", "customer", "C_D_ID", "district", "D_ID", 2),
                fk("FK_HISTORY_CUSTOMER", "PK_CUSTOMER", "history", "H_C_W_ID", "customer", "C_W_ID", 1),
                fk("FK_HISTORY_CUSTOMER", "PK_CUSTOMER", "history", "H_C_D_ID", "customer", "C_D_ID", 2),
                fk("FK_HISTORY_CUSTOMER", "PK_CUSTOMER", "history", "H_C_ID", "customer", "C_ID", 3),
                fk("FK_HISTORY_DISTRICT", "PK_DISTRICT", "history", "H_W_ID", "district", "D_W_ID", 1),
                fk("FK_HISTORY_DISTRICT", "PK_DISTRICT", "history", "H_D_ID", "district", "D_ID", 2),
                fk("FK_OORDER_CUSTOMER", "PK_CUSTOMER", "oorder", "O_W_ID", "customer", "C_W_ID", 1),
                fk("FK_OORDER_CUSTOMER", "PK_CUSTOMER", "oorder", "O_D_ID", "customer", "C_D_ID", 2),
                fk("FK_OORDER_CUSTOMER", "PK_CUSTOMER", "oorder", "O_C_ID", "customer", "C_ID", 3),
                fk("FK_NEW_ORDER_OORDER", "PK_OORDER", "new_order", "NO_W_ID", "oorder", "O_W_ID", 1),
                fk("FK_NEW_ORDER_OORDER", "PK_OORDER", "new_order", "NO_D_ID", "oorder", "O_D_ID", 2),
                fk("FK_NEW_ORDER_OORDER", "PK_OORDER", "new_order", "NO_O_ID", "oorder", "O_ID", 3),
                fk("FK_ORDER_LINE_OORDER", "PK_OORDER", "order_line", "OL_W_ID", "oorder", "O_W_ID", 1),
                fk("FK_ORDER_LINE_OORDER", "PK_OORDER", "order_line", "OL_D_ID", "oorder", "O_D_ID", 2),
                fk("FK_ORDER_LINE_OORDER", "PK_OORDER", "order_line", "OL_O_ID", "oorder", "O_ID", 3),
                fk("FK_ORDER_LINE_STOCK", "PK_STOCK", "order_line", "OL_SUPPLY_W_ID", "stock", "S_W_ID", 1),
                fk("FK_ORDER_LINE_STOCK", "PK_STOCK", "order_line", "OL_I_ID", "stock", "S_I_ID", 2));
    }

    private boolean matchesPattern(String value, String pattern) {
        if (pattern == null) {
            return true;
        }

        StringBuilder regex = new StringBuilder();
        for (int index = 0; index < pattern.length(); index += 1) {
            char character = pattern.charAt(index);
            if (character == '%') {
                regex.append(".*");
            } else if (character == '_') {
                regex.append('.');
            } else {
                regex.append(java.util.regex.Pattern.quote(String.valueOf(character)));
            }
        }
        return value.matches("(?i)" + regex);
    }

    private ColumnDefinition col(String name, int sqlType, String typeName, int columnSize, int decimalDigits, boolean nullable, int ordinalPosition) {
        return new ColumnDefinition(name, sqlType, typeName, columnSize, decimalDigits, nullable, ordinalPosition, false, false);
    }

    private ForeignKeyDefinition fk(String fkName, String pkName, String fkTableName, String fkColumnName, String pkTableName, String pkColumnName, int keySeq) {
        return new ForeignKeyDefinition(fkName, pkName, fkTableName, fkColumnName, pkTableName, pkColumnName, keySeq);
    }

    private static final class TableDefinition {
        private final String name;
        private final List<ColumnDefinition> columns;

        private TableDefinition(String name, List<ColumnDefinition> columns) {
            this.name = name;
            this.columns = columns;
        }
    }

    private static final class ColumnDefinition {
        private final String name;
        private final int sqlType;
        private final String typeName;
        private final int columnSize;
        private final int decimalDigits;
        private final boolean nullable;
        private final int ordinalPosition;
        private final boolean autoIncrement;
        private final boolean generated;
        private final int numPrecRadix;
        private final int charOctetLength;

        private ColumnDefinition(String name, int sqlType, String typeName, int columnSize, int decimalDigits,
                                 boolean nullable, int ordinalPosition, boolean autoIncrement, boolean generated) {
            this.name = name;
            this.sqlType = sqlType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.nullable = nullable;
            this.ordinalPosition = ordinalPosition;
            this.autoIncrement = autoIncrement;
            this.generated = generated;
            this.numPrecRadix = (sqlType == Types.DOUBLE || sqlType == Types.INTEGER) ? 10 : 0;
            this.charOctetLength = (sqlType == Types.VARCHAR || sqlType == Types.TIMESTAMP) ? columnSize : 0;
        }
    }

    private static final class ForeignKeyDefinition {
        private final String fkName;
        private final String pkName;
        private final String fkTableName;
        private final String fkColumnName;
        private final String pkTableName;
        private final String pkColumnName;
        private final int keySeq;

        private ForeignKeyDefinition(String fkName, String pkName, String fkTableName, String fkColumnName,
                                     String pkTableName, String pkColumnName, int keySeq) {
            this.fkName = fkName;
            this.pkName = pkName;
            this.fkTableName = fkTableName;
            this.fkColumnName = fkColumnName;
            this.pkTableName = pkTableName;
            this.pkColumnName = pkColumnName;
            this.keySeq = keySeq;
        }
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