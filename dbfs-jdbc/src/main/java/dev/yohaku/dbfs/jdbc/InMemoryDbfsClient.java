package dev.yohaku.dbfs.jdbc;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryDbfsClient implements DbfsClient {
    private static final Map<String, InMemoryTpccDatabase> DATABASES = new ConcurrentHashMap<>();

    private final InMemoryTpccDatabase database;

    InMemoryDbfsClient(String url) {
        this.database = DATABASES.computeIfAbsent(databaseKey(url), ignored -> new InMemoryTpccDatabase());
    }

    @Override
    public QueryResult executeQuery(String sql, List<Object> parameters) throws SQLException {
        return database.executeQuery(sql, parameters);
    }

    @Override
    public int executeUpdate(String sql, List<Object> parameters) throws SQLException {
        return database.executeUpdate(sql, parameters);
    }

    @Override
    public void close() {
    }

    private static String databaseKey(String url) {
        return url == null ? "jdbc:dbfs:default" : url.trim().toLowerCase(Locale.ROOT);
    }
}

final class InMemoryTpccDatabase {
    private final Map<Integer, Map<String, Object>> warehouses = new HashMap<>();
    private final Map<DistrictKey, Map<String, Object>> districts = new HashMap<>();
    private final Map<CustomerKey, Map<String, Object>> customers = new HashMap<>();
    private final Map<Integer, Map<String, Object>> items = new HashMap<>();
    private final Map<StockKey, Map<String, Object>> stocks = new HashMap<>();
    private final Map<OrderKey, Map<String, Object>> orders = new HashMap<>();
    private final Map<NewOrderKey, Map<String, Object>> newOrders = new HashMap<>();
    private final Map<OrderLineKey, Map<String, Object>> orderLines = new HashMap<>();
    private final List<Map<String, Object>> histories = new ArrayList<>();

    synchronized QueryResult executeQuery(String sql, List<Object> parameters) throws SQLException {
        String normalizedSql = normalizeSql(sql);

        if (normalizedSql.startsWith("select 1")) {
            return QueryResult.empty();
        }
        if (normalizedSql.contains("from customer")
                && normalizedSql.contains("where c_w_id = ?")
                && normalizedSql.contains("and c_d_id = ?")
                && normalizedSql.contains("and c_id = ?")) {
            if (normalizedSql.startsWith("select c_discount, c_last, c_credit from customer")) {
                return customerByIdProjection(parameters, List.of("C_DISCOUNT", "C_LAST", "C_CREDIT"));
            }
            if (normalizedSql.startsWith("select c_data from customer")) {
                return customerByIdProjection(parameters, List.of("C_DATA"));
            }
            if (normalizedSql.startsWith("select c_first, c_middle, c_last")) {
                return customerByIdProjection(parameters, List.of(
                        "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE",
                        "C_ZIP", "C_PHONE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE",
                        "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_SINCE"));
            }
        }
        if (normalizedSql.contains("from customer") && normalizedSql.contains("where c_w_id = ?")
                && normalizedSql.contains("and c_d_id = ?") && normalizedSql.contains("and c_last = ?")
                && normalizedSql.contains("order by c_first")) {
            return customerByLastNameProjection(parameters);
        }
        if (normalizedSql.startsWith("select w_tax from warehouse where w_id = ?")) {
            return warehouseProjection(parameters, List.of("W_TAX"));
        }
        if (normalizedSql.startsWith("select w_street_1")) {
            return warehouseProjection(parameters, List.of("W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_NAME"));
        }
        if (normalizedSql.startsWith("select d_next_o_id, d_tax from district")) {
            return districtProjection(parameters, List.of("D_NEXT_O_ID", "D_TAX"));
        }
        if (normalizedSql.startsWith("select d_next_o_id from district")) {
            return districtProjection(parameters, List.of("D_NEXT_O_ID"));
        }
        if (normalizedSql.startsWith("select d_street_1")) {
            return districtProjection(parameters, List.of("D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_NAME"));
        }
        if (normalizedSql.startsWith("select i_price, i_name, i_data from item where i_id = ?")) {
            return itemProjection(parameters, List.of("I_PRICE", "I_NAME", "I_DATA"));
        }
        if (normalizedSql.startsWith("select s_quantity, s_data")) {
            return stockProjection(parameters, List.of(
                    "S_QUANTITY", "S_DATA", "S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04", "S_DIST_05",
                    "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10"));
        }
        if (normalizedSql.startsWith("select no_o_id from new_order")) {
            return nextNewOrderProjection(parameters);
        }
        if (normalizedSql.startsWith("select o_c_id from oorder")) {
            return orderProjection(parameters, List.of("O_C_ID"));
        }
        if (normalizedSql.startsWith("select o_id, o_carrier_id, o_entry_d from oorder")) {
            return newestOrderProjection(parameters);
        }
        if (normalizedSql.startsWith("select sum(ol_amount) as ol_total from order_line")) {
            return orderLineTotalProjection(parameters);
        }
        if (normalizedSql.startsWith("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line")) {
            return orderLinesProjection(parameters);
        }
        if (normalizedSql.startsWith("select count(distinct (s_i_id)) as stock_count from order_line, stock")) {
            return stockCountProjection(parameters);
        }

        return QueryResult.empty();
    }

    synchronized int executeUpdate(String sql, List<Object> parameters) throws SQLException {
        String normalizedSql = normalizeSql(sql);

        if (isSchemaStatement(normalizedSql)) {
            reset();
            return 0;
        }
        if (normalizedSql.startsWith("delete from ") && !normalizedSql.contains(" where ")) {
            return clearTable(normalizedSql);
        }
        if (normalizedSql.startsWith("insert into warehouse")) {
            insertWarehouse(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into district")) {
            insertDistrict(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into customer")) {
            insertCustomer(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into history")) {
            insertHistory(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into item")) {
            insertItem(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into stock")) {
            insertStock(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into oorder")) {
            insertOrder(normalizedSql, parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into new_order")) {
            insertNewOrder(normalizedSql, parameters);
            return 1;
        }
        if (normalizedSql.startsWith("insert into order_line")) {
            insertOrderLine(parameters);
            return 1;
        }
        if (normalizedSql.startsWith("update warehouse set w_ytd = w_ytd + ? where w_id = ?")) {
            return updateWarehouseYtd(parameters);
        }
        if (normalizedSql.startsWith("update district set d_ytd = d_ytd + ? where d_w_id = ? and d_id = ?")) {
            return updateDistrictYtd(parameters);
        }
        if (normalizedSql.startsWith("update district set d_next_o_id = d_next_o_id + 1 where d_w_id = ? and d_id = ?")) {
            return updateDistrictNextOrder(parameters);
        }
        if (normalizedSql.startsWith("update customer set c_balance = c_balance + ?")) {
            return updateCustomerDelivery(parameters);
        }
        if (normalizedSql.startsWith("update customer set c_balance = ?")) {
            return updateCustomerBalance(normalizedSql, parameters);
        }
        if (normalizedSql.startsWith("update stock set s_quantity = ?")) {
            return updateStock(parameters);
        }
        if (normalizedSql.startsWith("update oorder set o_carrier_id = ?")) {
            return updateOrderCarrier(parameters);
        }
        if (normalizedSql.startsWith("update order_line set ol_delivery_d = ?")) {
            return updateOrderLineDelivery(parameters);
        }
        if (normalizedSql.startsWith("delete from new_order where no_o_id = ? and no_d_id = ? and no_w_id = ?")) {
            return deleteNewOrder(parameters);
        }

        return 0;
    }

    private QueryResult customerByIdProjection(List<Object> parameters, List<String> columns) throws SQLException {
        Map<String, Object> row = customers.get(new CustomerKey(asInt(parameters.get(0)), asInt(parameters.get(1)), asInt(parameters.get(2))));
        return singleRowProjection(row, columns);
    }

    private QueryResult customerByLastNameProjection(List<Object> parameters) throws SQLException {
        int warehouseId = asInt(parameters.get(0));
        int districtId = asInt(parameters.get(1));
        String lastName = asString(parameters.get(2));
        List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> row : customers.values()) {
            if (asInt(row.get("C_W_ID")) == warehouseId
                    && asInt(row.get("C_D_ID")) == districtId
                    && Objects.equals(asString(row.get("C_LAST")), lastName)) {
                matches.add(row);
            }
        }
        matches.sort(Comparator.comparing(row -> asString(row.get("C_FIRST"))));
        return rowsProjection(matches, List.of(
                "C_FIRST", "C_MIDDLE", "C_ID", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP",
                "C_PHONE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT",
                "C_PAYMENT_CNT", "C_SINCE"));
    }

    private QueryResult warehouseProjection(List<Object> parameters, List<String> columns) throws SQLException {
        return singleRowProjection(warehouses.get(asInt(parameters.get(0))), columns);
    }

    private QueryResult districtProjection(List<Object> parameters, List<String> columns) throws SQLException {
        return singleRowProjection(districts.get(new DistrictKey(asInt(parameters.get(0)), asInt(parameters.get(1)))), columns);
    }

    private QueryResult itemProjection(List<Object> parameters, List<String> columns) throws SQLException {
        return singleRowProjection(items.get(asInt(parameters.get(0))), columns);
    }

    private QueryResult stockProjection(List<Object> parameters, List<String> columns) throws SQLException {
        return singleRowProjection(stocks.get(new StockKey(asInt(parameters.get(1)), asInt(parameters.get(0)))), columns);
    }

    private QueryResult nextNewOrderProjection(List<Object> parameters) {
        int districtId = asInt(parameters.get(0));
        int warehouseId = asInt(parameters.get(1));
        Integer nextOrderId = null;
        for (NewOrderKey key : newOrders.keySet()) {
            if (key.warehouseId == warehouseId && key.districtId == districtId) {
                if (nextOrderId == null || key.orderId < nextOrderId) {
                    nextOrderId = key.orderId;
                }
            }
        }
        if (nextOrderId == null) {
            return new QueryResult(List.of("NO_O_ID"), List.of());
        }
        return new QueryResult(List.of("NO_O_ID"), List.of(List.of(nextOrderId)));
    }

    private QueryResult orderProjection(List<Object> parameters, List<String> columns) throws SQLException {
        OrderKey key = new OrderKey(asInt(parameters.get(2)), asInt(parameters.get(1)), asInt(parameters.get(0)));
        return singleRowProjection(orders.get(key), columns);
    }

    private QueryResult newestOrderProjection(List<Object> parameters) {
        int warehouseId = asInt(parameters.get(0));
        int districtId = asInt(parameters.get(1));
        int customerId = asInt(parameters.get(2));
        Map<String, Object> newest = null;
        for (Map<String, Object> row : orders.values()) {
            if (asInt(row.get("O_W_ID")) == warehouseId
                    && asInt(row.get("O_D_ID")) == districtId
                    && asInt(row.get("O_C_ID")) == customerId) {
                if (newest == null || asInt(row.get("O_ID")) > asInt(newest.get("O_ID"))) {
                    newest = row;
                }
            }
        }
        if (newest == null) {
            return new QueryResult(List.of("O_ID", "O_CARRIER_ID", "O_ENTRY_D"), List.of());
        }
        return rowsProjection(List.of(newest), List.of("O_ID", "O_CARRIER_ID", "O_ENTRY_D"));
    }

    private QueryResult orderLineTotalProjection(List<Object> parameters) {
        int orderId = asInt(parameters.get(0));
        int districtId = asInt(parameters.get(1));
        int warehouseId = asInt(parameters.get(2));
        double total = 0.0d;
        boolean found = false;
        for (Map<String, Object> row : orderLines.values()) {
            if (asInt(row.get("OL_O_ID")) == orderId
                    && asInt(row.get("OL_D_ID")) == districtId
                    && asInt(row.get("OL_W_ID")) == warehouseId) {
                total += asDouble(row.get("OL_AMOUNT"));
                found = true;
            }
        }
        if (!found) {
            return new QueryResult(List.of("OL_TOTAL"), List.of());
        }
        return new QueryResult(List.of("OL_TOTAL"), List.of(List.of(total)));
    }

    private QueryResult orderLinesProjection(List<Object> parameters) {
        int orderId = asInt(parameters.get(0));
        int districtId = asInt(parameters.get(1));
        int warehouseId = asInt(parameters.get(2));
        List<Map<String, Object>> matches = new ArrayList<>();
        for (Map<String, Object> row : orderLines.values()) {
            if (asInt(row.get("OL_O_ID")) == orderId
                    && asInt(row.get("OL_D_ID")) == districtId
                    && asInt(row.get("OL_W_ID")) == warehouseId) {
                matches.add(row);
            }
        }
        matches.sort(Comparator.comparingInt(row -> asInt(row.get("OL_NUMBER"))));
        return rowsProjection(matches, List.of("OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"));
    }

    private QueryResult stockCountProjection(List<Object> parameters) {
        int warehouseId = asInt(parameters.get(0));
        int districtId = asInt(parameters.get(1));
        int upperExclusive = asInt(parameters.get(2));
        int lowerInclusive = asInt(parameters.get(3));
        int stockWarehouseId = asInt(parameters.get(4));
        int threshold = asInt(parameters.get(5));
        Set<Integer> matchingItems = new HashSet<>();
        for (Map<String, Object> row : orderLines.values()) {
            int currentOrderId = asInt(row.get("OL_O_ID"));
            if (asInt(row.get("OL_W_ID")) != warehouseId || asInt(row.get("OL_D_ID")) != districtId) {
                continue;
            }
            if (currentOrderId >= upperExclusive || currentOrderId < lowerInclusive) {
                continue;
            }
            int itemId = asInt(row.get("OL_I_ID"));
            Map<String, Object> stock = stocks.get(new StockKey(stockWarehouseId, itemId));
            if (stock != null && asInt(stock.get("S_QUANTITY")) < threshold) {
                matchingItems.add(itemId);
            }
        }
        return new QueryResult(List.of("STOCK_COUNT"), List.of(List.of(matchingItems.size())));
    }

    private int updateWarehouseYtd(List<Object> parameters) {
        Map<String, Object> warehouse = warehouses.get(asInt(parameters.get(1)));
        if (warehouse == null) {
            return 0;
        }
        warehouse.put("W_YTD", asDouble(warehouse.get("W_YTD")) + asDouble(parameters.get(0)));
        return 1;
    }

    private int updateDistrictYtd(List<Object> parameters) {
        Map<String, Object> district = districts.get(new DistrictKey(asInt(parameters.get(1)), asInt(parameters.get(2))));
        if (district == null) {
            return 0;
        }
        district.put("D_YTD", asDouble(district.get("D_YTD")) + asDouble(parameters.get(0)));
        return 1;
    }

    private int updateDistrictNextOrder(List<Object> parameters) {
        Map<String, Object> district = districts.get(new DistrictKey(asInt(parameters.get(0)), asInt(parameters.get(1))));
        if (district == null) {
            return 0;
        }
        district.put("D_NEXT_O_ID", asInt(district.get("D_NEXT_O_ID")) + 1);
        return 1;
    }

    private int updateCustomerBalance(String normalizedSql, List<Object> parameters) {
        Map<String, Object> customer = customers.get(new CustomerKey(asInt(parameters.get(parameters.size() - 3)), asInt(parameters.get(parameters.size() - 2)), asInt(parameters.get(parameters.size() - 1))));
        if (customer == null) {
            return 0;
        }
        customer.put("C_BALANCE", asDouble(parameters.get(0)));
        customer.put("C_YTD_PAYMENT", asDouble(parameters.get(1)));
        customer.put("C_PAYMENT_CNT", asInt(parameters.get(2)));
        if (normalizedSql.contains("c_data = ?")) {
            customer.put("C_DATA", asString(parameters.get(3)));
        }
        return 1;
    }

    private int updateCustomerDelivery(List<Object> parameters) {
        Map<String, Object> customer = customers.get(new CustomerKey(asInt(parameters.get(1)), asInt(parameters.get(2)), asInt(parameters.get(3))));
        if (customer == null) {
            return 0;
        }
        customer.put("C_BALANCE", asDouble(customer.get("C_BALANCE")) + asDouble(parameters.get(0)));
        customer.put("C_DELIVERY_CNT", asInt(customer.get("C_DELIVERY_CNT")) + 1);
        return 1;
    }

    private int updateStock(List<Object> parameters) {
        Map<String, Object> stock = stocks.get(new StockKey(asInt(parameters.get(4)), asInt(parameters.get(3))));
        if (stock == null) {
            return 0;
        }
        stock.put("S_QUANTITY", asInt(parameters.get(0)));
        stock.put("S_YTD", asDouble(stock.get("S_YTD")) + asDouble(parameters.get(1)));
        stock.put("S_ORDER_CNT", asInt(stock.get("S_ORDER_CNT")) + 1);
        stock.put("S_REMOTE_CNT", asInt(stock.get("S_REMOTE_CNT")) + asInt(parameters.get(2)));
        return 1;
    }

    private int updateOrderCarrier(List<Object> parameters) {
        Map<String, Object> order = orders.get(new OrderKey(asInt(parameters.get(3)), asInt(parameters.get(2)), asInt(parameters.get(1))));
        if (order == null) {
            return 0;
        }
        order.put("O_CARRIER_ID", asNullableInt(parameters.get(0)));
        return 1;
    }

    private int updateOrderLineDelivery(List<Object> parameters) {
        int updated = 0;
        int orderId = asInt(parameters.get(1));
        int districtId = asInt(parameters.get(2));
        int warehouseId = asInt(parameters.get(3));
        Timestamp deliveryTimestamp = asTimestamp(parameters.get(0));
        for (Map<String, Object> row : orderLines.values()) {
            if (asInt(row.get("OL_O_ID")) == orderId
                    && asInt(row.get("OL_D_ID")) == districtId
                    && asInt(row.get("OL_W_ID")) == warehouseId) {
                row.put("OL_DELIVERY_D", deliveryTimestamp);
                updated += 1;
            }
        }
        return updated;
    }

    private int deleteNewOrder(List<Object> parameters) {
        return newOrders.remove(new NewOrderKey(asInt(parameters.get(2)), asInt(parameters.get(1)), asInt(parameters.get(0)))) == null ? 0 : 1;
    }

    private void insertWarehouse(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("W_ID", asInt(parameters.get(0)));
        row.put("W_YTD", asDouble(parameters.get(1)));
        row.put("W_TAX", asDouble(parameters.get(2)));
        row.put("W_NAME", asString(parameters.get(3)));
        row.put("W_STREET_1", asString(parameters.get(4)));
        row.put("W_STREET_2", asString(parameters.get(5)));
        row.put("W_CITY", asString(parameters.get(6)));
        row.put("W_STATE", asString(parameters.get(7)));
        row.put("W_ZIP", asString(parameters.get(8)));
        warehouses.put(asInt(parameters.get(0)), row);
    }

    private void insertDistrict(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("D_W_ID", asInt(parameters.get(0)));
        row.put("D_ID", asInt(parameters.get(1)));
        row.put("D_YTD", asDouble(parameters.get(2)));
        row.put("D_TAX", asDouble(parameters.get(3)));
        row.put("D_NEXT_O_ID", asInt(parameters.get(4)));
        row.put("D_NAME", asString(parameters.get(5)));
        row.put("D_STREET_1", asString(parameters.get(6)));
        row.put("D_STREET_2", asString(parameters.get(7)));
        row.put("D_CITY", asString(parameters.get(8)));
        row.put("D_STATE", asString(parameters.get(9)));
        row.put("D_ZIP", asString(parameters.get(10)));
        districts.put(new DistrictKey(asInt(parameters.get(0)), asInt(parameters.get(1))), row);
    }

    private void insertCustomer(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("C_W_ID", asInt(parameters.get(0)));
        row.put("C_D_ID", asInt(parameters.get(1)));
        row.put("C_ID", asInt(parameters.get(2)));
        row.put("C_DISCOUNT", asDouble(parameters.get(3)));
        row.put("C_CREDIT", asString(parameters.get(4)));
        row.put("C_LAST", asString(parameters.get(5)));
        row.put("C_FIRST", asString(parameters.get(6)));
        row.put("C_CREDIT_LIM", asDouble(parameters.get(7)));
        row.put("C_BALANCE", asDouble(parameters.get(8)));
        row.put("C_YTD_PAYMENT", asDouble(parameters.get(9)));
        row.put("C_PAYMENT_CNT", asInt(parameters.get(10)));
        row.put("C_DELIVERY_CNT", asInt(parameters.get(11)));
        row.put("C_STREET_1", asString(parameters.get(12)));
        row.put("C_STREET_2", asString(parameters.get(13)));
        row.put("C_CITY", asString(parameters.get(14)));
        row.put("C_STATE", asString(parameters.get(15)));
        row.put("C_ZIP", asString(parameters.get(16)));
        row.put("C_PHONE", asString(parameters.get(17)));
        row.put("C_SINCE", asTimestamp(parameters.get(18)));
        row.put("C_MIDDLE", asString(parameters.get(19)));
        row.put("C_DATA", asString(parameters.get(20)));
        customers.put(new CustomerKey(asInt(parameters.get(0)), asInt(parameters.get(1)), asInt(parameters.get(2))), row);
    }

    private void insertHistory(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("H_C_D_ID", asInt(parameters.get(0)));
        row.put("H_C_W_ID", asInt(parameters.get(1)));
        row.put("H_C_ID", asInt(parameters.get(2)));
        row.put("H_D_ID", asInt(parameters.get(3)));
        row.put("H_W_ID", asInt(parameters.get(4)));
        row.put("H_DATE", asTimestamp(parameters.get(5)));
        row.put("H_AMOUNT", asDouble(parameters.get(6)));
        row.put("H_DATA", asString(parameters.get(7)));
        histories.add(row);
    }

    private void insertItem(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("I_ID", asInt(parameters.get(0)));
        row.put("I_NAME", asString(parameters.get(1)));
        row.put("I_PRICE", asDouble(parameters.get(2)));
        row.put("I_DATA", asString(parameters.get(3)));
        row.put("I_IM_ID", asInt(parameters.get(4)));
        items.put(asInt(parameters.get(0)), row);
    }

    private void insertStock(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        row.put("S_W_ID", asInt(parameters.get(0)));
        row.put("S_I_ID", asInt(parameters.get(1)));
        row.put("S_QUANTITY", asInt(parameters.get(2)));
        row.put("S_YTD", asDouble(parameters.get(3)));
        row.put("S_ORDER_CNT", asInt(parameters.get(4)));
        row.put("S_REMOTE_CNT", asInt(parameters.get(5)));
        row.put("S_DATA", asString(parameters.get(6)));
        row.put("S_DIST_01", asString(parameters.get(7)));
        row.put("S_DIST_02", asString(parameters.get(8)));
        row.put("S_DIST_03", asString(parameters.get(9)));
        row.put("S_DIST_04", asString(parameters.get(10)));
        row.put("S_DIST_05", asString(parameters.get(11)));
        row.put("S_DIST_06", asString(parameters.get(12)));
        row.put("S_DIST_07", asString(parameters.get(13)));
        row.put("S_DIST_08", asString(parameters.get(14)));
        row.put("S_DIST_09", asString(parameters.get(15)));
        row.put("S_DIST_10", asString(parameters.get(16)));
        stocks.put(new StockKey(asInt(parameters.get(0)), asInt(parameters.get(1))), row);
    }

    private void insertOrder(String normalizedSql, List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        if (parameters.size() == 8) {
            row.put("O_W_ID", asInt(parameters.get(0)));
            row.put("O_D_ID", asInt(parameters.get(1)));
            row.put("O_ID", asInt(parameters.get(2)));
            row.put("O_C_ID", asInt(parameters.get(3)));
            row.put("O_CARRIER_ID", asNullableInt(parameters.get(4)));
            row.put("O_OL_CNT", asInt(parameters.get(5)));
            row.put("O_ALL_LOCAL", asInt(parameters.get(6)));
            row.put("O_ENTRY_D", asTimestamp(parameters.get(7)));
        } else {
            row.put("O_ID", asInt(parameters.get(0)));
            row.put("O_D_ID", asInt(parameters.get(1)));
            row.put("O_W_ID", asInt(parameters.get(2)));
            row.put("O_C_ID", asInt(parameters.get(3)));
            row.put("O_ENTRY_D", asTimestamp(parameters.get(4)));
            row.put("O_OL_CNT", asInt(parameters.get(5)));
            row.put("O_ALL_LOCAL", asInt(parameters.get(6)));
            row.put("O_CARRIER_ID", normalizedSql.contains("o_carrier_id") ? asNullableInt(parameters.get(4)) : null);
        }
        orders.put(new OrderKey(asInt(row.get("O_W_ID")), asInt(row.get("O_D_ID")), asInt(row.get("O_ID"))), row);
    }

    private void insertNewOrder(String normalizedSql, List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        if (normalizedSql.contains("(no_o_id")) {
            row.put("NO_O_ID", asInt(parameters.get(0)));
            row.put("NO_D_ID", asInt(parameters.get(1)));
            row.put("NO_W_ID", asInt(parameters.get(2)));
        } else {
            row.put("NO_W_ID", asInt(parameters.get(0)));
            row.put("NO_D_ID", asInt(parameters.get(1)));
            row.put("NO_O_ID", asInt(parameters.get(2)));
        }
        newOrders.put(new NewOrderKey(asInt(row.get("NO_W_ID")), asInt(row.get("NO_D_ID")), asInt(row.get("NO_O_ID"))), row);
    }

    private void insertOrderLine(List<Object> parameters) {
        Map<String, Object> row = new HashMap<>();
        if (parameters.size() == 10) {
            row.put("OL_W_ID", asInt(parameters.get(0)));
            row.put("OL_D_ID", asInt(parameters.get(1)));
            row.put("OL_O_ID", asInt(parameters.get(2)));
            row.put("OL_NUMBER", asInt(parameters.get(3)));
            row.put("OL_I_ID", asInt(parameters.get(4)));
            row.put("OL_DELIVERY_D", asTimestamp(parameters.get(5)));
            row.put("OL_AMOUNT", asDouble(parameters.get(6)));
            row.put("OL_SUPPLY_W_ID", asInt(parameters.get(7)));
            row.put("OL_QUANTITY", asInt(parameters.get(8)));
            row.put("OL_DIST_INFO", asString(parameters.get(9)));
        } else {
            row.put("OL_O_ID", asInt(parameters.get(0)));
            row.put("OL_D_ID", asInt(parameters.get(1)));
            row.put("OL_W_ID", asInt(parameters.get(2)));
            row.put("OL_NUMBER", asInt(parameters.get(3)));
            row.put("OL_I_ID", asInt(parameters.get(4)));
            row.put("OL_SUPPLY_W_ID", asInt(parameters.get(5)));
            row.put("OL_QUANTITY", asInt(parameters.get(6)));
            row.put("OL_AMOUNT", asDouble(parameters.get(7)));
            row.put("OL_DIST_INFO", asString(parameters.get(8)));
            row.put("OL_DELIVERY_D", null);
        }
        orderLines.put(new OrderLineKey(asInt(row.get("OL_W_ID")), asInt(row.get("OL_D_ID")), asInt(row.get("OL_O_ID")), asInt(row.get("OL_NUMBER"))), row);
    }

    private QueryResult singleRowProjection(Map<String, Object> row, List<String> columns) throws SQLException {
        if (row == null) {
            return new QueryResult(columns, List.of());
        }
        return rowsProjection(List.of(row), columns);
    }

    private QueryResult rowsProjection(List<Map<String, Object>> rows, List<String> columns) {
        List<List<Object>> resultRows = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            List<Object> values = new ArrayList<>(columns.size());
            for (String column : columns) {
                values.add(row.get(column));
            }
            resultRows.add(values);
        }
        return new QueryResult(columns, resultRows);
    }

    private int clearTable(String normalizedSql) {
        if (normalizedSql.startsWith("delete from warehouse")) {
            int count = warehouses.size();
            warehouses.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from district")) {
            int count = districts.size();
            districts.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from customer")) {
            int count = customers.size();
            customers.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from history")) {
            int count = histories.size();
            histories.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from item")) {
            int count = items.size();
            items.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from stock")) {
            int count = stocks.size();
            stocks.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from oorder")) {
            int count = orders.size();
            orders.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from new_order")) {
            int count = newOrders.size();
            newOrders.clear();
            return count;
        }
        if (normalizedSql.startsWith("delete from order_line")) {
            int count = orderLines.size();
            orderLines.clear();
            return count;
        }
        return 0;
    }

    private void reset() {
        warehouses.clear();
        districts.clear();
        customers.clear();
        items.clear();
        stocks.clear();
        orders.clear();
        newOrders.clear();
        orderLines.clear();
        histories.clear();
    }

    private boolean isSchemaStatement(String normalizedSql) {
        return normalizedSql.startsWith("create table")
                || normalizedSql.startsWith("drop table")
                || normalizedSql.startsWith("create index")
                || normalizedSql.startsWith("drop index")
                || normalizedSql.startsWith("alter table")
                || normalizedSql.startsWith("truncate table");
    }

    private static String normalizeSql(String sql) {
        return sql == null
                ? ""
                : sql.toLowerCase(Locale.ROOT)
                        .replace('"', ' ')
                        .replace(';', ' ')
                .replaceAll("\\s*,\\s*", ", ")
                .replaceAll("\\(\\s+", "(")
                .replaceAll("\\s+\\)", ")")
                        .replaceAll("\\s+", " ")
                        .trim();
    }

    private static int asInt(Object value) {
        return ((Number) value).intValue();
    }

    private static Integer asNullableInt(Object value) {
        return value == null ? null : asInt(value);
    }

    private static double asDouble(Object value) {
        return value == null ? 0.0d : ((Number) value).doubleValue();
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Timestamp asTimestamp(Object value) {
        return value == null ? null : (Timestamp) value;
    }

    private static final class DistrictKey {
        private final int warehouseId;
        private final int districtId;

        private DistrictKey(int warehouseId, int districtId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DistrictKey)) {
                return false;
            }
            DistrictKey that = (DistrictKey) other;
            return warehouseId == that.warehouseId && districtId == that.districtId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, districtId);
        }
    }

    private static final class CustomerKey {
        private final int warehouseId;
        private final int districtId;
        private final int customerId;

        private CustomerKey(int warehouseId, int districtId, int customerId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerId = customerId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof CustomerKey)) {
                return false;
            }
            CustomerKey that = (CustomerKey) other;
            return warehouseId == that.warehouseId && districtId == that.districtId && customerId == that.customerId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, districtId, customerId);
        }
    }

    private static final class StockKey {
        private final int warehouseId;
        private final int itemId;

        private StockKey(int warehouseId, int itemId) {
            this.warehouseId = warehouseId;
            this.itemId = itemId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof StockKey)) {
                return false;
            }
            StockKey that = (StockKey) other;
            return warehouseId == that.warehouseId && itemId == that.itemId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, itemId);
        }
    }

    private static final class OrderKey {
        private final int warehouseId;
        private final int districtId;
        private final int orderId;

        private OrderKey(int warehouseId, int districtId, int orderId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.orderId = orderId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof OrderKey)) {
                return false;
            }
            OrderKey that = (OrderKey) other;
            return warehouseId == that.warehouseId && districtId == that.districtId && orderId == that.orderId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, districtId, orderId);
        }
    }

    private static final class NewOrderKey {
        private final int warehouseId;
        private final int districtId;
        private final int orderId;

        private NewOrderKey(int warehouseId, int districtId, int orderId) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.orderId = orderId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof NewOrderKey)) {
                return false;
            }
            NewOrderKey that = (NewOrderKey) other;
            return warehouseId == that.warehouseId && districtId == that.districtId && orderId == that.orderId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, districtId, orderId);
        }
    }

    private static final class OrderLineKey {
        private final int warehouseId;
        private final int districtId;
        private final int orderId;
        private final int number;

        private OrderLineKey(int warehouseId, int districtId, int orderId, int number) {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.orderId = orderId;
            this.number = number;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof OrderLineKey)) {
                return false;
            }
            OrderLineKey that = (OrderLineKey) other;
            return warehouseId == that.warehouseId && districtId == that.districtId && orderId == that.orderId && number == that.number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warehouseId, districtId, orderId, number);
        }
    }
}