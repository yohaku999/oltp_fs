package dev.yohaku.dbfs.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.junit.jupiter.api.Test;

class InMemoryDbfsClientTest {
    @Test
    void tpccRowsPersistAcrossConnectionsAndSupportNewOrderQueries() throws SQLException {
        String url = "jdbc:dbfs://localhost/tpcc-new-order";
        Timestamp now = new Timestamp(System.currentTimeMillis());

        try (Connection connection = DriverManager.getConnection(url)) {
            resetStore(connection);
            insertWarehouse(connection, 1);
            insertDistrict(connection, 1, 1, 3001);
            insertCustomer(connection, 1, 1, 1, "BARBARBAR", "ALICE", now);
            insertItem(connection, 1, 12.5d);
            insertStock(connection, 1, 1, 91);

            try (PreparedStatement insertOrderLine = connection.prepareStatement(
                    "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                insertOrderLine.setInt(1, 3001);
                insertOrderLine.setInt(2, 1);
                insertOrderLine.setInt(3, 1);
                insertOrderLine.setInt(4, 1);
                insertOrderLine.setInt(5, 1);
                insertOrderLine.setInt(6, 1);
                insertOrderLine.setInt(7, 5);
                insertOrderLine.setDouble(8, 62.5d);
                insertOrderLine.setString(9, "DIST-1");
                insertOrderLine.addBatch();
                assertEquals(1, insertOrderLine.executeBatch()[0]);
            }

            try (PreparedStatement updateDistrict = connection.prepareStatement(
                    "UPDATE DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?")) {
                updateDistrict.setInt(1, 1);
                updateDistrict.setInt(2, 1);
                assertEquals(1, updateDistrict.executeUpdate());
            }

            try (PreparedStatement insertOrder = connection.prepareStatement(
                    "INSERT INTO OORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                insertOrder.setInt(1, 3001);
                insertOrder.setInt(2, 1);
                insertOrder.setInt(3, 1);
                insertOrder.setInt(4, 1);
                insertOrder.setTimestamp(5, now);
                insertOrder.setInt(6, 1);
                insertOrder.setInt(7, 1);
                assertEquals(1, insertOrder.executeUpdate());
            }

            try (PreparedStatement insertNewOrder = connection.prepareStatement(
                    "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)")) {
                insertNewOrder.setInt(1, 3001);
                insertNewOrder.setInt(2, 1);
                insertNewOrder.setInt(3, 1);
                assertEquals(1, insertNewOrder.executeUpdate());
            }

            try (PreparedStatement updateStock = connection.prepareStatement(
                    "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? WHERE S_I_ID = ? AND S_W_ID = ?")) {
                updateStock.setInt(1, 86);
                updateStock.setInt(2, 5);
                updateStock.setInt(3, 0);
                updateStock.setInt(4, 1);
                updateStock.setInt(5, 1);
                assertEquals(1, updateStock.executeUpdate());
            }
        }

        try (Connection connection = DriverManager.getConnection(url)) {
            try (PreparedStatement customer = connection.prepareStatement(
                    "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?")) {
                customer.setInt(1, 1);
                customer.setInt(2, 1);
                customer.setInt(3, 1);
                try (ResultSet resultSet = customer.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals("BARBARBAR", resultSet.getString("C_LAST"));
                    assertEquals(0.1f, resultSet.getFloat("C_DISCOUNT"));
                }
            }

            try (PreparedStatement district = connection.prepareStatement(
                    "SELECT D_NEXT_O_ID, D_TAX FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE")) {
                district.setInt(1, 1);
                district.setInt(2, 1);
                try (ResultSet resultSet = district.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(3002, resultSet.getInt("D_NEXT_O_ID"));
                    assertEquals(0.07d, resultSet.getDouble("D_TAX"));
                }
            }

            try (PreparedStatement item = connection.prepareStatement(
                    "SELECT I_PRICE, I_NAME , I_DATA FROM ITEM WHERE I_ID = ?")) {
                item.setInt(1, 1);
                try (ResultSet resultSet = item.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(12.5f, resultSet.getFloat("I_PRICE"));
                }
            }

            try (PreparedStatement stock = connection.prepareStatement(
                    "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10 FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ? FOR UPDATE")) {
                stock.setInt(1, 1);
                stock.setInt(2, 1);
                try (ResultSet resultSet = stock.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(86, resultSet.getInt("S_QUANTITY"));
                    assertEquals("DIST-1", resultSet.getString("S_DIST_01"));
                }
            }

            try (PreparedStatement orderLineTotal = connection.prepareStatement(
                    "SELECT SUM(OL_AMOUNT) AS OL_TOTAL FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?")) {
                orderLineTotal.setInt(1, 3001);
                orderLineTotal.setInt(2, 1);
                orderLineTotal.setInt(3, 1);
                try (ResultSet resultSet = orderLineTotal.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(62.5d, resultSet.getDouble("OL_TOTAL"));
                }
            }
        }
    }

    @Test
    void tpccQueriesSupportDeliveryAndStockLevelPatterns() throws SQLException {
        String url = "jdbc:dbfs://localhost/tpcc-delivery";
        Timestamp now = new Timestamp(System.currentTimeMillis());

        try (Connection connection = DriverManager.getConnection(url)) {
            resetStore(connection);
            insertWarehouse(connection, 1);
            insertDistrict(connection, 1, 1, 3005);
            insertCustomer(connection, 1, 1, 11, "BARBARBAR", "ALICE", now);
            insertCustomer(connection, 1, 1, 12, "BARBARBAR", "BOB", now);
            insertItem(connection, 1, 5.0d);
            insertStock(connection, 1, 1, 3);

            try (PreparedStatement insertOrder = connection.prepareStatement(
                    "INSERT INTO OORDER (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                insertOrder.setInt(1, 1);
                insertOrder.setInt(2, 1);
                insertOrder.setInt(3, 3004);
                insertOrder.setInt(4, 12);
                insertOrder.setNull(5, java.sql.Types.INTEGER);
                insertOrder.setInt(6, 1);
                insertOrder.setInt(7, 1);
                insertOrder.setTimestamp(8, now);
                insertOrder.addBatch();
                assertEquals(1, insertOrder.executeBatch()[0]);
            }

            try (PreparedStatement insertNewOrder = connection.prepareStatement(
                    "INSERT INTO NEW_ORDER (NO_W_ID, NO_D_ID, NO_O_ID) VALUES (?, ?, ?)")) {
                insertNewOrder.setInt(1, 1);
                insertNewOrder.setInt(2, 1);
                insertNewOrder.setInt(3, 3004);
                insertNewOrder.addBatch();
                assertEquals(1, insertNewOrder.executeBatch()[0]);
            }

            try (PreparedStatement insertOrderLine = connection.prepareStatement(
                    "INSERT INTO ORDER_LINE (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                insertOrderLine.setInt(1, 1);
                insertOrderLine.setInt(2, 1);
                insertOrderLine.setInt(3, 3004);
                insertOrderLine.setInt(4, 1);
                insertOrderLine.setInt(5, 1);
                insertOrderLine.setNull(6, java.sql.Types.TIMESTAMP);
                insertOrderLine.setDouble(7, 15.0d);
                insertOrderLine.setInt(8, 1);
                insertOrderLine.setInt(9, 5);
                insertOrderLine.setString(10, "DIST-1");
                insertOrderLine.addBatch();
                assertEquals(1, insertOrderLine.executeBatch()[0]);
            }
        }

        try (Connection connection = DriverManager.getConnection(url)) {
            try (PreparedStatement byLastName = connection.prepareStatement(
                    "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST")) {
                byLastName.setInt(1, 1);
                byLastName.setInt(2, 1);
                byLastName.setString(3, "BARBARBAR");
                try (ResultSet resultSet = byLastName.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(11, resultSet.getInt("C_ID"));
                    assertTrue(resultSet.next());
                    assertEquals(12, resultSet.getInt("C_ID"));
                }
            }

            try (PreparedStatement nextNewOrder = connection.prepareStatement(
                    "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? ORDER BY NO_O_ID ASC LIMIT 1")) {
                nextNewOrder.setInt(1, 1);
                nextNewOrder.setInt(2, 1);
                try (ResultSet resultSet = nextNewOrder.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(3004, resultSet.getInt("NO_O_ID"));
                }
            }

            try (PreparedStatement customerId = connection.prepareStatement(
                    "SELECT O_C_ID FROM OORDER WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?")) {
                customerId.setInt(1, 3004);
                customerId.setInt(2, 1);
                customerId.setInt(3, 1);
                try (ResultSet resultSet = customerId.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(12, resultSet.getInt("O_C_ID"));
                }
            }

            try (PreparedStatement newestOrder = connection.prepareStatement(
                    "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM OORDER WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1")) {
                newestOrder.setInt(1, 1);
                newestOrder.setInt(2, 1);
                newestOrder.setInt(3, 12);
                try (ResultSet resultSet = newestOrder.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(3004, resultSet.getInt("O_ID"));
                    assertNotNull(resultSet.getTimestamp("O_ENTRY_D"));
                }
            }

            try (PreparedStatement updateCarrier = connection.prepareStatement(
                    "UPDATE OORDER SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?")) {
                updateCarrier.setInt(1, 7);
                updateCarrier.setInt(2, 3004);
                updateCarrier.setInt(3, 1);
                updateCarrier.setInt(4, 1);
                assertEquals(1, updateCarrier.executeUpdate());
            }

            try (PreparedStatement updateDelivery = connection.prepareStatement(
                    "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?")) {
                updateDelivery.setTimestamp(1, now);
                updateDelivery.setInt(2, 3004);
                updateDelivery.setInt(3, 1);
                updateDelivery.setInt(4, 1);
                assertEquals(1, updateDelivery.executeUpdate());
            }

            try (PreparedStatement orderLines = connection.prepareStatement(
                    "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?")) {
                orderLines.setInt(1, 3004);
                orderLines.setInt(2, 1);
                orderLines.setInt(3, 1);
                try (ResultSet resultSet = orderLines.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getInt("OL_I_ID"));
                    assertNotNull(resultSet.getTimestamp("OL_DELIVERY_D"));
                }
            }

            try (PreparedStatement stockCount = connection.prepareStatement(
                    "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?")) {
                stockCount.setInt(1, 1);
                stockCount.setInt(2, 1);
                stockCount.setInt(3, 3005);
                stockCount.setInt(4, 2985);
                stockCount.setInt(5, 1);
                stockCount.setInt(6, 10);
                try (ResultSet resultSet = stockCount.executeQuery()) {
                    assertTrue(resultSet.next());
                    assertEquals(1, resultSet.getInt("STOCK_COUNT"));
                }
            }

            try (PreparedStatement deleteNewOrder = connection.prepareStatement(
                    "DELETE FROM NEW_ORDER WHERE NO_O_ID = ? AND NO_D_ID = ? AND NO_W_ID = ?")) {
                deleteNewOrder.setInt(1, 3004);
                deleteNewOrder.setInt(2, 1);
                deleteNewOrder.setInt(3, 1);
                assertEquals(1, deleteNewOrder.executeUpdate());
            }
        }
    }

    private void resetStore(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS CUSTOMER");
        }
    }

    private void insertWarehouse(Connection connection, int warehouseId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setInt(1, warehouseId);
            statement.setDouble(2, 300000.0d);
            statement.setDouble(3, 0.05d);
            statement.setString(4, "W-1");
            statement.setString(5, "STREET-1");
            statement.setString(6, "STREET-2");
            statement.setString(7, "CITY");
            statement.setString(8, "ST");
            statement.setString(9, "123456789");
            statement.executeUpdate();
        }
    }

    private void insertDistrict(Connection connection, int warehouseId, int districtId, int nextOrderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setDouble(3, 30000.0d);
            statement.setDouble(4, 0.07d);
            statement.setInt(5, nextOrderId);
            statement.setString(6, "D-1");
            statement.setString(7, "D-STREET-1");
            statement.setString(8, "D-STREET-2");
            statement.setString(9, "D-CITY");
            statement.setString(10, "DS");
            statement.setString(11, "123456789");
            statement.executeUpdate();
        }
    }

    private void insertCustomer(Connection connection, int warehouseId, int districtId, int customerId, String lastName, String firstName, Timestamp since) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, districtId);
            statement.setInt(3, customerId);
            statement.setDouble(4, 0.1d);
            statement.setString(5, "GC");
            statement.setString(6, lastName);
            statement.setString(7, firstName);
            statement.setDouble(8, 50000.0d);
            statement.setDouble(9, -10.0d);
            statement.setDouble(10, 10.0d);
            statement.setInt(11, 1);
            statement.setInt(12, 0);
            statement.setString(13, "C-STREET-1");
            statement.setString(14, "C-STREET-2");
            statement.setString(15, "C-CITY");
            statement.setString(16, "CS");
            statement.setString(17, "1234511111");
            statement.setString(18, "0123456789");
            statement.setTimestamp(19, since);
            statement.setString(20, "OE");
            statement.setString(21, "C-DATA");
            statement.executeUpdate();
        }
    }

    private void insertItem(Connection connection, int itemId, double price) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO ITEM VALUES (?, ?, ?, ?, ?)")) {
            statement.setInt(1, itemId);
            statement.setString(2, "ITEM-1");
            statement.setDouble(3, price);
            statement.setString(4, "ITEM-DATA");
            statement.setInt(5, 10);
            statement.executeUpdate();
        }
    }

    private void insertStock(Connection connection, int warehouseId, int itemId, int quantity) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setInt(1, warehouseId);
            statement.setInt(2, itemId);
            statement.setInt(3, quantity);
            statement.setDouble(4, 0.0d);
            statement.setInt(5, 0);
            statement.setInt(6, 0);
            statement.setString(7, "S-DATA");
            for (int index = 8; index <= 17; index += 1) {
                statement.setString(index, "DIST-1");
            }
            statement.executeUpdate();
        }
    }
}