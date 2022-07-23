/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.lsd.tpcc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.lsd.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.lsd.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.lsd.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.lsd.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.lsd.tpcc.pojo.FutureStock;
import kotlin.Unit;
import lsd.v2.api.Future;
import lsd.v2.api.FutureConnection;
import lsd.v2.api.FutureResultSet;
import lsd.v2.api.PreparedFutureStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

public class NewOrder extends FutureTPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
            "SELECT W_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
                    " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX " +
                    "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                    " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                    " VALUES ( ?, ?, ?)");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
                    "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                    " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_ITEM +
                    " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
                    "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                    "  FROM " + TPCCConstants.TABLENAME_STOCK +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                    "   SET S_QUANTITY = ? , " +
                    "       S_YTD = S_YTD + ?, " +
                    "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
                    "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
                    " WHERE S_I_ID = ? " +
                    "   AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    " VALUES (?,?,?,?,?,?,?,?,?)");


    public void run(FutureConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException {
        if (!(conn instanceof FutureConnection)) {
            throw new UnsupportedOperationException("Regular connection when LSD connection is necessary!");
        }

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);

        int numItems = TPCCUtil.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;

        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID(gen);
            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, numWarehouses, gen);
                }
                while (supplierWarehouseIDs[i] == terminalWarehouseID && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;
        }

        newOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, (FutureConnection) conn);

    }


    private void newOrderTransaction(int w_id, int d_id, int c_id,
                                     int o_ol_cnt, int o_all_local, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities, FutureConnection conn) throws SQLException {


        getCustomer(conn, w_id, d_id, c_id);

        getWarehouse(conn, w_id);

        Future<Integer> d_next_o_id = getDistrict(conn, w_id, d_id);

        updateDistrict(conn, w_id, d_id);

        insertOpenOrder(conn, w_id, d_id, c_id, o_ol_cnt, o_all_local, d_next_o_id);

        insertNewOrder(conn, w_id, d_id, d_next_o_id);

        PreparedFutureStatement stmtUpdateStock = this.getFuturePreparedStatement(conn, stmtUpdateStockSQL);
        PreparedFutureStatement stmtInsertOrderLine = this.getFuturePreparedStatement(conn, stmtInsertOrderLineSQL);

        for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
            int ol_i_id = itemIDs[ol_number - 1];
            int ol_quantity = orderQuantities[ol_number - 1];

            // this may occasionally error and that's ok!
            var i_price = getItemPrice(conn, ol_i_id);

            Future<Float> ol_amount = new Future<>() {
                @Override
                public Float resolve() {
                    return ol_quantity * i_price.resolve();
                }

                @Override
                public void dispose() {

                }
            };

            FutureStock s = getStock(conn, ol_supply_w_id, ol_i_id, ol_quantity);

            var ol_dist_info = getDistInfo(d_id, s);

            stmtInsertOrderLine.setFutureInt(1, d_next_o_id);
            stmtInsertOrderLine.setInt(2, d_id);
            stmtInsertOrderLine.setInt(3, w_id);
            stmtInsertOrderLine.setInt(4, ol_number);
            stmtInsertOrderLine.setInt(5, ol_i_id);
            stmtInsertOrderLine.setInt(6, ol_supply_w_id);
            stmtInsertOrderLine.setInt(7, ol_quantity);
            stmtInsertOrderLine.setFutureFloat(8, ol_amount);
            stmtInsertOrderLine.setFutureString(9, ol_dist_info);
            stmtInsertOrderLine.addFutureBatch();

            int s_remote_cnt_increment;

            if (ol_supply_w_id == w_id) {
                s_remote_cnt_increment = 0;
            } else {
                s_remote_cnt_increment = 1;
            }

            stmtUpdateStock.setFutureInt(1, s.s_quantity);
            stmtUpdateStock.setInt(2, ol_quantity);
            stmtUpdateStock.setInt(3, s_remote_cnt_increment);
            stmtUpdateStock.setInt(4, ol_i_id);
            stmtUpdateStock.setInt(5, ol_supply_w_id);
            stmtUpdateStock.addFutureBatch();

        }
        stmtInsertOrderLine.executeFutureBatch();

        stmtUpdateStock.executeFutureBatch();
    }

    private Future<String> getDistInfo(int d_id, FutureStock s) {
        switch (d_id) {
            case 1: {
				return s.s_dist_01;
			}
            case 2: {
				return s.s_dist_02;
			}
            case 3: {
				return s.s_dist_03;
			}
            case 4: {
				return s.s_dist_04;
			}
            case 5: {
				return s.s_dist_05;
			}
            case 6: {
				return s.s_dist_06;
			}
            case 7: {
				return s.s_dist_07;
			}
            case 8: {
				return s.s_dist_08;
			}
            case 9: {
				return s.s_dist_09;
			}
            case 10: {
				return s.s_dist_10;
			}
            default: {
				return null;
			}
        }
    }

    private FutureStock getStock(FutureConnection conn, int ol_supply_w_id, int ol_i_id, int ol_quantity) throws SQLException {
        PreparedFutureStatement stmtGetStock = this.getFuturePreparedStatement(conn, stmtGetStockSQL);
        stmtGetStock.setInt(1, ol_i_id);
        stmtGetStock.setInt(2, ol_supply_w_id);
        FutureResultSet result = stmtGetStock.executeFutureQuery();

        stmtGetStock.afterQueryExecution(operationResultSet -> {
            try {
                if (operationResultSet.get().isClosed()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " not found!");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return Unit.INSTANCE;
        });

        FutureStock s = new FutureStock();
//        s.s_quantity = result.getFutureInt("S_QUANTITY");
        s.s_dist_01 = result.getFutureString("S_DIST_01");
        s.s_dist_02 = result.getFutureString("S_DIST_02");
        s.s_dist_03 = result.getFutureString("S_DIST_03");
        s.s_dist_04 = result.getFutureString("S_DIST_04");
        s.s_dist_05 = result.getFutureString("S_DIST_05");
        s.s_dist_06 = result.getFutureString("S_DIST_06");
        s.s_dist_07 = result.getFutureString("S_DIST_07");
        s.s_dist_08 = result.getFutureString("S_DIST_08");
        s.s_dist_09 = result.getFutureString("S_DIST_09");
        s.s_dist_10 = result.getFutureString("S_DIST_10");

        s.s_quantity = new Future<>() {
            @Override
            public Integer resolve() {
                Integer resolved_stock = result.getFutureInt("S_QUANTITY").resolve();
                if (resolved_stock - ol_quantity >= 10) {
                    return resolved_stock - ol_quantity;
                } else {
                    return resolved_stock - ol_quantity + 91;
                }
            }

            @Override
            public void dispose() {

            }
        };

        return s;
    }

    private Future<Float> getItemPrice(FutureConnection conn, int ol_i_id) throws SQLException {
        PreparedFutureStatement stmtGetItem = this.getFuturePreparedStatement(conn, stmtGetItemSQL);
        stmtGetItem.setInt(1, ol_i_id);


        stmtGetItem.afterQueryExecution((operationResult -> {
            try {
                if (operationResult.get().isClosed()) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException("EXPECTED new order rollback: I_ID=" + ol_i_id + " not found!");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
            return Unit.INSTANCE;
        }));

        return stmtGetItem.executeFutureQuery().getFutureFloat("I_PRICE");
    }

    private void insertNewOrder(FutureConnection conn, int w_id, int d_id, Future<Integer> o_id) throws SQLException {
        PreparedFutureStatement stmtInsertNewOrder = this.getFuturePreparedStatement(conn, stmtInsertNewOrderSQL);
        stmtInsertNewOrder.setFutureInt(1, o_id);
        stmtInsertNewOrder.setInt(2, d_id);
        stmtInsertNewOrder.setInt(3, w_id);
        stmtInsertNewOrder.executeFutureUpdate();

        stmtInsertNewOrder.afterUpdateExecution((operationResult -> {
            if (operationResult.get() == 0) {
                LOG.warn("new order not inserted");
            }
            return Unit.INSTANCE;
        }));
    }

    private void insertOpenOrder(FutureConnection conn, int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, Future<Integer> o_id) throws SQLException {
        PreparedFutureStatement stmtInsertOOrder = this.getFuturePreparedStatement(conn, stmtInsertOOrderSQL);
        stmtInsertOOrder.setFutureInt(1, o_id);
        stmtInsertOOrder.setInt(2, d_id);
        stmtInsertOOrder.setInt(3, w_id);
        stmtInsertOOrder.setInt(4, c_id);
        stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
        stmtInsertOOrder.setInt(6, o_ol_cnt);
        stmtInsertOOrder.setInt(7, o_all_local);

        stmtInsertOOrder.executeFutureUpdate();

        stmtInsertOOrder.afterUpdateExecution((operationResult -> {
            if (operationResult.get() == 0) {
                LOG.warn("open order not inserted");
            }
            return Unit.INSTANCE;
        }));
    }

    private void updateDistrict(FutureConnection conn, int w_id, int d_id) throws SQLException {
        PreparedFutureStatement stmtUpdateDist = this.getFuturePreparedStatement(conn, stmtUpdateDistSQL);
        stmtUpdateDist.setInt(1, w_id);
        stmtUpdateDist.setInt(2, d_id);

        stmtUpdateDist.executeFutureUpdate();
        stmtUpdateDist.afterUpdateExecution((operationResult -> {
            if (operationResult.get() == 0) {
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
            }
            return Unit.INSTANCE;
        }));
    }

    private Future<Integer> getDistrict(FutureConnection conn, int w_id, int d_id) throws SQLException {
        PreparedFutureStatement stmtGetDist = this.getFuturePreparedStatement(conn, stmtGetDistSQL);
        stmtGetDist.setInt(1, w_id);
        stmtGetDist.setInt(2, d_id);

        stmtGetDist.afterQueryExecution((operationResultSet -> {
            try {
                if (operationResultSet.get().isClosed()) {
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unexpected exception!");
            }
            return Unit.INSTANCE;
        }));

        return stmtGetDist.executeFutureQuery().getFutureInt("D_NEXT_O_ID");
    }


    private void getWarehouse(FutureConnection conn, int w_id) throws SQLException {
        PreparedFutureStatement stmtGetWhse = this.getFuturePreparedStatement(conn, stmtGetWhseSQL);
        stmtGetWhse.setInt(1, w_id);
        stmtGetWhse.executeFutureQuery();

        stmtGetWhse.afterQueryExecution((operationResultSet -> {
            try {
                if (operationResultSet.get().isClosed()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unexpected exception!");
            }
            return Unit.INSTANCE;
        }));
    }


    private void getCustomer(FutureConnection conn, int w_id, int d_id, int c_id) throws SQLException {
        PreparedFutureStatement stmtGetCust = this.getFuturePreparedStatement(conn, stmtGetCustSQL);
        stmtGetCust.setInt(1, w_id);
        stmtGetCust.setInt(2, d_id);
        stmtGetCust.setInt(3, c_id);
        stmtGetCust.executeFutureQuery();

        stmtGetCust.afterQueryExecution((operationResultSet -> {
            try {
                if (operationResultSet.get().isClosed()) {
                    throw new RuntimeException("C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }
            } catch (SQLException e) {
                throw new RuntimeException("Unexpected exception!");
            }
            return Unit.INSTANCE;
        }));
    }

}
