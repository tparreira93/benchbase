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
import com.oltpbenchmark.benchmarks.lsd.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.lsd.tpcc.pojo.FutureCustomer;
import com.oltpbenchmark.benchmarks.lsd.tpcc.pojo.FutureDistrict;
import com.oltpbenchmark.benchmarks.lsd.tpcc.pojo.FutureWarehouse;
import kotlin.Unit;
import trxsys.lsd.api.FutureResultChain;
import trxsys.lsd.future.Future;
import trxsys.lsd.api.FutureConnection;
import trxsys.lsd.api.FutureResultSet;
import trxsys.lsd.api.FutureStatementCondition;
import trxsys.lsd.api.PreparedFutureStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;

public class Payment extends FutureTPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Payment.class);

    public SQLStmt payUpdateWhseSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE +
                    "   SET W_YTD = W_YTD + ? " +
                    " WHERE W_ID = ? ");

    public SQLStmt payGetWhseSQL = new SQLStmt(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" +
                    "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
                    " WHERE W_ID = ?");

    public SQLStmt payUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
                    "   SET D_YTD = D_YTD + ? " +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public SQLStmt payGetDistSQL = new SQLStmt(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" +
                    "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
                    " WHERE D_W_ID = ? " +
                    "   AND D_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
                    "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
                    "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public SQLStmt payGetCustCdataSQL = new SQLStmt(
            "SELECT C_DATA " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = ?, " +
                    "       C_YTD_PAYMENT = ?, " +
                    "       C_PAYMENT_CNT = ?, " +
                    "       C_DATA = ? " +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
                    "   SET C_BALANCE = ?, " +
                    "       C_YTD_PAYMENT = ?, " +
                    "       C_PAYMENT_CNT = ? " +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_ID = ?");

    public SQLStmt payInsertHistSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
                    " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
                    " VALUES (?,?,?,?,?,?,?,?)");

    public SQLStmt customerByNameSQL = new SQLStmt(ResultSet.TYPE_SCROLL_INSENSITIVE,
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
                    "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
                    "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
                    "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
                    " WHERE C_W_ID = ? " +
                    "   AND C_D_ID = ? " +
                    "   AND C_LAST = ? " +
                    " ORDER BY C_FIRST");

    public void run(FutureConnection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker worker) throws SQLException {
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        updateWarehouse(conn, w_id, paymentAmount);

        FutureWarehouse w = getWarehouse(conn, w_id);

        updateDistrict(conn, w_id, districtID, paymentAmount);

        FutureDistrict d = getDistrict(conn, w_id, districtID);

        int x = TPCCUtil.randomNumber(1, 100, gen);

        int customerDistrictID = getCustomerDistrictId(gen, districtID, x);
        int customerWarehouseID = getCustomerWarehouseID(gen, w_id, numWarehouses, x);

        FutureCustomer c = getCustomer(conn, gen, customerDistrictID, customerWarehouseID, paymentAmount);

        FutureStatementCondition creditStatus = conn.isTrue("? = ? ");
        creditStatus.setFutureString(1, TPCCUtil.newFuture(() -> c.c_credit.resolve()));
        creditStatus.setString(2, "BC");

        creditStatus.whenTrue(() -> {
            try {
                c.c_data = getCData(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, c);

                updateBalanceCData(conn, customerDistrictID, customerWarehouseID, c);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });

        creditStatus.whenFalse(() -> {
            try {
                updateBalance(conn, customerDistrictID, customerWarehouseID, c);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });

//        if (c.c_credit.equals("BC")) {
//            // bad credit
//            c.c_data = getCData(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, c);
//
//            updateBalanceCData(conn, customerDistrictID, customerWarehouseID, c);
//
//        } else {
//            // GoodCredit
//
//            updateBalance(conn, customerDistrictID, customerWarehouseID, c);
//
//        }

        insertHistory(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, w.w_name, d.d_name, c);
    }

    private int getCustomerWarehouseID(Random gen, int w_id, int numWarehouses, int x) {
        int customerWarehouseID;
        if (x <= 85) {
            customerWarehouseID = w_id;
        } else {
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            }
            while (customerWarehouseID == w_id && numWarehouses > 1);
        }
        return customerWarehouseID;
    }

    private int getCustomerDistrictId(Random gen, int districtID, int x) {
        if (x <= 85) {
            return districtID;
        } else {
            return TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
        }


    }

    private void updateWarehouse(FutureConnection conn, int w_id, float paymentAmount) throws SQLException {
        PreparedFutureStatement payUpdateWhse = this.getFuturePreparedStatement(conn, payUpdateWhseSQL);

        payUpdateWhse.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
        payUpdateWhse.setInt(2, w_id);
        // MySQL reports deadlocks due to lock upgrades:
        // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
        FutureResultChain<Integer> result = payUpdateWhse.executeFutureUpdate();

        result.then((operationResult -> {
            if (operationResult == 0) {
                throw new RuntimeException("W_ID=" + w_id + " not found!");
            }
        }));
    }

    private FutureWarehouse getWarehouse(FutureConnection conn, int w_id) throws SQLException {
        PreparedFutureStatement payGetWhse = this.getFuturePreparedStatement(conn, payGetWhseSQL);
        payGetWhse.setInt(1, w_id);
        FutureResultSet rs = payGetWhse.executeFutureQuery();


        rs.ifEmpty(() -> {
            throw new RuntimeException("W_ID=" + w_id + " not found!");
        });

        FutureWarehouse w = new FutureWarehouse();
        w.w_street_1 = rs.getFutureString("W_STREET_1");
        w.w_street_2 = rs.getFutureString("W_STREET_2");
        w.w_city = rs.getFutureString("W_CITY");
        w.w_state = rs.getFutureString("W_STATE");
        w.w_zip = rs.getFutureString("W_ZIP");
        w.w_name = rs.getFutureString("W_NAME");

        return w;
    }

    private FutureCustomer getCustomer(FutureConnection conn, Random gen, int customerDistrictID, int customerWarehouseID, float paymentAmount) throws SQLException {
        int y = TPCCUtil.randomNumber(1, 100, gen);

        FutureCustomer c;

        if (y <= 60) {
            // 60% lookups by last name
            c = getCustomerByName(customerWarehouseID, customerDistrictID, TPCCUtil.getNonUniformRandomLastNameForRun(gen), conn);
        } else {
            // 40% lookups by customer ID
            c = getCustomerById(customerWarehouseID, customerDistrictID, TPCCUtil.getCustomerID(gen), conn);
        }

        var fBalance = c.c_balance;
        c.c_balance = TPCCUtil.newFuture(() -> fBalance.resolve() - paymentAmount);

        var fYtdPayment = c.c_ytd_payment;
        c.c_ytd_payment = TPCCUtil.newFuture(() -> fYtdPayment.resolve() + paymentAmount);

        var fPaymentCnt = c.c_payment_cnt;
        c.c_payment_cnt = TPCCUtil.newFuture(() -> fPaymentCnt.resolve() + 1);

        return c;
    }

    private void updateDistrict(FutureConnection conn, int w_id, int districtID, float paymentAmount) throws SQLException {
        PreparedFutureStatement payUpdateDist = this.getFuturePreparedStatement(conn, payUpdateDistSQL);
        payUpdateDist.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
        payUpdateDist.setInt(2, w_id);
        payUpdateDist.setInt(3, districtID);

        FutureResultChain<Integer> result = payUpdateDist.executeFutureUpdate();

        result.then((operationResult -> {
            if (operationResult == 0) {
                throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
            }
        }));
    }

    private FutureDistrict getDistrict(FutureConnection conn, int w_id, int districtID) throws SQLException {
        PreparedFutureStatement payGetDist = this.getFuturePreparedStatement(conn, payGetDistSQL);
        payGetDist.setInt(1, w_id);
        payGetDist.setInt(2, districtID);

        FutureResultSet rs = payGetDist.executeFutureQuery();
        rs.ifEmpty(() -> {
            throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
        });

        FutureDistrict d = new FutureDistrict();
        d.d_street_1 = rs.getFutureString("D_STREET_1");
        d.d_street_2 = rs.getFutureString("D_STREET_2");
        d.d_city = rs.getFutureString("D_CITY");
        d.d_state = rs.getFutureString("D_STATE");
        d.d_zip = rs.getFutureString("D_ZIP");
        d.d_name = rs.getFutureString("D_NAME");

        return d;
    }

    private Future<String> getCData(FutureConnection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, FutureCustomer c) throws SQLException {
        PreparedFutureStatement payGetCustCdata = this.getFuturePreparedStatement(conn, payGetCustCdataSQL);

        FutureResultSet rs = payGetCustCdata.executeFutureQuery();


        rs.ifEmpty(() -> {
            throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
        });
        Future<String> c_data;
        payGetCustCdata.setInt(1, customerWarehouseID);
        payGetCustCdata.setInt(2, customerDistrictID);
        payGetCustCdata.setFutureInt(3, c.c_id);
        c_data = new Future<>() {
            @Override
            public void dispose() {

            }

            @Override
            public String resolve() {
                var tmp = c.c_id.resolve() + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + rs.getFutureString("C_DATA").resolve();


                if (tmp.length() > 500) {
                    tmp = tmp.substring(0, 500);
                }

                return tmp;
            }
        };

        return c_data;
    }

    private void updateBalanceCData(FutureConnection conn, int customerDistrictID, int customerWarehouseID, FutureCustomer c) throws SQLException {
        PreparedFutureStatement payUpdateCustBalCdata = this.getFuturePreparedStatement(conn, payUpdateCustBalCdataSQL);
        payUpdateCustBalCdata.setFutureFloat(1, c.c_balance);
        payUpdateCustBalCdata.setFutureFloat(2, c.c_ytd_payment);
        payUpdateCustBalCdata.setFutureInt(3, c.c_payment_cnt);
        payUpdateCustBalCdata.setFutureString(4, c.c_data);
        payUpdateCustBalCdata.setInt(5, customerWarehouseID);
        payUpdateCustBalCdata.setInt(6, customerDistrictID);
        payUpdateCustBalCdata.setFutureInt(7, c.c_id);

        FutureResultChain<Integer> result = payUpdateCustBalCdata.executeFutureUpdate();

        result.then((operationResult -> {
            if (operationResult == 0) {
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);
            }
        }));
    }

    private void updateBalance(FutureConnection conn, int customerDistrictID, int customerWarehouseID, FutureCustomer c) throws SQLException {

        PreparedFutureStatement payUpdateCustBal = this.getFuturePreparedStatement(conn, payUpdateCustBalSQL);
        payUpdateCustBal.setFutureFloat(1, c.c_balance);
        payUpdateCustBal.setFutureFloat(2, c.c_ytd_payment);
        payUpdateCustBal.setFutureInt(3, c.c_payment_cnt);
        payUpdateCustBal.setInt(4, customerWarehouseID);
        payUpdateCustBal.setInt(5, customerDistrictID);
        payUpdateCustBal.setFutureInt(6, c.c_id);

        FutureResultChain<Integer> result = payUpdateCustBal.executeFutureUpdate();

        result.then((operationResult -> {
            if (operationResult == 0) {
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            }
        }));
    }

    private void insertHistory(FutureConnection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, Future<String> w_name, Future<String> d_name, FutureCustomer c) throws SQLException {
        var fWName = TPCCUtil.newFuture(() -> {
            var n = w_name.resolve();
            if (n.length() > 10) {
                n = n.substring(0, 10);
            }
            return n;
        });

        var fDName = TPCCUtil.newFuture(() -> {
            var n = d_name.resolve();
            if (n.length() > 10) {
                n = n.substring(0, 10);
            }
            return n;
        });
        Future<String> h_data = TPCCUtil.newFuture(() -> fWName.resolve() + "    " + fDName.resolve());

        PreparedFutureStatement payInsertHist = this.getFuturePreparedStatement(conn, payInsertHistSQL);
        payInsertHist.setInt(1, customerDistrictID);
        payInsertHist.setInt(2, customerWarehouseID);
        payInsertHist.setFutureInt(3, TPCCUtil.newFuture(() -> c.c_id.resolve()));
        payInsertHist.setInt(4, districtID);
        payInsertHist.setInt(5, w_id);
        payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        payInsertHist.setDouble(7, paymentAmount);
        payInsertHist.setFutureString(8, h_data);
        payInsertHist.executeFutureUpdate();
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public FutureCustomer getCustomerById(int c_w_id, int c_d_id, int c_id, FutureConnection conn) throws SQLException {
        var payGetCust = this.getFuturePreparedStatement(conn, payGetCustSQL);

        payGetCust.setInt(1, c_w_id);
        payGetCust.setInt(2, c_d_id);
        payGetCust.setInt(3, c_id);

        FutureResultSet rs = payGetCust.executeFutureQuery();

        rs.ifEmpty(() -> {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        });

        FutureCustomer c = TPCCUtil.newFutureCustomerFromResults(rs);
        c.c_id = TPCCUtil.newFuture(() -> c_id);
        c.c_last = rs.getFutureString("C_LAST");

        return c;
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public FutureCustomer getCustomerByName(int c_w_id, int c_d_id, String customerLastName, FutureConnection conn) throws SQLException {
        PreparedFutureStatement customerByName = this.getFuturePreparedStatement(conn, customerByNameSQL);

        customerByName.setInt(1, c_w_id);
        customerByName.setInt(2, c_d_id);
        customerByName.setString(3, customerLastName);

        FutureResultSet rs = customerByName.executeFutureQuery();

        rs.then(operationResultSet -> {
            try {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("C_LAST={} C_D_ID={} C_W_ID={}", customerLastName, c_d_id, c_w_id);
                }

                if (operationResultSet.isClosed()) {
                    throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                }

                // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
                // that
                // counts starting from 1.
                operationResultSet.last();
                int rowCount = operationResultSet.getRow();
                operationResultSet.beforeFirst();
                int index = rowCount / 2;
                if (rowCount % 2 == 0) {
                    index -= 1;
                }

                int i = 0;
                //noinspection StatementWithEmptyBody
                while (operationResultSet.next() && i++ < index) ;
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });

        FutureCustomer customer = TPCCUtil.newFutureCustomerFromResults(rs);
        customer.c_id = rs.getFutureInt("C_ID");
//        customer.c_id =  TPCCUtil.newFuture( () -> c.c_id.resolve());
//        customer.c_first =  TPCCUtil.newFuture( () -> c.c_first.resolve());
//        customer.c_middle =  TPCCUtil.newFuture( () -> c.c_middle.resolve());
//        customer.c_street_1 =  TPCCUtil.newFuture( () -> c.c_street_1.resolve());
//        customer.c_street_2 =  TPCCUtil.newFuture( () -> c.c_street_2.resolve());
//        customer.c_city =  TPCCUtil.newFuture( () -> c.c_city.resolve());
//        customer.c_state =  TPCCUtil.newFuture( () -> c.c_state.resolve());
//        customer.c_zip =  TPCCUtil.newFuture( () -> c.c_zip.resolve());
//        customer.c_phone =  TPCCUtil.newFuture( () -> c.c_phone.resolve());
//        customer.c_credit =  TPCCUtil.newFuture( () -> c.c_credit.resolve());
//        customer.c_credit_lim =  TPCCUtil.newFuture( () -> c.c_credit_lim.resolve());
//        customer.c_discount =  TPCCUtil.newFuture( () -> c.c_discount.resolve());
//        customer.c_balance =  TPCCUtil.newFuture( () -> c.c_balance.resolve());
//        customer.c_ytd_payment =  TPCCUtil.newFuture( () -> c.c_ytd_payment.resolve());
//        customer.c_payment_cnt =  TPCCUtil.newFuture( () -> c.c_payment_cnt.resolve());
//        customer.c_since =  TPCCUtil.newFuture( () -> c.c_since.resolve());

        return customer;
    }



}
