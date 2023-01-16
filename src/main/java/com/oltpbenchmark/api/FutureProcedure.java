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

package com.oltpbenchmark.api;

import com.oltpbenchmark.types.DatabaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import trxsys.lsd.api.FutureConnection;
import trxsys.lsd.api.PreparedFutureStatement;
import trxsys.lsd.future.Future;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class FutureProcedure extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(FutureProcedure.class);

    private final String procName;
    private DatabaseType dbType;

    /**
     * Constructor
     */
    protected FutureProcedure() {
        this.procName = this.getClass().getSimpleName();
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     * This will automatically call setObject for all the parameters you pass in
     *
     * @param conn
     * @param stmt
     * @param params
     * @return
     * @throws SQLException
     */
    public final PreparedFutureStatement getFuturePreparedStatement(FutureConnection conn, SQLStmt stmt, Object... params) throws SQLException {
        PreparedFutureStatement pStmt = this.getFuturePreparedStatementReturnKeys(conn, stmt);
        for (int i = 0; i < params.length; i++) {
            if (params[i] instanceof Future<?>) {
                pStmt.setFutureObject(i + 1, (Future<Object>) params[i]);
            }
            pStmt.setObject(i + 1, params[i]);
        }
        return (pStmt);
    }

    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     *
     * @param conn
     * @param stmt
     * @return
     * @throws SQLException
     */
    public final PreparedFutureStatement getFuturePreparedStatementReturnKeys(FutureConnection conn, SQLStmt stmt) throws SQLException {
        if (stmt.getResultSet() != null) {
            return conn.prepareFutureStatement(stmt.getSQL(), stmt.getResultSet(), ResultSet.CONCUR_READ_ONLY);
        }
        return conn.prepareFutureStatement(stmt.getSQL());
    }
}
