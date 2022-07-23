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

package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.TPCHConstants;
import com.oltpbenchmark.benchmarks.tpch.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q7 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               supp_nation,\n" +
            "               cust_nation,\n" +
            "               l_year,\n" +
            "               SUM(volume) AS revenue\n" +
            "            FROM\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     n1.n_name AS supp_nation,\n" +
            "                     n2.n_name AS cust_nation,\n" +
            "                     EXTRACT(YEAR\n" +
            "                  FROM\n" +
            "                     l_shipdate) AS l_year,\n" +
            "                     l_extendedprice * (1 - l_discount) AS volume\n" +
            "                  FROM\n" +
            "                     supplier,\n" +
            "                     lineitem,\n" +
            "                     orders,\n" +
            "                     customer,\n" +
            "                     nation n1,\n" +
            "                     nation n2\n" +
            "                  WHERE\n" +
            "                     s_suppkey = l_suppkey\n" +
            "                     AND o_orderkey = l_orderkey\n" +
            "                     AND c_custkey = o_custkey\n" +
            "                     AND s_nationkey = n1.n_nationkey\n" +
            "                     AND c_nationkey = n2.n_nationkey\n" +
            "                     AND\n" +
            "                     (\n" +
            "            (n1.n_name = ?\n" +
            "                        AND n2.n_name = ? )\n" +
            "                        OR\n" +
            "                        (\n" +
            "                           n1.n_name = ?\n" +
            "                           AND n2.n_name = ?\n" +
            "                        )\n" +
            "                     )\n" +
            "                     AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'\n" +
            "               )\n" +
            "               AS shipping\n" +
            "            GROUP BY\n" +
            "               supp_nation,\n" +
            "               cust_nation,\n" +
            "               l_year\n" +
            "            ORDER BY\n" +
            "               supp_nation,\n" +
            "               cust_nation,\n" +
            "               l_year"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // NATION1 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation1 = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // NATION2 is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        // and must be different from the value selected for NATION1 in item 1 above
        String nation2 = nation1;
        while (nation2.equals(nation1)) {
            nation2 = TPCHUtil.choice(TPCHConstants.N_NAME, rand);
        }

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation1);
        stmt.setString(2, nation2);
        stmt.setString(3, nation2);
        stmt.setString(4, nation1);
        return stmt;
    }
}
