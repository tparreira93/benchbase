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

public class Q21 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               s_name,\n" +
            "               COUNT(*) AS numwait\n" +
            "            FROM\n" +
            "               supplier,\n" +
            "               lineitem l1,\n" +
            "               orders,\n" +
            "               nation\n" +
            "            WHERE\n" +
            "               s_suppkey = l1.l_suppkey\n" +
            "               AND o_orderkey = l1.l_orderkey\n" +
            "               AND o_orderstatus = 'F'\n" +
            "               AND l1.l_receiptdate > l1.l_commitdate\n" +
            "               AND EXISTS\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     *\n" +
            "                  FROM\n" +
            "                     lineitem l2\n" +
            "                  WHERE\n" +
            "                     l2.l_orderkey = l1.l_orderkey\n" +
            "                     AND l2.l_suppkey <> l1.l_suppkey\n" +
            "               )\n" +
            "               AND NOT EXISTS\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     *\n" +
            "                  FROM\n" +
            "                     lineitem l3\n" +
            "                  WHERE\n" +
            "                     l3.l_orderkey = l1.l_orderkey\n" +
            "                     AND l3.l_suppkey <> l1.l_suppkey\n" +
            "                     AND l3.l_receiptdate > l3.l_commitdate\n" +
            "               )\n" +
            "               AND s_nationkey = n_nationkey\n" +
            "               AND n_name = ?\n" +
            "            GROUP BY\n" +
            "               s_name\n" +
            "            ORDER BY\n" +
            "               numwait DESC,\n" +
            "               s_name LIMIT 100"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation);
        return stmt;
    }
}
