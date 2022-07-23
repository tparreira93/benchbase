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
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q18 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               c_name,\n" +
            "               c_custkey,\n" +
            "               o_orderkey,\n" +
            "               o_orderdate,\n" +
            "               o_totalprice,\n" +
            "               SUM(l_quantity)\n" +
            "            FROM\n" +
            "               customer,\n" +
            "               orders,\n" +
            "               lineitem\n" +
            "            WHERE\n" +
            "               o_orderkey IN\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     l_orderkey\n" +
            "                  FROM\n" +
            "                     lineitem\n" +
            "                  GROUP BY\n" +
            "                     l_orderkey\n" +
            "                  HAVING\n" +
            "                     SUM(l_quantity) > ?\n" +
            "               )\n" +
            "               AND c_custkey = o_custkey\n" +
            "               AND o_orderkey = l_orderkey\n" +
            "            GROUP BY\n" +
            "               c_name,\n" +
            "               c_custkey,\n" +
            "               o_orderkey,\n" +
            "               o_orderdate,\n" +
            "               o_totalprice\n" +
            "            ORDER BY\n" +
            "               o_totalprice DESC,\n" +
            "               o_orderdate LIMIT 100"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // QUANTITY is randomly selected within [312..315]
        int quantity = rand.number(312, 315);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setInt(1, quantity);
        return stmt;
    }
}
