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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q20 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               s_name,\n" +
            "               s_address\n" +
            "            FROM\n" +
            "               supplier,\n" +
            "               nation\n" +
            "            WHERE\n" +
            "               s_suppkey IN\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     ps_suppkey\n" +
            "                  FROM\n" +
            "                     partsupp\n" +
            "                  WHERE\n" +
            "                     ps_partkey IN\n" +
            "                     (\n" +
            "                        SELECT\n" +
            "                           p_partkey\n" +
            "                        FROM\n" +
            "                           part\n" +
            "                        WHERE\n" +
            "                           p_name LIKE ?\n" +
            "                     )\n" +
            "                     AND ps_availqty > (\n" +
            "                     SELECT\n" +
            "                        0.5 * SUM(l_quantity)\n" +
            "                     FROM\n" +
            "                        lineitem\n" +
            "                     WHERE\n" +
            "                        l_partkey = ps_partkey\n" +
            "                        AND l_suppkey = ps_suppkey\n" +
            "                        AND l_shipdate >= DATE ?\n" +
            "                        AND l_shipdate < DATE ? + INTERVAL '1' YEAR )\n" +
            "               )\n" +
            "               AND s_nationkey = n_nationkey\n" +
            "               AND n_name = ?\n" +
            "            ORDER BY\n" +
            "               s_name"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // COLOR is randomly selected within the list of values defined for the generation of P_NAME
        String color = TPCHUtil.choice(TPCHConstants.P_NAME_GENERATOR, rand) + "%";

        // DATE is the first of January of a randomly selected year within 1993..1997
        int year = rand.number(1993, 1997);
        String date = String.format("%d-01-01", year);

        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, color);
        stmt.setDate(2, Date.valueOf(date));
        stmt.setDate(3, Date.valueOf(date));
        stmt.setString(4, nation);
        return stmt;
    }
}
