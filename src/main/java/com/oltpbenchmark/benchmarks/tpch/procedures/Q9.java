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

public class Q9 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               nation,\n" +
            "               o_year,\n" +
            "               SUM(amount) AS sum_profit\n" +
            "            FROM\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     n_name AS nation,\n" +
            "                     EXTRACT(YEAR\n" +
            "                  FROM\n" +
            "                     o_orderdate) AS o_year,\n" +
            "                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount\n" +
            "                  FROM\n" +
            "                     part,\n" +
            "                     supplier,\n" +
            "                     lineitem,\n" +
            "                     partsupp,\n" +
            "                     orders,\n" +
            "                     nation\n" +
            "                  WHERE\n" +
            "                     s_suppkey = l_suppkey\n" +
            "                     AND ps_suppkey = l_suppkey\n" +
            "                     AND ps_partkey = l_partkey\n" +
            "                     AND p_partkey = l_partkey\n" +
            "                     AND o_orderkey = l_orderkey\n" +
            "                     AND s_nationkey = n_nationkey\n" +
            "                     AND p_name LIKE ?\n" +
            "               )\n" +
            "               AS profit\n" +
            "            GROUP BY\n" +
            "               nation,\n" +
            "               o_year\n" +
            "            ORDER BY\n" +
            "               nation,\n" +
            "               o_year DESC"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // COLOR is randomly selected within the list of values defined for the generation of P_NAME in Clause 4.2.3
        String color = "%" + TPCHUtil.choice(TPCHConstants.P_NAME_GENERATOR, rand) + "%";

        LOG.debug("attempting to execute sql [{}] for color [{}]", query_stmt.getSQL(), color);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, color);
        return stmt;
    }
}
