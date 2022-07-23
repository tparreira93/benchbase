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

public class Q8 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               o_year,\n" +
            "               SUM(\n" +
            "               CASE\n" +
            "                  WHEN\n" +
            "                     nation = ?\n" +
            "                  THEN\n" +
            "                     volume\n" +
            "                  ELSE\n" +
            "                     0\n" +
            "               END\n" +
            "            ) / SUM(volume) AS mkt_share\n" +
            "            FROM\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     EXTRACT(YEAR\n" +
            "                  FROM\n" +
            "                     o_orderdate) AS o_year,\n" +
            "                     l_extendedprice * (1 - l_discount) AS volume,\n" +
            "                     n2.n_name AS nation\n" +
            "                  FROM\n" +
            "                     part,\n" +
            "                     supplier,\n" +
            "                     lineitem,\n" +
            "                     orders,\n" +
            "                     customer,\n" +
            "                     nation n1,\n" +
            "                     nation n2,\n" +
            "                     region\n" +
            "                  WHERE\n" +
            "                     p_partkey = l_partkey\n" +
            "                     AND s_suppkey = l_suppkey\n" +
            "                     AND l_orderkey = o_orderkey\n" +
            "                     AND o_custkey = c_custkey\n" +
            "                     AND c_nationkey = n1.n_nationkey\n" +
            "                     AND n1.n_regionkey = r_regionkey\n" +
            "                     AND r_name = ?\n" +
            "                     AND s_nationkey = n2.n_nationkey\n" +
            "                     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'\n" +
            "                     AND p_type = ?\n" +
            "               )\n" +
            "               AS all_nations\n" +
            "            GROUP BY\n" +
            "               o_year\n" +
            "            ORDER BY\n" +
            "               o_year"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // NATION is randomly selected within the list of values defined for N_NAME in Clause 4.2.3
        String nation = TPCHUtil.choice(TPCHConstants.N_NAME, rand);

        // REGION is the value defined in Clause 4.2.3 for R_NAME where R_REGIONKEY corresponds to
        // N_REGIONKEY for the selected NATION in item 1 above
        int n_regionkey = TPCHUtil.getRegionKeyFromNation(nation);
        String region = TPCHUtil.getRegionFromRegionKey(n_regionkey);

        // TYPE is randomly selected within the list of 3-syllable strings defined for Types in Clause 4.2.2.13
        String syllable1 = TPCHUtil.choice(TPCHConstants.TYPE_S1, rand);
        String syllable2 = TPCHUtil.choice(TPCHConstants.TYPE_S2, rand);
        String syllable3 = TPCHUtil.choice(TPCHConstants.TYPE_S3, rand);
        String type = String.format("%s %s %s", syllable1, syllable2, syllable3);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setString(1, nation);
        stmt.setString(2, region);
        stmt.setString(3, type);
        return stmt;
    }
}
