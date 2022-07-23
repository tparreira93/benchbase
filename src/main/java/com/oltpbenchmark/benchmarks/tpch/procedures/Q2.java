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

public class Q2 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "                s_acctbal,\n" +
            "                s_name,\n" +
            "                n_name,\n" +
            "                p_partkey,\n" +
            "                p_mfgr,\n" +
            "                s_address,\n" +
            "                s_phone,\n" +
            "                s_comment\n" +
            "             FROM\n" +
            "                part,\n" +
            "                supplier,\n" +
            "                partsupp,\n" +
            "                nation,\n" +
            "                region\n" +
            "             WHERE\n" +
            "                p_partkey = ps_partkey\n" +
            "                AND s_suppkey = ps_suppkey\n" +
            "                AND p_size = ?\n" +
            "                AND p_type LIKE ?\n" +
            "                AND s_nationkey = n_nationkey\n" +
            "                AND n_regionkey = r_regionkey\n" +
            "                AND r_name = ?\n" +
            "                AND ps_supplycost =\n" +
            "                (\n" +
            "                   SELECT\n" +
            "                      MIN(ps_supplycost)\n" +
            "                   FROM\n" +
            "                      partsupp,\n" +
            "                      supplier,\n" +
            "                      nation,\n" +
            "                      region\n" +
            "                   WHERE\n" +
            "                      p_partkey = ps_partkey\n" +
            "                      AND s_suppkey = ps_suppkey\n" +
            "                      AND s_nationkey = n_nationkey\n" +
            "                      AND n_regionkey = r_regionkey\n" +
            "                      AND r_name = ?\n" +
            "                )\n" +
            "             ORDER BY\n" +
            "                s_acctbal DESC,\n" +
            "                n_name,\n" +
            "                s_name,\n" +
            "                p_partkey LIMIT 100"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        int size = rand.number(1, 50);
        String type = TPCHUtil.choice(TPCHConstants.TYPE_S3, rand);
        String region = TPCHUtil.choice(TPCHConstants.R_NAME, rand);

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        stmt.setInt(1, size);
        stmt.setString(2, "%" + type);
        stmt.setString(3, region);
        stmt.setString(4, region);
        return stmt;
    }
}
