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
import java.sql.Statement;

public class Q15 extends GenericQuery {

    public final SQLStmt createview_stmt = new SQLStmt("CREATE view revenue0 (supplier_no, total_revenue) AS\n" +
            "            SELECT\n" +
            "               l_suppkey,\n" +
            "               SUM(l_extendedprice * (1 - l_discount))\n" +
            "            FROM\n" +
            "               lineitem\n" +
            "            WHERE\n" +
            "               l_shipdate >= DATE ?\n" +
            "               AND l_shipdate < DATE ? + INTERVAL '3' MONTH\n" +
            "            GROUP BY\n" +
            "               l_suppkey"
    );

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               s_suppkey,\n" +
            "               s_name,\n" +
            "               s_address,\n" +
            "               s_phone,\n" +
            "               total_revenue\n" +
            "            FROM\n" +
            "               supplier,\n" +
            "               revenue0\n" +
            "            WHERE\n" +
            "               s_suppkey = supplier_no\n" +
            "               AND total_revenue = (\n" +
            "                  SELECT\n" +
            "                     MAX(total_revenue)\n" +
            "                  FROM\n" +
            "                     revenue0\n" +
            "               )\n" +
            "            ORDER BY\n" +
            "               s_suppkey"
    );

    public final SQLStmt dropview_stmt = new SQLStmt("DROP VIEW revenue0"
    );

    @Override
    public void run(Connection conn, RandomGenerator rand) throws SQLException {
        // With this query, we have to set up a view before we execute the
        // query, then drop it once we're done.
        try (Statement stmt = conn.createStatement()) {
            try {
                // DATE is the first day of a randomly selected month between
                // the first month of 1993 and the 10th month of 1997
                int year = rand.number(1993, 1997);
                int month = rand.number(1, year == 1997 ? 10 : 12);
                String date = String.format("%d-%02d-01", year, month);

                String sql = createview_stmt.getSQL();
                sql = sql.replace("?", String.format("'%s'", date));
                stmt.execute(sql);
                super.run(conn, rand);
            } finally {
                String sql = dropview_stmt.getSQL();
                stmt.execute(sql);
            }
        }

    }

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        return this.getPreparedStatement(conn, query_stmt);
    }
}
