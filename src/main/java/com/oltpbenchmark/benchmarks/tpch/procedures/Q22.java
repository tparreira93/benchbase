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
import java.util.HashSet;
import java.util.Set;

public class Q22 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("SELECT\n" +
            "               cntrycode,\n" +
            "               COUNT(*) AS numcust,\n" +
            "               SUM(c_acctbal) AS totacctbal\n" +
            "            FROM\n" +
            "               (\n" +
            "                  SELECT\n" +
            "                     SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,\n" +
            "                     c_acctbal\n" +
            "                  FROM\n" +
            "                     customer\n" +
            "                  WHERE\n" +
            "                     SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)\n" +
            "                     AND c_acctbal > \n" +
            "                     (\n" +
            "                         SELECT\n" +
            "                            AVG(c_acctbal)\n" +
            "                         FROM\n" +
            "                            customer\n" +
            "                         WHERE\n" +
            "                            c_acctbal > 0.00\n" +
            "                            AND SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)\n" +
            "                     )\n" +
            "                     AND NOT EXISTS\n" +
            "                     (\n" +
            "                         SELECT\n" +
            "                            *\n" +
            "                         FROM\n" +
            "                            orders\n" +
            "                         WHERE\n" +
            "                            o_custkey = c_custkey\n" +
            "                     )\n" +
            "               )\n" +
            "               AS custsale\n" +
            "            GROUP BY\n" +
            "               cntrycode\n" +
            "            ORDER BY\n" +
            "               cntrycode"
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand) throws SQLException {
        // I1 - I7 are randomly selected without repetition from the possible values


        // We are given
        //      Let i be an index into the list of strings Nations
        //          (i.e., ALGERIA is 0, ARGENTINA is 1, etc., see Clause 4.2.3),
        //      Let country_code be the sub-string representation of the number (i + 10)
        // There are 25 nations, hence country_code ranges from [10, 34]

        Set<Integer> seen = new HashSet<>(7);
        int[] codes = new int[7];
        for (int i = 0; i < 7; i++) {
            int num = rand.number(10, 34);

            while (seen.contains(num)) {
                num = rand.number(10, 34);
            }

            codes[i] = num;
            seen.add(num);
        }

        PreparedStatement stmt = this.getPreparedStatement(conn, query_stmt);
        for (int i = 0; i < 7; i++) {
            stmt.setString(1 + i, String.valueOf(codes[i]));
        }
        for (int i = 0; i < 7; i++) {
            stmt.setString(8 + i, String.valueOf(codes[i]));
        }
        return stmt;
    }
}
