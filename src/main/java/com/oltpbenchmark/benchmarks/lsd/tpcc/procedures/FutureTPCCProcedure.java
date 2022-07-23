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

package com.oltpbenchmark.benchmarks.lsd.tpcc.procedures;

import com.oltpbenchmark.api.FutureProcedure;
import com.oltpbenchmark.benchmarks.lsd.tpcc.TPCCWorker;
import lsd.v2.api.FutureConnection;

import java.sql.SQLException;
import java.util.Random;

public abstract class FutureTPCCProcedure extends FutureProcedure {

    public abstract void run(FutureConnection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCWorker w) throws SQLException;

}
