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


package com.oltpbenchmark.benchmarks.lsd.tpcc.pojo;

import lsd.v2.api.Future;

import java.io.Serializable;

public class FutureStock implements Serializable {

    public Future<Integer> s_i_id; // PRIMARY KEY 2
    public Future<Integer> s_w_id; // PRIMARY KEY 1
    public Future<Integer> s_order_cnt;
    public Future<Integer> s_remote_cnt;
    public Future<Integer> s_quantity;
    public Future<Float> s_ytd;
    public Future<String> s_data;
    public Future<String> s_dist_01;
    public Future<String> s_dist_02;
    public Future<String> s_dist_03;
    public Future<String> s_dist_04;
    public Future<String> s_dist_05;
    public Future<String> s_dist_06;
    public Future<String> s_dist_07;
    public Future<String> s_dist_08;
    public Future<String> s_dist_09;
    public Future<String> s_dist_10;
}
