#!/usr/bin/env bash
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dlsd_s10_t1.xml -d tpcc_dlsd_s10_t1 --create=true --load=true --execute=true -b tpcc_lsd
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dlsd_s10_t10.xml -d tpcc_dlsd_s10_t10 --create=true --load=true --execute=true -b tpcc_lsd
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dlsd_s1_t1.xml -d tpcc_dlsd_s1_t1 --create=true --load=true --execute=true -b tpcc_lsd
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dlsd_s1_t10.xml -d tpcc_dlsd_s1_t10 --create=true --load=true --execute=true -b tpcc_lsd
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dbase_s10_t1.xml -d tpcc_dbase_s10_t1 --create=true --load=true --execute=true -b tpcc
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dbase_s10_t10.xml -d tpcc_dbase_s10_t10 --create=true --load=true --execute=true -b tpcc
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dbase_s1_t1.xml -d tpcc_dbase_s1_t1 --create=true --load=true --execute=true -b tpcc
java -jar benchbase.jar -c C:\Workspace\thesis\benchmark\benchbase\scripts\tpcc_dbase_s1_t10.xml -d tpcc_dbase_s1_t10 --create=true --load=true --execute=true -b tpcc
python plot_data.py tpcc_dlsd_s10_t1 tpcc_dbase_s10_t1
python plot_data.py tpcc_dlsd_s10_t10 tpcc_dbase_s10_t10
python plot_data.py tpcc_dlsd_s1_t1 tpcc_dbase_s1_t1
python plot_data.py tpcc_dlsd_s1_t10 tpcc_dbase_s1_t10
