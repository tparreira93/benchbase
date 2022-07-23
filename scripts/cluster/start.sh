#!/usr/bin/bash

user="tparreira"
pgsql_bin="/home/jlourenco/$user/pgsql/bin"
conf="/home/jlourenco/$user/conf"
database_cluster_dir="/tmp/$user/psql/databases"

export PGDATA=$database_cluster_dir

function start_cluster() {
    # erase any previous directories and server
    $pgsql_bin/pg_ctl stop
    rm -rf /tmp/$user

    # create database directory
    set -e
    mkdir -p $database_cluster_dir

    # create database cluster
    $pgsql_bin/initdb
    cp /home/jlourenco/tparreira/conf "$database_cluster_dir/"
    $pgsql_bin/pg_ctl start

    # create benchmark user and database
    $pgsql_bin/createuser -lds benchmarksql
    $pgsql_bin/createuser -lds admin
    $pgsql_bin/createuser -lds benchbase
    $pgsql_bin/createdb -O admin benchbase_base_1w_10t
    $pgsql_bin/createdb -O admin benchbase_base_10w_1t
    $pgsql_bin/createdb -O admin benchbase_base_10w_10t
    $pgsql_bin/createdb -O admin benchbase_lsd_1w_10t
    $pgsql_bin/createdb -O admin benchbase_lsd_10w_1t
    $pgsql_bin/createdb -O admin benchbase_lsd_10w_10t
    $pgsql_bin/createdb -O benchmarksql benchmarksql_lsd
    $pgsql_bin/createdb -O benchmarksql benchmarksql_psql

    set +e
}