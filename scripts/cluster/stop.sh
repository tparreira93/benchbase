#!/usr/bin/bash

user="tparreira"
pgsql_bin="/home/jlourenco/$user/pgsql/bin"
database_cluster_dir="/tmp/$user/psql/databases"

export PGDATA=$database_cluster_dir

# erase any previous directories and server
$pgsql_bin/pg_ctl stop
rm -rf /tmp/$user
