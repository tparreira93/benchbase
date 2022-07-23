#!/usr/bin/bash

user="tparreira"
host=$(hostname)

JAVA_OPTIONS="-Xms1024m -Xmx1024m"

source "/home/jlourenco/tparreira/sync_nodes.sh"

function build_db() {
    local config
    local benchmark
    local save_dir

    type=$1
    config=$2
    benchmark=$3

    random_place=$(cat /proc/sys/kernel/random/uuid)
    save_dir="/tmp/$user/$random_place"

    pushd "/home/jlourenco/$user/benchbase-postgres"

        java $JAVA_OPTIONS -jar benchbase.jar -c "tpcc/$type/$config.xml" -d "$save_dir/$type/$config" --create=true --load=true --execute=false -b "$benchmark"

    popd
}

# test type - lsd or base
# config - config file
# benchmark - tpcc or tpcc_lsd
# save location
function benchmark_db() {
    local config
    local benchmark
    local config_dir
    local save_dir

    type=$1
    config=$2
    benchmark=$3
    save_to=$4
    
    random_place=$(cat /proc/sys/kernel/random/uuid)
    config_dir="/home/jlourenco/$user/benchbase-postgres/tpcc"

    [ ! -d "$save_to" ] && mkdir -p "$save_to"

    save_config_dir="/tmp/$user/$random_place/$type/$config"
    [ ! -d "$save_config_dir" ] && mkdir -p "$save_config_dir"
    
    pushd "/home/jlourenco/$user/benchbase-postgres"
        
        java $JAVA_OPTIONS -jar benchbase.jar -c "$config_dir/$type/$config.xml" -d "$save_config_dir" --create=false --load=false --execute=true -b "$benchmark"

    popd
    
    cp -r "$save_config_dir/" "$save_to/"
}
