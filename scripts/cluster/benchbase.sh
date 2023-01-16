#!/usr/bin/bash

JAVA_OPTIONS="-XX:+UseG1GC -Xms4g -Xmx4g -Djava.util.logging.config.file=logging.properties"

function build_db() {
    local benchbase_dir
    local config_file
    local benchmark

    benchbase_dir=$1
    config_file=$2
    benchmark=$3

    pushd "$benchbase_dir"
        
        java $JAVA_OPTIONS -jar "benchbase.jar" -c "$config_file" --create=true --load=true --execute=false -b "$benchmark"

    popd
}

function benchmark_db() {
    local benchbase_dir
    local config_file
    local benchmark
    local save_config_dir

    benchbase_dir=$1
    config_file=$2
    benchmark=$3
    save_config_dir=$4
    
    pushd "$benchbase_dir"
        
        java $JAVA_OPTIONS -jar "benchbase.jar" -c "$config_file" -d "$save_config_dir" --create=false --load=false --execute=true -b "$benchmark"

    popd

}
