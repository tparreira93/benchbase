#!/usr/bin/bash

wokers_count=$1
tests=$2
database=$3

host=$(hostname)
user="tparreira"
run_file="/home/jlourenco/tparreira/current_run"
config_dir="/home/jlourenco/$user/benchbase-postgres/tpcc"
run=$(cat $run_file)
run=$(expr $run + 1);
run_dir="$config_dir/run-$run"
worker_tests_completed="$run_dir/completions"
load_ready_file="$run_dir/load-ready"

source "/home/jlourenco/tparreira/start.sh"
source "/home/jlourenco/tparreira/sync_nodes.sh"
source "/home/jlourenco/tparreira/benchbase.sh"

[ ! -d "$worker_tests_completed" ] && mkdir -p "$worker_tests_completed"
[ ! -d "$run_dir" ] && mkdir -p "$run_dir"

echo "Starting Run $run"

function build_test_databases() {
    local test_file

    test_file=$1
    
    while read line; 
    do 
        stringarray=($line)
        type=${stringarray[0]}
        config=${stringarray[1]}
        benchmark=${stringarray[2]}

        echo "Loading database - Type: $type - Config: $config - Benchmark: $benchmark"

        build_db $type $config $benchmark $run_host_dir
    done < "$test_file"
}

function run_benchmarks() {
    local run_config_dir
    local run_host_dir
    local test_file

    test_file=$1

    while read line; 
    do 
        stringarray=($line)
        type=${stringarray[0]}
        config=${stringarray[1]}
        benchmark=${stringarray[2]}

        echo "Running test - Type: $type - Config: $config - Benchmark: $benchmark"

        run_config_dir="$run_dir/$type/$config"
        run_host_dir="$run_config_dir/$host"
        
        [ ! -d "$run_host_dir" ] && mkdir -p "$run_host_dir"

        set_worker_ready "$host" "$run_config_dir"
        wait_all_workers "$run_config_dir" "$wokers_count"

        benchmark_db $type $config $benchmark $run_host_dir

    done < "$test_file"
}

if [ "$host" = "$database" ]; then
    echo "Starting database..."
    start_cluster
    echo "The server has started!"

    echo "Build databases..."
    build_test_databases "$tests"
    echo "Databases were built!"

    echo "Setting load as completed..."
    set_worker_ready "load-$host" "$run_dir"
    echo $run > $run_file
    echo "Run file updated and load completed!"
else
    echo "Waiting for load to be completed..."
    wait_all_workers "$run_dir" 1

    echo "Starting benchmarks..."
    run_benchmarks "$tests"
    echo "Benchmarks completed!"

    set_worker_ready "$host" "$worker_tests_completed"
fi


if [ "$host" = "$database" ]; then
    wait_all_workers "$worker_tests_completed" "$wokers_count"

    ./stop.sh

    ./plot_data.py "${run_dir}"
fi