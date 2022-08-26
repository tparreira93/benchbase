#!/usr/bin/bash +x

worker_count=$1
test_file=$2
database=$3

host=$(hostname)
user="tparreira"
run_file="/home/jlourenco/tparreira/current_run"
config_dir="/home/jlourenco/$user/benchbase-postgres/tpcc"
run=$(cat $run_file)
run=$(expr $run + 1);
run_dir="$config_dir/run-$run"
load_ready_file="$run_dir/load-ready"

source "/home/jlourenco/tparreira/start.sh"
source "/home/jlourenco/tparreira/sync_nodes.sh"
source "/home/jlourenco/tparreira/benchbase.sh"

[ ! -d "$run_dir" ] && mkdir -p "$run_dir"

echo "Starting run $run with $worker_count workers, ${test_file} test file and ${database} database..."

function run_benchmark() {
    local run_config_dir
    local run_host_dir

    local type
    local config
    local benchmark

    type=$1
    config=$2
    benchmark=$3

    echo "Running test - Type: $type - Config: $config - Benchmark: $benchmark"

    run_config_dir="$run_dir/$type/$config"
    run_worker_ready_dir="$run_config_dir/running"
    run_host_dir="$run_config_dir/$host"

    [ ! -d "$run_host_dir" ] && mkdir -p "$run_host_dir"
    [ ! -d "$run_worker_ready_dir" ] && mkdir -p "$run_worker_ready_dir"

    set_worker_ready "$host" "$run_worker_ready_dir"
    wait_all_workers "$run_worker_ready_dir" "$worker_count"

    benchmark_db $type $config $benchmark $run_host_dir
}


while read line;
do
    stringarray=($line)
    type=${stringarray[0]}
    config=${stringarray[1]}
    benchmark=${stringarray[2]}

    run_config_dir="$run_dir/$type/$config"
    load_completion="$run_config_dir/completions/load"
    worker_tests_completed="$run_config_dir/completions/workers"
    run_worker_ready_dir="$run_config_dir/running"
    [ ! -d "$run_config_dir" ] && mkdir -p "$run_config_dir"
    [ ! -d "$load_completion" ] && mkdir -p "$load_completion"
    [ ! -d "$worker_tests_completed" ] && mkdir -p "$worker_tests_completed"
    [ ! -d "$run_worker_ready_dir" ] && mkdir -p "$run_worker_ready_dir"

    if [ "$host" = "$database" ]; then
        echo "Loading database - Type: $type - Config: $config - Benchmark: $benchmark"
        echo "Starting database..."
        start_cluster
        echo "The server has started!"

        echo "Build databases..."
        build_db $type $config $benchmark
        echo "Database was built!"

        echo "Setting load as completed..."
        set_worker_ready "$database" "$load_completion"
        echo $run > $run_file
        echo "Load completed!"

    else
        echo "Waiting for load to be completed..."
        wait_worker "$database" "$load_completion"

        echo "Starting benchmarks..."
        run_benchmark "$type" "$config" "$benchmark"
        echo "Benchmarks completed!"

        set_worker_ready "$host" "$worker_tests_completed"
    fi

    if [ "$host" = "$database" ]; then
        wait_all_workers "$worker_tests_completed" "$worker_count"

        ./stop.sh

        ./plot_data.py -d "${run_dir}"

        echo $run > $run_file
        echo "Run file updated!"
    fi

done < "$test_file"