#!/usr/bin/bash +x

worker_count=$1
test_file=$2
database=$3
run=$4
bench_type="tpcc"

host=$(hostname)
user="tparreira"
benchbase_dir="/home/jlourenco/$user/benchbase-postgres"
config_dir="/home/jlourenco/$user/benchbase-postgres/$bench_type"
run_dir="$config_dir/run-$run"
load_ready_file="$run_dir/load-ready"

source "/home/jlourenco/tparreira/start.sh"
source "/home/jlourenco/tparreira/sync_nodes.sh"
source "/home/jlourenco/tparreira/benchbase.sh"

[ ! -d "$run_dir" ] && mkdir -p "$run_dir"

random_place=$(cat /proc/sys/kernel/random/uuid)
save_dir="/tmp/$user/$random_place"

function progressMessage() {
    local message

    message=$1

    echo "==================================================================================================================================="
    echo "$message"
    echo "==================================================================================================================================="
}

function run_benchmark() {
    local run_config_dir
    local run_host_dir
    local save_config_dir

    local type
    local config
    local benchmark
    local database

    type=$1
    config=$2
    benchmark=$3
    database=$4

    config_file="$config_dir/$type/$config.xml"
    interpolated_config_dir="$save_dir/$bench_type/$type"
    interpolated_config="$interpolated_config_dir/$config.xml"
    save_config_dir="$save_dir/$bench_type/$type/$config"

    [ ! -d "$save_config_dir" ] && mkdir -p "$save_config_dir"
    [ ! -d "$interpolated_config_dir" ] && mkdir -p "$interpolated_config_dir"

    sed "s/databaseUrl/$database/g" "$config_file" > "$interpolated_config"

    run_config_dir="$run_dir/$type/$config"
    run_worker_ready_dir="$run_config_dir/running"
    save_to="$run_config_dir/$host"

    [ ! -d "$save_to" ] && mkdir -p "$save_to"
    [ ! -d "$run_worker_ready_dir" ] && mkdir -p "$run_worker_ready_dir"


    set_worker_ready "$host" "$run_worker_ready_dir"
    wait_all_workers "$run_worker_ready_dir" "$worker_count"

    benchmark_db "$benchbase_dir" "$interpolated_config" "$benchmark" "$save_config_dir"

    cp -r "$save_config_dir/." "$save_to/"
}


function create_db() {
    local type
    local config
    local benchmark
    local database
    
    local config_file

    type=$1
    config=$2
    benchmark=$3
    database=$4


    config_file="$config_dir/$type/$config.xml"
    interpolated_config_dir="$save_dir/$bench_type/$type"
    interpolated_config="$interpolated_config_dir/$config.xml"

    [ ! -d "$interpolated_config_dir" ] && mkdir -p "$interpolated_config_dir"

    sed "s/databaseUrl/$database/g" "$config_file" > "$interpolated_config"

    build_db "$benchbase_dir" "$interpolated_config" "$benchmark"
}


progressMessage "Host $host running in $save_dir directory"
progressMessage "Starting run $run with $worker_count workers, ${test_file} test file and ${database} database..."

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
        progressMessage "Loading database - Type: $type - Config: $config - Benchmark: $benchmark"
        progressMessage "Starting database..."
        start_cluster
        progressMessage "The server has started!"

        progressMessage "Building database - Type: $type - Config: $config - Benchmark: $benchmark"
        create_db "$type" "$config" "$benchmark" "$database"
        progressMessage "Database was built!"

        progressMessage "Setting load as completed..."
        set_worker_ready "$database" "$load_completion"
        
        progressMessage "Load completed!"

    else
        progressMessage "Waiting for load to be completed..."
        wait_worker "$database" "$load_completion"

        progressMessage "Running test - Type: $type - Config: $config - Benchmark: $benchmark"
        run_benchmark "$type" "$config" "$benchmark" "$database"
        progressMessage "Benchmarks completed!"

        set_worker_ready "$host" "$worker_tests_completed"
    fi

    if [ "$host" = "$database" ]; then
        wait_all_workers "$worker_tests_completed" "$worker_count"

        ./stop.sh

        ./plot_data.py -d "${run_dir}"

        progressMessage "Run file updated!"
    fi

done < "$test_file"