#!/usr/bin/bash

run_file="/home/jlourenco/tparreira/current_run"
tpcc_dir="/home/jlourenco/tparreira/tpcc"
source "/home/jlourenco/tparreira/sync_nodes.sh"


function progressMessage() {
    local message

    message=$1

    echo "==================================================================================================================================="
    echo "$message"
    echo "==================================================================================================================================="
}

function start() {
  local run

  run=$(cat $run_file)
  run=$(expr $run + 1);

  for n in ${nodes[@]} ; do
    oarsub -l nodes=1,walltime=18:00:00 -p "network_address in ('$n')" "/home/jlourenco/tparreira/run.sh ${worker_count} /home/jlourenco/tparreira/tests.txt $database $run"
  done

  echo $run > $run_file
}

for i in {1..5}; do
  progressMessage "Starting run $i out of 5....."
  database="charmander-4"
  nodes=("${database}" "gengar-1" "gengar-2" "gengar-3" "gengar-4" "gengar-5")
  worker_count=$(expr ${#nodes[@]} - 1)
  start

  current_run=$(cat $run_file)

  wait_all_workers "$tpcc_dir/run-$current_run" "1"
  sleep 1m
done


# database="squirtle-2"
# nodes=("${database}" "charmander-2" "charmander-3" "charmander-4" "charmander-5")
# worker_count=$(expr ${#nodes[@]} - 1)
# start


# database="shelder-1"
# nodes=("${database}" "psyduck-1" "psyduck-2" "psyduck-3")
# worker_count=$(expr ${#nodes[@]} - 1)
# start