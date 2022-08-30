#!/usr/bin/bash

run_file="/home/jlourenco/tparreira/current_run"

function start() {
  run=$(cat $run_file)
  run=$(expr $run + 1);

  for n in ${nodes[@]} ; do
    oarsub -l nodes=1,walltime=18:00:00 -p "network_address in ('$n')" "/home/jlourenco/tparreira/run.sh ${worker_count} /home/jlourenco/tparreira/tests.txt $database $run"
  done

  echo $run > $run_file
}


database="squirtle-2"
nodes=("${database}" "charmander-2" "charmander-3" "charmander-4" "charmander-5")
worker_count=$(expr ${#nodes[@]} - 1)
start()

database="squirtle-3"
nodes=("${database}" "gengar-1" "gengar-2" "gengar-3" "gengar-4" "gengar-5")
worker_count=$(expr ${#nodes[@]} - 1)
start()