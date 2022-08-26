#!/usr/bin/bash

database="charmander-2"
nodes=("${database}" "charmander-3" "charmander-4" "charmander-5" "squirtle-2")
worker_count=$(expr ${#nodes[@]} - 1)
# nodes=("${database}")


for n in ${nodes[@]} ; do
  oarsub -l nodes=1,walltime=18:00:00 -p "network_address in ('$n')" "/home/jlourenco/tparreira/run.sh ${worker_count} /home/jlourenco/tparreira/tests.txt $database"
done