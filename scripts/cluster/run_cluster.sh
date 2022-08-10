#!/usr/bin/bash

database="charmander-2"
nodes=("charmander-2" "charmander-3" "charmander-4" "squirtle-2")


for n in nodes ; do
  oarsub -l nodes=1,walltime=18:00:00 -p "network_address in ('$n')" "/home/jlourenco/tparreira/run.sh ${#nodes[@]} /home/jlourenco/tparreira/tests.txt $database"
done