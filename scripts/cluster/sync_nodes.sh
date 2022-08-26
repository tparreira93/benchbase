#!/usr/bin/bash

function set_worker_ready() {
    local worker
    local ready_location

    worker=$1
    ready_location=$2

    echo "ready" > "$ready_location/$worker-ready"
}



function wait_worker() {
    local worker
    local ready_location

    worker=$1
    ready_location=$2

    workers_ready=$(ls $ready_location/$worker-ready 2> /dev/null | wc -l)
    echo "Waiting for $worker"
    until [ $workers_ready -eq 1 ]
    do
        sleep 2s
        workers_ready=$(ls $ready_location/$worker-ready 2> /dev/null | wc -l)
    done
    echo "$worker is ready!"
}

function wait_all_workers() {
    local ready_location
    local workers_count

    ready_location=$1
    workers_count=$2

    workers_ready=$(ls $ready_location/*ready 2> /dev/null | wc -l)
    echo "Waiting for all workers to be ready ($workers_ready / $workers_count)"
    until [ $workers_ready -eq $workers_count ]
    do
        sleep 2s
        tmp=$(ls $ready_location/*ready 2> /dev/null | wc -l)
        [ $tmp -gt $workers_ready ] && echo "Waiting for all workers to be ready ($workers_ready / $workers_count)"
        workers_ready=$tmp
    done
    echo "All workers ready ($workers_ready / $workers_count)!"
}