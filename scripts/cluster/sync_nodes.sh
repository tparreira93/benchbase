#!/usr/bin/bash

function set_load_is_ready() {
    echo "setting"
    touch $1
    echo "ready" > "$1"
}

function load_is_ready() {
    local run

    run=$1
    if [ -f "$run" ]; then
        cat "$run" | grep "ready"
    else
        return 1
    fi
}

function wait_load_is_ready() {
    until [ load_is_ready ]
    do
        sleep 1s
    done
}


function set_worker_ready() {
    local worker
    local ready_location

    worker=$1
    ready_location=$2

    echo "ready" > "$ready_location/$worker-ready"
}

function wait_all_workers() {
    local ready_location
    local workers_count

    ready_location=$1
    workers_count=$2
    
    workers_ready=$(ls $ready_location/*ready 2> /dev/null | wc -l)
    until [ $workers_ready = $workers_count ]
    do
        echo "Waiting for all workers to be ready ($workers_ready / $workers_count)"
        sleep 2s
        workers_ready=$(ls $ready_location/*ready 2> /dev/null | wc -l)
    done
    echo "All workers ready ($workers_ready / $workers_count)!"
}