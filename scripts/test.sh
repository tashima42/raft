#!/bin/bash

cd "$(dirname "$0")/.." || exit

COMMANDS=(
)
PIDS=()

cleanup() {
  echo -e "\ninterrupt received, stopping all instances..."
  kill "${PIDS[@]}" 2>/dev/null
  echo "all instances closed."
  exit 1
}

trap cleanup SIGINT SIGTERM

echo "Starting 4 parallel instances of raft"
echo "Press [CTRL+C] at any time to close all of them."

./dist/raft -peers-ids "2,3,4" -peers-addresses "localhost:6432,localhost:6433,localhost:6434" -peers-kv-addresses "http://localhost:5432,http://localhost:5433,http://localhost:5434" -port 6431 -kv-port 5431 -id 1 -db-location "./dist/raft-1.db" -log-location "./dist/raft-1.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,3,4" -peers-addresses "localhost:6431,localhost:6433,localhost:6434" -peers-kv-addresses "http://localhost:5431,http://localhost:5433,http://localhost:5434" -port 6432 -kv-port 5432 -id 2 -db-location "./dist/raft-2.db" -log-location "./dist/raft-2.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2,4" -peers-addresses "localhost:6431,localhost:6432,localhost:6434" -peers-kv-addresses "http://localhost:5431,http://localhost:5432,http://localhost:5434" -port 6433 -kv-port 5433 -id 3 -db-location "./dist/raft-3.db" -log-location "./dist/raft-3.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2,3" -peers-addresses "localhost:6431,localhost:6432,localhost:6433" -peers-kv-addresses "http://localhost:5431,http://localhost:5432,http://localhost:5433" -port 6434 -kv-port 5434 -id 4 -db-location "./dist/raft-4.db" -log-location "./dist/raft-4.log" &
PIDS+=($!)

wait
