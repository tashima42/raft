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

echo "Starting 3 parallel instances of raft"
echo "Press [CTRL+C] at any time to close all of them."

./dist/raft -peers-ids "2,3" -peers-addresses "localhost:6438,localhost:6439" -port 6437 -api-port 5437 -id 1 -db-location "raft-1.db" &>"raft-1.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,3" -peers-addresses "localhost:6437,localhost:6439" -port 6438 -api-port 5438 -id 2 -db-location "raft-2.db" &>"raft-2.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2" -peers-addresses "localhost:6437,localhost:6438" -port 6439 -api-port 5439 -id 3 -db-location "raft-3.db" &>"raft-3.log" &
PIDS+=($!)

wait
