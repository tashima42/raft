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

./dist/raft -peers-ids "2,3" -peers-addresses "http://localhost:6438,http://localhost:6439" -port 6437 -id 1 &>"raft-1.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,3" -peers-addresses "http://localhost:6437,http://localhost:6439" -port 6438 -id 2 &>"raft-2.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2" -peers-addresses "http://localhost:6437,http://localhost:6438" -port 6439 -id 3 &>"raft-3.log" &
PIDS+=($!)

wait
