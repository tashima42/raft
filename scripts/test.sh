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

./dist/raft -peers-ids "2,3,4" -peers-addresses "localhost:6438,localhost:6439,localhost:6440" -peers-api-addresses "http://localhost:5438,http://localhost:5439,http://localhost:5440" -port 6437 -api-port 5437 -id 1 -db-location "./dist/raft-1.db" -log-location "./dist/raft-1.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,3,4" -peers-addresses "localhost:6437,localhost:6439,localhost:6440" -peers-api-addresses "http://localhost:5437,http://localhost:5439,http://localhost:5440" -port 6438 -api-port 5438 -id 2 -db-location "./dist/raft-2.db" -log-location "./dist/raft-2.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2,4" -peers-addresses "localhost:6437,localhost:6438,localhost:6440" -peers-api-addresses "http://localhost:5437,http://localhost:5438,http://localhost:5440" -port 6439 -api-port 5439 -id 3 -db-location "./dist/raft-3.db" -log-location "./dist/raft-3.log" &
PIDS+=($!)
./dist/raft -peers-ids "1,2,3" -peers-addresses "localhost:6437,localhost:6438,localhost:6439" -peers-api-addresses "http://localhost:5437,http://localhost:5438,http://localhost:5439" -port 6440 -api-port 5440 -id 4 -db-location "./dist/raft-4.db" -log-location "./dist/raft-4.log" &
PIDS+=($!)

wait
