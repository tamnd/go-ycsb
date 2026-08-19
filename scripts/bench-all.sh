#!/usr/bin/env bash
# Run every engine through the same workloads in one go, on one host.
#
# usage: scripts/bench-all.sh [recordcount] [threadcount] [engine ...]
#
# This exists because the run has to be one process from start to finish.
# On WSL the VM is torn down whenever it goes idle, which takes the docker
# daemon and both server containers with it, so starting the servers in
# one ssh and benchmarking in the next finds nothing listening. Everything
# that has to stay up stays up inside this script.

set -uo pipefail

cd "$(dirname "$0")/.."

RECORDS="${1:-10000}"
THREADS="${2:-1}"
shift 2 2>/dev/null || shift $#
ENGINES=("$@")
if [ ${#ENGINES[@]} -eq 0 ]; then
  ENGINES=(sqlite duckdb ladybug pg neo4j)
fi

WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"
export WORK

need_servers=0
for e in "${ENGINES[@]}"; do
  case "$e" in pg|neo4j) need_servers=1 ;; esac
done

if [ "$need_servers" = 1 ]; then
  echo "### servers"
  bash scripts/bench-servers.sh "${SERVERS_MODE:-wait}" || exit 1
fi

echo "### build"
bash scripts/build-engine.sh "${ENGINES[@]}" || exit 1

for e in "${ENGINES[@]}"; do
  echo "### $e"
  bash scripts/bench-engine.sh "$e" "$RECORDS" "$THREADS"
done

echo "### done"
ls -la "$WORK"/*.tsv
