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
  # The same list build-engine.sh defaults to, because the top of this
  # file says every engine and for a long time it meant six of them. Nine
  # adapters landed after that line was written, boltdb most recently, and
  # a run with no arguments quietly left every one of them out. An engine
  # that will not build on the host is named and dropped below, so the
  # cost of listing one that is not there is a line of output.
  #
  # Ordered so the embedded engines go first and the three that need a
  # container go last. A sweep that dies partway then still has the rows
  # that did not depend on anything outside the process.
  ENGINES=(sqlite duckdb ladybug lmdb boltdb badger pebble zu zu2
    redis valkey keydb garnet pg neo4j mongodb)
fi

WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"
export WORK

need_servers=0
for e in "${ENGINES[@]}"; do
  case "$e" in pg|neo4j|mongodb) need_servers=1 ;; esac
done

if [ "$need_servers" = 1 ]; then
  echo "### servers"
  bash scripts/bench-servers.sh "${SERVERS_MODE:-wait}" || exit 1
fi

echo "### build"
# One engine at a time, and an engine that will not build is named and
# left out rather than taking the sweep with it. Building the whole set
# in one call meant a missing ladybug header on server2 cost sqlite and
# duckdb as well, and the sweep reported nothing at all for a host where
# two of the three engines were fine.
RUN=()
for e in "${ENGINES[@]}"; do
  if bash scripts/build-engine.sh "$e"; then
    RUN+=("$e")
  else
    echo "### $e will not build on this host, leaving it out"
  fi
done
if [ ${#RUN[@]} -eq 0 ]; then
  echo "### nothing built, so there is nothing to run" >&2
  exit 1
fi
echo "### running: ${RUN[*]}"

for e in "${RUN[@]}"; do
  echo "### $e"
  bash scripts/bench-engine.sh "$e" "$RECORDS" "$THREADS"
done

echo "### done"
ls -la "$WORK"/*.tsv
