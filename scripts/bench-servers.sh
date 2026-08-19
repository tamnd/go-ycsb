#!/usr/bin/env bash
# Bring up the two engines that need a server, at the settings that make
# each one as fast as it goes, and wait until both answer.
#
# usage: scripts/bench-servers.sh [up|down|wait]
#
# Both containers run on the host doing the measuring, so the hop is
# loopback. Neo4j cannot be embedded at all and PostgreSQL is not embedded
# here, so those two pay a protocol round trip the in process engines do
# not. That is what running them costs and the numbers say so.

set -uo pipefail

NEO4J_PASSWORD="${NEO4J_PASSWORD:-benchpass}"
PGPASSWORD="${PGPASSWORD:-benchpass}"
PGPORT="${PGPORT:-55432}"
BOLT_PORT="${BOLT_PORT:-7687}"

# Sized against a 32 GB host. Both get enough that the whole working set
# is resident, because a benchmark that pages is measuring the disk.
HEAP="${NEO4J_HEAP:-8G}"
PAGECACHE="${NEO4J_PAGECACHE:-8G}"
SHARED_BUFFERS="${PG_SHARED_BUFFERS:-8GB}"

up() {
  docker rm -f neo4j-ycsb pg-ycsb >/dev/null 2>&1

  docker run -d --name neo4j-ycsb --restart unless-stopped \
    -p "$BOLT_PORT":7687 -p 7474:7474 \
    -e NEO4J_AUTH="neo4j/$NEO4J_PASSWORD" \
    -e NEO4J_server_memory_heap_initial__size="$HEAP" \
    -e NEO4J_server_memory_heap_max__size="$HEAP" \
    -e NEO4J_server_memory_pagecache_size="$PAGECACHE" \
    neo4j:latest >/dev/null

  # fsync off, synchronous_commit off and full_page_writes off is the
  # same bargain SQLite makes at synchronous=OFF, which is the setting
  # every engine in this comparison is held to. None of these numbers is
  # a durability claim.
  docker run -d --name pg-ycsb --restart unless-stopped \
    -p "$PGPORT":5432 --shm-size=2g \
    -e POSTGRES_PASSWORD="$PGPASSWORD" -e POSTGRES_DB=ycsb \
    postgres:latest \
    -c shared_buffers="$SHARED_BUFFERS" \
    -c fsync=off \
    -c synchronous_commit=off \
    -c full_page_writes=off \
    -c max_wal_size=8GB \
    -c checkpoint_timeout=30min \
    -c wal_level=minimal \
    -c max_wal_senders=0 >/dev/null
}

# restart unless-stopped only helps once the docker daemon is back. On
# WSL the daemon goes away with the VM, so ask for a start either way.
start_existing() {
  docker start neo4j-ycsb pg-ycsb >/dev/null 2>&1
}

wait_ready() {
  local i
  for i in $(seq 1 120); do
    if docker exec pg-ycsb pg_isready -q -U postgres >/dev/null 2>&1 &&
       docker exec neo4j-ycsb cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
         'RETURN 1' >/dev/null 2>&1; then
      echo "servers ready after ${i}s"
      docker exec pg-ycsb psql -U postgres -tAc 'select version()'
      docker exec neo4j-ycsb cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
        'call dbms.components() yield name, versions return name, versions' | tail -3
      return 0
    fi
    sleep 1
  done
  echo "servers did not come up in 120s" >&2
  docker ps -a --format '{{.Names}} {{.Status}}' >&2
  return 1
}

case "${1:-up}" in
  up)   up; wait_ready ;;
  wait) start_existing; wait_ready ;;
  down) docker rm -f neo4j-ycsb pg-ycsb ;;
  *)    echo "usage: $0 [up|down|wait]" >&2; exit 2 ;;
esac
