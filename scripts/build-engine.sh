#!/usr/bin/env bash
# Build one YCSB binary per engine.
#
# usage: scripts/build-engine.sh <engine> [engine ...]
#        scripts/build-engine.sh all
#
# One binary per engine rather than one binary with every tag on, for two
# reasons. The first is that it has to be this way: the DuckDB driver and
# the LadybugDB static library both vendor mbedtls, so linking both into
# one executable fails on duplicate symbols. The second is that it should
# be this way even where it links. The DuckDB driver pulls in a jemalloc
# extension, and an allocator linked into the process is the allocator for
# every engine in it, so a binary carrying DuckDB would be measuring
# SQLite against a different malloc than a binary without it. Separate
# binaries mean each engine is measured in the process it would actually
# ship in.

set -euo pipefail

cd "$(dirname "$0")/.."

WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

ENGINES=("$@")
if [ "${1:-}" = "all" ] || [ $# -eq 0 ]; then
  ENGINES=(sqlite duckdb ladybug neo4j pg zu)
fi

for e in "${ENGINES[@]}"; do
  tag="$e"
  case "$e" in
    # pg and neo4j are pure Go and always compiled in, so they have no
    # tag of their own and any binary can run them. They still get their
    # own binary so nothing else is linked alongside.
    pg|neo4j) tag="" ;;
  esac

  echo "building $WORK/ycsb-$e${tag:+ (tag $tag)}"
  if [ -n "$tag" ]; then
    go build -tags "$tag" -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  else
    go build -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  fi
done

ls -la "$WORK"/ycsb-*
