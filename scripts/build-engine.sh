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
    # pg came with go-ycsb and has no tag, it is always compiled in. It
    # still gets its own binary so nothing else is linked alongside.
    pg) tag="" ;;
  esac

  # Extra link flags, per engine and per platform.
  #
  # liblbug is a static archive on Linux and it does not carry its own
  # dependencies, so the link fails on a wall of undefined references to
  # OpenSSL out of the extension installer and to __atomic_compare_exchange
  # out of the buffer manager. That reads like a broken library and is not
  # one, the caller just has to name what the archive uses. The cgo lines
  # in db.go cover the Homebrew case on macOS, so this only fires on Linux
  # and only if the caller has not already set CGO_LDFLAGS.
  ldflags="${CGO_LDFLAGS:-}"
  if [ "$e" = ladybug ] && [ -z "$ldflags" ] && [ "$(uname -s)" = Linux ]; then
    ldflags="-L${LBUG_LIB:-/usr/local/lib} -llbug -lssl -lcrypto -latomic -lstdc++ -lm -ldl -Wl,-rpath,${LBUG_LIB:-/usr/local/lib}"
  fi

  echo "building $WORK/ycsb-$e${tag:+ (tag $tag)}"
  if [ -n "$tag" ]; then
    CGO_LDFLAGS="$ldflags" go build -tags "$tag" -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  else
    go build -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  fi
done

ls -la "$WORK"/ycsb-*
