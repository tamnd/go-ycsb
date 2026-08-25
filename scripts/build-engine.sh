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
  ENGINES=(sqlite duckdb ladybug neo4j pg redis valkey keydb garnet badger pebble lmdb boltdb zu zu2)
fi

for e in "${ENGINES[@]}"; do
  tag="$e"
  case "$e" in
    # pg and mongodb came with go-ycsb and have no tag, they are always
    # compiled in. They still get their own binary so nothing else is
    # linked alongside.
    # redis and valkey are the same story as pg: no tag, always compiled
    # in, and they still get a binary each so nothing else is linked
    # beside them. valkey is the redis adapter under another name, so the
    # tag would be redis either way.
    # badger and boltdb have no tag either. Both are pure Go and came
    # with go-ycsb compiled in unconditionally, so a -tags badger would
    # be a tag nothing reads. They still get their own binary for the
    # reason at the top of this file.
    pg|mongodb|redis|valkey|keydb|garnet|badger|boltdb) tag="" ;;
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
  # Extra compile flags, same idea.
  #
  # SQLITE_DEFAULT_MEMSTATUS=0 for sqlite. mattn/go-sqlite3 compiles the
  # amalgamation without it, so sqlite's allocator takes one process wide
  # mutex per malloc to maintain a byte counter for sqlite3_memory_used,
  # which nothing in this harness reads. A point read does several
  # allocations, so at n threads every read serialises n ways on that
  # lock and sqlite gets slower with more threads instead of faster.
  # Measured natively outside the harness on an M4 at eight threads it is
  # the difference between 83000 reads a second and 635000, which is
  # 7.6x. Through this harness the same comparison is 127000 against
  # 140000, which is ten percent, and the gap between those two figures
  # is itself the point: the client and the cgo crossing cost tens of
  # microseconds an operation here, so a lock inside sqlite that costs
  # microseconds is most of the engine and a tenth of what the harness
  # times. Ten percent is still ten percent and it is a compile flag.
  # See tamnd/zu#646.
  # The default when CGO_CFLAGS is unset is "-O2 -g", and setting the
  # variable replaces that rather than adding to it, so the default has
  # to be written out or the sqlite amalgamation compiles at -O0 and the
  # fix below costs more than it saves.
  #
  # -O2 -g leads and the caller's flags follow, rather than the caller
  # replacing it. Writing it as ${CGO_CFLAGS:--O2 -g} meant a caller who
  # set CGO_CFLAGS for one engine took the optimisation away from all of
  # them: gp-run.sh exports an include path so the sweep can find zu2's
  # header, and that alone was enough to compile the sqlite amalgamation
  # at -O0, which is the exact failure the paragraph above describes.
  # Anything the caller passes comes after and so still wins.
  cflags="-O2 -g ${CGO_CFLAGS:-}"
  if [ "$e" = sqlite ]; then
    cflags="$cflags -DSQLITE_DEFAULT_MEMSTATUS=0"
  fi

  # Appended rather than only used when CGO_LDFLAGS is empty. The empty
  # test assumed the variable is either unset or already ladybug's, and a
  # caller that sets it for a different engine breaks that: gp-run.sh
  # exports CGO_LDFLAGS pointing at libzu2 so the whole sweep can link
  # zu2, which silently took -latomic away from ladybug, and ladybug then
  # failed to link on a host where its library was installed and fine.
  # Appending gives every engine what it needs and leaves what the caller
  # asked for in place.
  ldflags="${CGO_LDFLAGS:-}"
  if [ "$e" = ladybug ] && [ "$(uname -s)" = Linux ]; then
    ldflags="$ldflags -L${LBUG_LIB:-/usr/local/lib} -llbug -lssl -lcrypto -latomic -lstdc++ -lm -ldl -Wl,-rpath,${LBUG_LIB:-/usr/local/lib}"
  fi

  # Print the flags. #689 was a compile flag that silently did not apply
  # for weeks and cost sqlite most of its throughput in every table taken
  # in that time, and the reason it went unnoticed is that nothing in the
  # output said what the build was actually using. One line a build makes
  # the next one of these visible in the log that the run already keeps.
  echo "building $WORK/ycsb-$e${tag:+ (tag $tag)} cflags [$cflags] ldflags [$ldflags]"
  if [ -n "$tag" ]; then
    CGO_CFLAGS="$cflags" CGO_LDFLAGS="$ldflags" go build -tags "$tag" -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  else
    CGO_CFLAGS="$cflags" go build -o "$WORK/ycsb-$e" ./cmd/go-ycsb
  fi
done

ls -la "$WORK"/ycsb-*
