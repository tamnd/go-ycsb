#!/usr/bin/env bash
# Sweep one engine across fieldcount at a fixed record count.
#
# This exists to separate two explanations of zu's insert cost that the
# record count sweep cannot tell apart. zu commits by folding, and a fold
# rewrites whole columns, so the cost per insert grows with the number of
# columns. It also defers commits into a patch until a bound trips, and
# one of those bounds is a cell count, so the number of columns also sets
# how many inserts fit between folds.
#
# If the per insert cost is linear in fieldcount, folds are happening at
# a fixed rate and each one costs a column rewrite. If it is quadratic,
# the cell bound is what triggers the fold and the two effects multiply.
# Those want different fixes, and one measurement across four fieldcounts
# tells them apart.
#
# zu2 is here as the control rather than as a suspect. It appends a
# record to a log as one blob and folds nothing, so at a constant payload
# its per insert cost should not move with fieldcount at all. A zu2 line
# that does slope says the cost is in the adapter or in the harness's own
# per field work, which is worth knowing before reading anything into the
# zu1 line taken on the same host in the same run.
#
# The payload is held constant on purpose. fieldlength moves with
# fieldcount so every row is about the same number of bytes, otherwise a
# fieldcount sweep is also a payload sweep and neither answer is clean.
#
# usage: scripts/bench-fields.sh <engine> [fieldcounts...]
#
#   scripts/bench-fields.sh zu 1 2 5 10
#   RECORDS=2000 BATCH=100 scripts/bench-fields.sh zu

set -uo pipefail

cd "$(dirname "$0")/.."

ENGINE="${1:?usage: bench-fields.sh <engine> [fieldcounts...]}"
shift
FIELDS=("$@")
[ ${#FIELDS[@]} -eq 0 ] && FIELDS=(1 2 5 10)

RECORDS="${RECORDS:-2000}"
BATCH="${BATCH:-1}"
PAYLOAD="${PAYLOAD:-1000}"   # bytes per row, held constant across the sweep

WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

BIN="${BIN:-$WORK/ycsb-$ENGINE}"
OUT="${OUT:-$WORK/$ENGINE-fields-r$RECORDS-b$BATCH.tsv}"

if [ ! -x "$BIN" ]; then
  echo "no binary at $BIN, run scripts/build-engine.sh $ENGINE first" >&2
  exit 1
fi

DATA="$WORK/fields-$ENGINE"
case "$ENGINE" in
  sqlite)  ARGS=(-p sqlite.db="$DATA.db" -p sqlite.journalmode=WAL -p sqlite.synchronous=OFF) ;;
  duckdb)  ARGS=(-p duckdb.dbpath="$DATA.db") ;;
  ladybug) ARGS=(-p ladybug.dbpath="$DATA.lbug") ;;
  # The plain key value engines, which this sweep never had and which are
  # the ones a fieldcount question is most pointed at: their whole cost is
  # the row encoding and the write, with no planner or table in between.
  # no_sync on bolt for the reason bench-engine.sh gives, that every
  # engine here is measured at its fastest setting and this is the only
  # durability knob it has.
  lmdb)    ARGS=(-p lmdb.dir="$DATA.lmdb") ;;
  boltdb)  ARGS=(-p bolt.path="$DATA.bolt" -p bolt.no_sync="${BOLT_NO_SYNC:-true}") ;;
  badger)  ARGS=(-p badger.dir="$DATA.badger") ;;
  pebble)  ARGS=(-p pebble.dir="$DATA.pebble") ;;
  zu)      ARGS=(-p zu.dbpath="$DATA.zu1") ;;
  # Sized off the record count like bench-engine.sh does, for the same
  # reason: the table grows under traffic, so this only saves the load
  # the doublings it would take on the way up.
  zu2)     ARGS=(-p zu2.path="$DATA.zu2" -p zu2.index_buckets="$((RECORDS / 4 + 1))") ;;
  pg)      ARGS=(-p pg.host="${PGHOST:-127.0.0.1}" -p pg.port="${PGPORT:-55432}"
                 -p pg.user="${PGUSER:-postgres}" -p pg.password="${PGPASSWORD:-benchpass}"
                 -p pg.db="${PGDATABASE:-ycsb}" -p pg.sslmode=disable) ;;
  neo4j)   ARGS=(-p neo4j.uri="${NEO4J_URI:-bolt://127.0.0.1:7687}"
                 -p neo4j.username="${NEO4J_USER:-neo4j}"
                 -p neo4j.password="${NEO4J_PASSWORD:-benchpass}") ;;
  *) echo "unknown engine $ENGINE" >&2; exit 1 ;;
esac

DROP=()
case "$ENGINE" in pg|neo4j) DROP=(-p dropdata=true) ;; esac

reset_data() {
  case "$ENGINE" in
    sqlite)  rm -f "$DATA.db" "$DATA.db-wal" "$DATA.db-shm" "$DATA.db-journal" ;;
    duckdb)  rm -rf "$DATA.db" "$DATA.db.wal" ;;
    ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" "$DATA.lbug.wal.checkpoint" \
                   "$DATA.lbug.checkpoint.apply.lock" \
                   "$DATA.lbug.checkpoint.intent.lock" ;;
    lmdb)    rm -rf "$DATA.lmdb" ;;
    boltdb)  rm -f "$DATA.bolt" ;;
    badger)  rm -rf "$DATA.badger" ;;
    pebble)  rm -rf "$DATA.pebble" ;;
    zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
    # The log plus every sidecar. The bytes column below globs $DATA.*, so
    # a checkpoint left by the previous fieldcount would be charged to the
    # next one. tamnd/zu#610.
    zu2)     rm -rf "$DATA.zu2" "$DATA.zu2.ckpt" "$DATA.zu2.ckpt.writing" \
                   "$DATA.zu2.cold" "$DATA.zu2.relink" ;;
  esac
}

field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | tail -1; }

{
  echo "# engine: $ENGINE records: $RECORDS batch: $BATCH payload bytes per row: $PAYLOAD"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} | tee "$OUT"

# One throwaway load before the sweep, discarded. Every point here pays
# process start, and for a slow engine at two thousand rows that is noise
# against a load measured in seconds. For zu2 it is not: the first point
# came out at 94 us a row and the rest at 20, purely because the first
# one is the one that pages in the shared library and warms the file
# cache. Read in fieldcount order that looks like a per field cost that
# falls as fields are added, which is not a thing.
reset_data
"$BIN" load "$ENGINE" -P workloads/workloadc "${ARGS[@]}" "${DROP[@]+"${DROP[@]}"}" \
  -p recordcount=100 -p threadcount=1 -p batch.size="$BATCH" \
  -p fieldcount="${FIELDS[0]}" -p fieldlength=100 >/dev/null 2>&1

printf 'engine\trecords\tfields\tfieldlength\tbatch\tload_s\tus_per_row\tbytes\n' | tee -a "$OUT"

for f in "${FIELDS[@]}"; do
  reset_data
  len=$(( PAYLOAD / f ))
  [ "$len" -lt 1 ] && len=1

  start=$(date +%s.%N)
  out=$("$BIN" load "$ENGINE" -P workloads/workloadc "${ARGS[@]}" "${DROP[@]+"${DROP[@]}"}" \
    -p recordcount="$RECORDS" -p threadcount=1 -p batch.size="$BATCH" \
    -p fieldcount="$f" -p fieldlength="$len" 2>&1 \
    | grep -E '^(INSERT|BATCH_INSERT) ' | tail -1)
  end=$(date +%s.%N)

  [ -z "$out" ] && echo "load failed for $ENGINE at fieldcount $f" >&2

  # Three decimals, not one. zu2 loads two thousand rows in under a
  # twentieth of a second, which at one decimal is 0.0, and 0.0 divides
  # into a per row cost of zero. A row of zeroes reads like a result.
  wall=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.3f", b - a }')
  per=$(awk -v t="$wall" -v r="$RECORDS" 'BEGIN { if (r > 0) printf "%.0f", t * 1000000 / r; else print "na" }')
  bytes=$(du -sb "$DATA".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$ENGINE" "$RECORDS" "$f" "$len" "$BATCH" "$wall" "$per" "$bytes" | tee -a "$OUT"
done

reset_data
