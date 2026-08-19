#!/usr/bin/env bash
# Sweep one engine across thread counts on workload C and write a TSV.
#
# Load once, then run the read phase repeatedly against the same data at
# each thread count. Workload C is read only so the dataset survives the
# sweep, and reloading between points would put load variance into a
# number that is supposed to be about concurrency only.
#
# The result to look at is the shape and not the peak. An engine that
# scales prints rising ops with a flat p50. An engine that is queueing
# somewhere prints flat ops with a p50 that doubles every time the thread
# count doubles, and that pattern is worth chasing before publishing,
# because it is as often the adapter as the engine. It was the adapter
# twice here, see the comments in db/sqlite/db.go and db/duckdb/db.go.
#
# usage: scripts/bench-threads.sh <engine> [threads...]
#
#   scripts/bench-threads.sh sqlite 1 2 4 8 16
#   RECORDS=50000 scripts/bench-threads.sh pg
#   WORKLOAD=workloada scripts/bench-threads.sh sqlite 1 4 16
#
# WORKLOAD picks the workload file and defaults to workloadc. Anything
# other than C writes, and then the note above about the dataset
# surviving the sweep stops being true in the sense that matters: the row
# count is stable but the contents are not, and later points run against
# data earlier points modified. For A and F that is acceptable because
# the keys and the sizes are what drive the cost. For a workload that
# grows the table it would not be, and there is no such workload here.
#
# BATCH sets -p batch.size for the load phase only. It changes how long
# the load takes and not what the run phase measures, which is the point
# of it being here at all: without it the zu load does not finish.

set -uo pipefail

cd "$(dirname "$0")/.."

ENGINE="${1:?usage: bench-threads.sh <engine> [threads...]}"
shift
THREADS=("$@")
[ ${#THREADS[@]} -eq 0 ] && THREADS=(1 2 4 8 16)

WORKLOAD="${WORKLOAD:-workloadc}"
RECORDS="${RECORDS:-10000}"
BATCH="${BATCH:-1}"
OPS="${OPS:-20000}"

# Not /tmp. WSL tears its VM down when idle and /tmp is tmpfs.
WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

BIN="${BIN:-$WORK/ycsb-$ENGINE}"
OUT="${OUT:-$WORK/$ENGINE-threads-$WORKLOAD-r$RECORDS.tsv}"

if [ ! -x "$BIN" ]; then
  echo "no binary at $BIN, run scripts/build-engine.sh $ENGINE first" >&2
  exit 1
fi

DATA="$WORK/threads-$ENGINE"
case "$ENGINE" in
  sqlite)  ARGS=(-p sqlite.db="$DATA.db" -p sqlite.journalmode=WAL -p sqlite.synchronous=OFF) ;;
  duckdb)  ARGS=(-p duckdb.dbpath="$DATA.db") ;;
  ladybug) ARGS=(-p ladybug.dbpath="$DATA.lbug") ;;
  zu)      ARGS=(-p zu.dbpath="$DATA.zu1") ;;
  pg)      ARGS=(-p pg.host="${PGHOST:-127.0.0.1}" -p pg.port="${PGPORT:-55432}"
                 -p pg.user="${PGUSER:-postgres}" -p pg.password="${PGPASSWORD:-benchpass}"
                 -p pg.db="${PGDATABASE:-ycsb}" -p pg.sslmode=disable) ;;
  neo4j)   ARGS=(-p neo4j.uri="${NEO4J_URI:-bolt://127.0.0.1:7687}"
                 -p neo4j.username="${NEO4J_USER:-neo4j}"
                 -p neo4j.password="${NEO4J_PASSWORD:-benchpass}") ;;
  *) echo "unknown engine $ENGINE" >&2; exit 1 ;;
esac

# Extra properties for the sweep, so a pool size or a cache mode can be
# varied without editing the script. This is how the sqlite mode columns
# in 05 were taken.
# shellcheck disable=SC2206
EXTRA=(${EXTRA_ARGS:-})

case "$ENGINE" in
  sqlite)  rm -f "$DATA.db" "$DATA.db-wal" "$DATA.db-shm" "$DATA.db-journal" ;;
  duckdb)  rm -rf "$DATA.db" "$DATA.db.wal" ;;
  ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" ;;
  zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
esac

DROP=()
case "$ENGINE" in pg|neo4j) DROP=(-p dropdata=true) ;; esac

# Take the last matching line. A phase slower than the reporting interval
# prints progress lines that look exactly like the summary.
field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | tail -1; }

{
  echo "# engine: $ENGINE workload: $WORKLOAD records: $RECORDS load batch: $BATCH ops per point: $OPS"
  echo "# extra: ${EXTRA_ARGS:-none}"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
  echo "# cores: $(nproc 2>/dev/null)"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} | tee "$OUT"

echo "loading $RECORDS records into $ENGINE" >&2
"$BIN" load "$ENGINE" -P "workloads/$WORKLOAD" "${ARGS[@]}" "${EXTRA[@]}" "${DROP[@]}" \
  -p recordcount="$RECORDS" -p threadcount=1 -p batch.size="$BATCH" >/dev/null 2>&1

printf 'engine\tworkload\trecords\tthreads\top\tops\tp50_us\tp99_us\n' | tee -a "$OUT"

for t in "${THREADS[@]}"; do
  all=$("$BIN" run "$ENGINE" -P "workloads/$WORKLOAD" "${ARGS[@]}" "${EXTRA[@]}" \
    -p recordcount="$RECORDS" -p operationcount="$OPS" -p threadcount="$t" 2>&1)

  # One row per operation kind rather than per run. Workload C has only
  # READ, A has READ and UPDATE, and the mix is the thing being measured
  # so both halves have to be visible. TOTAL comes last and is the one to
  # quote for a mixed workload.
  for op in READ UPDATE INSERT SCAN READ_MODIFY_WRITE TOTAL; do
    line=$(grep -E "^$op " <<<"$all" | tail -1)
    [ -z "$line" ] && continue
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$ENGINE" "$WORKLOAD" "$RECORDS" "$t" "$op" \
      "$(field 'OPS' "$line")" \
      "$(field ', 50th(us)' "$line")" \
      "$(field ', 99th(us)' "$line")" | tee -a "$OUT"
  done
done

case "$ENGINE" in
  sqlite)  rm -f "$DATA.db" "$DATA.db-wal" "$DATA.db-shm" "$DATA.db-journal" ;;
  duckdb)  rm -rf "$DATA.db" "$DATA.db.wal" ;;
  ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" ;;
  zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
esac
