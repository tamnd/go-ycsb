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
  zu)      ARGS=(-p zu.dbpath="$DATA.zu1") ;;
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
    ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" ;;
    zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
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

printf 'engine\trecords\tfields\tfieldlength\tbatch\tload_s\tus_per_row\tbytes\n' | tee -a "$OUT"

for f in "${FIELDS[@]}"; do
  reset_data
  len=$(( PAYLOAD / f ))
  [ "$len" -lt 1 ] && len=1

  start=$(date +%s.%N)
  out=$("$BIN" load "$ENGINE" -P workloads/workloadc "${ARGS[@]}" "${DROP[@]}" \
    -p recordcount="$RECORDS" -p threadcount=1 -p batch.size="$BATCH" \
    -p fieldcount="$f" -p fieldlength="$len" 2>&1 \
    | grep -E '^(INSERT|BATCH_INSERT) ' | tail -1)
  end=$(date +%s.%N)

  [ -z "$out" ] && echo "load failed for $ENGINE at fieldcount $f" >&2

  wall=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.1f", b - a }')
  per=$(awk -v t="$wall" -v r="$RECORDS" 'BEGIN { if (r > 0) printf "%.0f", t * 1000000 / r; else print "na" }')
  bytes=$(du -sb "$DATA".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$ENGINE" "$RECORDS" "$f" "$len" "$BATCH" "$wall" "$per" "$bytes" | tee -a "$OUT"
done

reset_data
