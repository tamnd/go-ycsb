#!/usr/bin/env bash
# Sweep one engine across record counts on workload C and write a TSV.
#
# This is the measurement that says whether a lookup is O(1) or O(n), and
# it is the reason the sweep exists as its own script rather than as a
# flag on bench-engine.sh. A single record count cannot tell an indexed
# engine from an unindexed one. Four can, and the shape of the line is
# the result rather than any single value in it.
#
# usage: scripts/bench-scale.sh <engine> [records...]
#
#   scripts/bench-scale.sh sqlite 10000 20000 50000
#   BATCH=1000 scripts/bench-scale.sh zu 10000 20000 50000
#
# BATCH sets -p batch.size. Only zu, zu2, sqlite and duckdb implement BatchDB,
# and with it set the harness reports the load as BATCH_INSERT rather
# than as INSERT, which is why the load row is taken from wall time here
# instead of from the summary line. A batched number is only comparable
# against another batched number, so BATCH goes in the output.

set -uo pipefail

cd "$(dirname "$0")/.."

ENGINE="${1:?usage: bench-scale.sh <engine> [records...]}"
shift
RECORDS=("$@")
[ ${#RECORDS[@]} -eq 0 ] && RECORDS=(10000 20000 50000)

BATCH="${BATCH:-1}"
OPS="${OPS:-2000}"

# Not /tmp. WSL tears its VM down when idle and /tmp is tmpfs.
WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

BIN="${BIN:-$WORK/ycsb-$ENGINE}"
OUT="${OUT:-$WORK/$ENGINE-scale-b$BATCH.tsv}"

if [ ! -x "$BIN" ]; then
  echo "no binary at $BIN, run scripts/build-engine.sh $ENGINE first" >&2
  exit 1
fi

DATA="$WORK/scale-$ENGINE"
args_for() {  # args_for <recordcount>
  case "$ENGINE" in
    sqlite)  echo "-p sqlite.db=$DATA.db -p sqlite.journalmode=WAL -p sqlite.synchronous=OFF" ;;
    duckdb)  echo "-p duckdb.dbpath=$DATA.db" ;;
    ladybug) echo "-p ladybug.dbpath=$DATA.lbug" ;;
    zu)      echo "-p zu.dbpath=$DATA.zu1" ;;
    # The bucket hint is the reason args_for takes the record count at
    # all. Sizing it per point keeps the table at the same load factor
    # across the sweep, so the line says how a lookup scales and not how
    # many times the table doubled on the way to that point.
    zu2)     echo "-p zu2.path=$DATA.zu2 -p zu2.index_buckets=$(( $1 / 4 + 1 ))" ;;
    pg)      echo "-p pg.host=${PGHOST:-127.0.0.1} -p pg.port=${PGPORT:-55432} -p pg.user=${PGUSER:-postgres} -p pg.password=${PGPASSWORD:-benchpass} -p pg.db=${PGDATABASE:-ycsb} -p pg.sslmode=disable" ;;
    neo4j)   echo "-p neo4j.uri=${NEO4J_URI:-bolt://127.0.0.1:7687} -p neo4j.username=${NEO4J_USER:-neo4j} -p neo4j.password=${NEO4J_PASSWORD:-benchpass}" ;;
  esac
}

reset_data() {
  case "$ENGINE" in
    sqlite)  rm -f "$DATA.db" "$DATA.db-wal" "$DATA.db-shm" "$DATA.db-journal" ;;
    duckdb)  rm -rf "$DATA.db" "$DATA.db.wal" ;;
    ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" "$DATA.lbug.wal.checkpoint" \
                   "$DATA.lbug.checkpoint.apply.lock" \
                   "$DATA.lbug.checkpoint.intent.lock" ;;
    zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
    # Every sidecar, not just the log. The bytes column globs $DATA.* and
    # a checkpoint from the previous record count would land in the next
    # point's row. tamnd/zu#610.
    zu2)     rm -rf "$DATA.zu2" "$DATA.zu2.ckpt" "$DATA.zu2.ckpt.writing" \
                   "$DATA.zu2.cold" "$DATA.zu2.relink" ;;
    pg|neo4j) : ;;  # nothing on this side, dropdata handles it
  esac
}

DROP=()
case "$ENGINE" in pg|neo4j) DROP=(-p dropdata=true) ;; esac

# Take the last matching line. A phase slower than the reporting interval
# prints progress lines that look exactly like the summary.
field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | tail -1; }

{
  echo "# engine: $ENGINE batch: $BATCH ops per run phase: $OPS"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} | tee "$OUT"

printf 'engine\trecords\tbatch\tload_s\trows_per_s\tload_p50_us\tbytes\tread_ops\tread_p50_us\n' | tee -a "$OUT"

for r in "${RECORDS[@]}"; do
  reset_data
  # shellcheck disable=SC2046
  ARGS=($(args_for "$r"))

  start=$(date +%s.%N)
  load_out=$("$BIN" load "$ENGINE" -P workloads/workloadc "${ARGS[@]}" "${DROP[@]+"${DROP[@]}"}" \
    -p recordcount="$r" -p threadcount=1 -p batch.size="$BATCH" 2>&1 \
    | grep -E '^(INSERT|BATCH_INSERT) ' | tail -1)
  end=$(date +%s.%N)

  # A load that printed no summary line did not load anything. Say so
  # here rather than letting the row through, because the row it produces
  # looks like a win: wall time near zero divides into a load rate of
  # half a million per second, and the only sign it is wrong is the empty
  # read column further along the same line.
  if [ -z "$load_out" ]; then
    echo "load failed for $ENGINE at $r records, no summary line" >&2
  fi

  # Wall time rather than the summary OPS line, because with batching on
  # the summary counts batches and not rows, and the two are not the same
  # number divided by anything obvious once the last batch is short.
  # Three decimals, not one. A fast engine at a small record count lands
  # under 0.05s, which at one decimal is 0.0, and then rows_per_s prints
  # "na" for the fastest point in the sweep.
  wall=$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.3f", b - a }')
  rows=$(awk -v t="$wall" -v r="$r" 'BEGIN { if (t > 0) printf "%.1f", r / t; else print "na" }')

  bytes=$(du -sb "$DATA".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')

  run_out=$("$BIN" run "$ENGINE" -P workloads/workloadc "${ARGS[@]}" \
    -p recordcount="$r" -p operationcount="$OPS" -p threadcount=1 2>&1 \
    | grep -E '^READ ' | tail -1)

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$ENGINE" "$r" "$BATCH" "$wall" "$rows" \
    "$(field ', 50th(us)' "$load_out")" "$bytes" \
    "$(field 'OPS' "$run_out")" "$(field ', 50th(us)' "$run_out")" | tee -a "$OUT"
done

reset_data
