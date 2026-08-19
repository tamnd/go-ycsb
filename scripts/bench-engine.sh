#!/usr/bin/env bash
# Run the YCSB core workloads A to F against one engine and write a TSV.
#
# Every engine goes through this same script so the shape of the run is
# identical and only the engine differs. Engine specific settings live in
# the case block below and nowhere else.
#
# usage: scripts/bench-engine.sh <engine> [recordcount] [threadcount]
#
#   scripts/bench-engine.sh sqlite 10000 1
#   scripts/bench-engine.sh duckdb 10000 1
#   scripts/bench-engine.sh pg 10000 1
#
# The binary has to be built with the matching tag first. BIN overrides
# where it is looked for.

set -uo pipefail

cd "$(dirname "$0")/.."

ENGINE="${1:?usage: bench-engine.sh <engine> [records] [threads]}"
RECORDS="${2:-10000}"
THREADS="${3:-1}"

# Not /tmp. On gamingpc this runs inside WSL, WSL tears the VM down when
# it goes idle, and /tmp is tmpfs, so results written there vanish.
WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

BIN="${BIN:-$WORK/ycsb}"
OUT="${OUT:-$WORK/$ENGINE-r$RECORDS-t$THREADS.tsv}"

if [ ! -x "$BIN" ]; then
  echo "no binary at $BIN, build it with the tag for $ENGINE" >&2
  exit 1
fi

# Where each engine keeps its data, and anything it needs told. Engines
# run at their fastest setting, uniformly, so each number is a throughput
# ceiling rather than a durability statement.
DATA="$WORK/data-$ENGINE"
ENGINE_ARGS=()
case "$ENGINE" in
  sqlite)
    ENGINE_ARGS=(-p "sqlite.db=$DATA.db" -p sqlite.journalmode=WAL -p sqlite.synchronous=OFF)
    ;;
  duckdb)
    ENGINE_ARGS=(-p "duckdb.dbpath=$DATA.db")
    ;;
  pg)
    # 55432 rather than 5432 because a host that already runs PostgreSQL
    # for something else should not have to stop it to be measured, and
    # measuring somebody else's tuned instance by accident is worse.
    ENGINE_ARGS=(-p pg.host="${PGHOST:-127.0.0.1}" -p pg.port="${PGPORT:-55432}"
                 -p pg.user="${PGUSER:-postgres}" -p pg.password="${PGPASSWORD:-benchpass}"
                 -p pg.db="${PGDATABASE:-ycsb}" -p pg.sslmode=disable)
    ;;
  neo4j)
    ENGINE_ARGS=(-p neo4j.uri="${NEO4J_URI:-bolt://127.0.0.1:7687}"
                 -p neo4j.username="${NEO4J_USER:-neo4j}"
                 -p neo4j.password="${NEO4J_PASSWORD:-benchpass}")
    ;;
  ladybug)
    ENGINE_ARGS=(-p "ladybug.dbpath=$DATA.lbug")
    ;;
  zu)
    ENGINE_ARGS=(-p "zu.dbpath=$DATA.zu1")
    ;;
esac

# Wipe whatever the previous workload left behind. Only paths this script
# chose are removed, never a path handed in from outside.
reset_data() {
  case "$ENGINE" in
    sqlite)  rm -f "$DATA.db" "$DATA.db-wal" "$DATA.db-shm" "$DATA.db-journal" ;;
    duckdb)  rm -rf "$DATA.db" "$DATA.db.wal" ;;
    ladybug) rm -rf "$DATA.lbug" ;;
    zu)      rm -rf "$DATA.zu1" ;;
    pg|neo4j) : ;;  # nothing on this side, see LOAD_ARGS below
  esac
}

# The file backed engines start each workload from an empty path, so the
# load phase is a load into nothing. The two server side engines have no
# path to delete, so they get told to drop instead. dropdata defaults to
# false, and without this the second workload would be loading on top of
# the first and every insert after A would be a duplicate key.
LOAD_ARGS=()
case "$ENGINE" in
  pg|neo4j) LOAD_ARGS=(-p dropdata=true) ;;
esac

field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | head -1; }

{
  echo "# engine: $ENGINE"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || sysctl -n machdep.cpu.brand_string 2>/dev/null)"
  echo "# cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu)"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime)"
  echo "# records: $RECORDS threads: $THREADS"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} | tee "$OUT"

printf 'engine\tworkload\tphase\top\tops\tavg_us\tp50_us\tp95_us\tp99_us\n' | tee -a "$OUT"

# go-ycsb reports progress while a phase runs, and those lines carry the
# same INSERT or READ prefix as the summary printed at the end. Taking
# every matching line would report a mid run snapshot as if it were the
# result, so keep only the last line seen for each operation.
last_per_op() {
  awk '{ seen[$1] = $0 } END { for (op in seen) print seen[op] }' | sort
}

emit() {  # emit <workload> <phase> <output>
  local w="$1" phase="$2" out="$3" line op
  out="$(last_per_op <<<"$out")"
  while IFS= read -r line; do
    op="${line%% *}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$ENGINE" "$w" "$phase" "$op" \
      "$(field 'OPS' "$line")" \
      "$(field 'Avg(us)' "$line")" \
      "$(field '50th(us)' "$line")" \
      "$(field '95th(us)' "$line")" \
      "$(field ', 99th(us)' "$line")"
  done <<<"$out" | tee -a "$OUT"
}

for w in a b c d e f; do
  reset_data

  load_out=$("$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]}" "${LOAD_ARGS[@]}" \
    -p recordcount="$RECORDS" -p threadcount="$THREADS" 2>&1 \
    | grep -E '^(INSERT|TOTAL) ')
  if [ -z "$load_out" ]; then
    echo "# load failed for workload $w, see $WORK/$ENGINE-fail-$w.log" | tee -a "$OUT"
    "$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]}" "${LOAD_ARGS[@]}" \
      -p recordcount="$RECORDS" -p threadcount="$THREADS" > "$WORK/$ENGINE-fail-$w.log" 2>&1
    continue
  fi
  emit "$w" load "$load_out"

  run_out=$("$BIN" run "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]}" \
    -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
    -p threadcount="$THREADS" 2>&1 \
    | grep -E '^(READ|UPDATE|INSERT|SCAN|READ_MODIFY_WRITE|TOTAL) ')
  emit "$w" run "$run_out"
done

reset_data
echo
echo "wrote $OUT"
