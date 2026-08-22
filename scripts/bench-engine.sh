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
# One binary per engine, built by scripts/build-engine.sh. BIN overrides
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

BIN="${BIN:-$WORK/ycsb-$ENGINE}"
OUT="${OUT:-$WORK/$ENGINE-r$RECORDS-t$THREADS.tsv}"

# Records a loader hands over at once. One is go-ycsb's own default and
# is what every sweep so far has run at, so it stays the default here:
# it is the shape every engine can do and the only one two of them can.
# Above one the client calls BatchInsert instead of Insert, which sqlite,
# duckdb, zu and zu2 implement and ladybug, pg and neo4j do not, so a
# batched pass is a comparison between those four and not a sweep. What
# it measures is the load path rather than the engine: zu2's batch entry
# point waits for the device once for the batch instead of once a record,
# which is the wait a loader did not ask for. See tamnd/zu#377.
BATCH="${BATCH:-1}"
BATCH_ARGS=()
[ "$BATCH" -gt 1 ] && BATCH_ARGS=(-p "batch.size=$BATCH")

if [ ! -x "$BIN" ]; then
  echo "no binary at $BIN, run scripts/build-engine.sh $ENGINE first" >&2
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
  zu2)
    # index_buckets sized off the record count, and eight entries to a
    # bucket wants to stay under half full. The table does grow under
    # traffic since Z9, so this is a hint and not a requirement, it just
    # saves the load phase the doublings it would otherwise take on the
    # way up. Everything else takes the engine's default, which is the
    # async commit every other engine here is also running at.
    ENGINE_ARGS=(-p "zu2.path=$DATA.zu2" -p "zu2.index_buckets=$((RECORDS / 4 + 1))")
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
    zu2)     rm -rf "$DATA.zu2" ;;
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
  echo "# records: $RECORDS threads: $THREADS batch: $BATCH"
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

# An adapter may print what its engine left on the device. Only zu2 does
# so far, and the line is what the filesystem is holding rather than the
# file length, which is the only version of that number worth recording.
# It goes in as a comment because it is one line a phase and not a row.
storage() {  # storage <workload> <phase> <output>
  local note
  # Every line the adapter offers and not just the first. The scan plane
  # costs memory and the promotions cost writes, and both are printed
  # beside the disk number for the same reason the disk number is
  # printed beside the throughput: a result that reports one resource is
  # picking whichever one reads better.
  while IFS= read -r note; do
    [ -n "$note" ] && echo "# $1 $2: $note" | tee -a "$OUT"
  done < <(grep -E '^[a-z0-9]+ (storage|index|scan plane|promotion|recovery): ' <<<"$3")
  return 0
}

# The same question asked of every engine by the filesystem rather than
# by the engine. du counts blocks, so a file with holes punched in it by
# a compaction is counted at what it really costs and a file with a free
# list inside it is counted at what it really holds. The engines with no
# local path print nothing. This runs after the phase, with the process
# gone, and before the next workload wipes the path.
space() {  # space <workload> <phase>
  local kb
  kb="$(du -sk "$DATA".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')"
  # A phase that reported throughput and left nothing on the device is not
  # a missing measurement, it is a result, and staying quiet about it reads
  # in the file as if the engine had no local path at all. duckdb did this
  # on gamingpc for workload b and the log said nothing.
  if [ "${kb:-0}" -le 0 ]; then
    case "$ENGINE" in
      sqlite|duckdb|ladybug|zu|zu2)
        echo "# $1 $2: WRONG, $ENGINE left nothing at $DATA.*" | tee -a "$OUT"
        ;;
    esac
    return 0
  fi
  awk -v w="$1" -v p="$2" -v e="$ENGINE" -v kb="$kb" -v n="$RECORDS" \
    'BEGIN { printf "# %s %s: %s on device %.1f MiB, %.0f bytes a record\n", w, p, e, kb / 1024, kb * 1024 / n }' \
    | tee -a "$OUT"
  return 0
}

# What the engine actually holds, counted rather than assumed.
#
# It exists because of a number that did not add up. duckdb reported 8.5
# MiB on device after a load of 100000 records of about a kilobyte each,
# which is 89 bytes a record, and YCSB values are uniform draws from 52
# letters, so no compressor can go below about 71 percent of the payload.
# Either the storage line was wrong or the load was, and the harness had
# no way to say which, because it took the ops count the client reported
# as proof that the rows arrived. It is not proof: a client counts calls
# that returned without an error, and a row that never landed is a row
# nobody asked about again.
#
# So this asks the engine. Where the engine has no client on this host
# the line is left out rather than guessed at, and where it answers a
# number other than the record count the line says so loudly, because
# every other row in the file is meaningless if this one is wrong.
rows() {  # rows <workload> <phase>
  local n="" have=""
  case "$ENGINE" in
    sqlite)
      command -v sqlite3 >/dev/null 2>&1 && have=yes &&
        n=$(sqlite3 "$DATA.db" 'select count(*) from usertable' 2>/dev/null)
      ;;
    duckdb)
      command -v duckdb >/dev/null 2>&1 && have=yes &&
        n=$(duckdb "$DATA.db" -noheader -list 'select count(*) from usertable' 2>/dev/null)
      ;;
    pg)
      command -v psql >/dev/null 2>&1 && have=yes &&
        n=$(PGPASSWORD="${PGPASSWORD:-benchpass}" psql -qtAX \
              -h "${PGHOST:-127.0.0.1}" -p "${PGPORT:-55432}" \
              -U "${PGUSER:-postgres}" -d "${PGDATABASE:-ycsb}" \
              -c 'select count(*) from usertable' 2>/dev/null)
      ;;
    neo4j)
      command -v cypher-shell >/dev/null 2>&1 && have=yes &&
        n=$(cypher-shell -a "${NEO4J_URI:-bolt://127.0.0.1:7687}" \
              -u "${NEO4J_USER:-neo4j}" -p "${NEO4J_PASSWORD:-benchpass}" \
              --format plain 'match (r:usertable) return count(r)' 2>/dev/null | tail -1)
      ;;
    zu|zu2)
      # These print their own index occupancy on the storage line, which
      # is the same question asked of the plane that answers reads.
      return 0
      ;;
  esac
  n="${n//[[:space:]]/}"
  # A client that is present and answered nothing is a third case, and it
  # used to be silent, which made it look like the host had no client. It
  # is the case that matters most, because the count is there to catch a
  # load that reported success and wrote nothing.
  case "$n" in
    '' | *[!0-9]*)
      [ -n "$have" ] && echo "# $1 $2: the $ENGINE client would not answer the row count" | tee -a "$OUT"
      return 0
      ;;
  esac
  if [ "$n" -eq "$RECORDS" ]; then
    echo "# $1 $2: $ENGINE holds $n rows, which is the record count" | tee -a "$OUT"
  else
    echo "# $1 $2: WRONG, $ENGINE holds $n rows and the load asked for $RECORDS" | tee -a "$OUT"
  fi
  return 0
}

# Nothing is skipped any more. zu2 used to skip workload E because it had
# no ordered iteration to hand a range scan, and since tamnd/zu#548 it has
# a scan plane, which is a key ordered structure beside the hash index. It
# costs memory, so it is on for workload E and off for the rest, and the
# storage line then says what it costs where it is on and stays quiet
# where it is not.
SKIP=""

for w in a b c d e f; do
  if [[ " $SKIP " == *" $w "* ]]; then
    echo "# workload $w skipped: $ENGINE does not support scan" | tee -a "$OUT"
    continue
  fi

  # Per workload arguments, appended after ENGINE_ARGS so they win.
  EXTRA=()
  if [ "$ENGINE" = zu2 ] && [ "$w" = e ]; then
    EXTRA=(-p "zu2.ordered=true")
  fi

  reset_data

  raw=$("$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}" "${BATCH_ARGS[@]+"${BATCH_ARGS[@]}"}" \
    -p recordcount="$RECORDS" -p threadcount="$THREADS" 2>&1)
  load_out=$(grep -E '^(INSERT|TOTAL) ' <<<"$raw")
  if [ -z "$load_out" ]; then
    echo "# load failed for workload $w, see $WORK/$ENGINE-fail-$w.log" | tee -a "$OUT"
    "$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}" "${BATCH_ARGS[@]+"${BATCH_ARGS[@]}"}" \
      -p recordcount="$RECORDS" -p threadcount="$THREADS" > "$WORK/$ENGINE-fail-$w.log" 2>&1
    continue
  fi
  emit "$w" load "$load_out"
  storage "$w" load "$raw"
  space "$w" load
  rows "$w" load

  raw=$("$BIN" run "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" \
    -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
    -p threadcount="$THREADS" 2>&1)
  run_out=$(grep -E '^(READ|UPDATE|INSERT|SCAN|READ_MODIFY_WRITE|TOTAL) ' <<<"$raw")
  # The same courtesy the load phase gets. A run that produced nothing
  # used to emit a row of empty columns and throw its output away, which
  # left a reader with a blank line and no way to find out why, and that
  # is exactly the case somebody wants the output for.
  if [ -z "$run_out" ]; then
    echo "# run produced nothing for workload $w, see $WORK/$ENGINE-runfail-$w.log" | tee -a "$OUT"
    printf '%s\n' "$raw" > "$WORK/$ENGINE-runfail-$w.log"
    space "$w" run
    continue
  fi
  emit "$w" run "$run_out"
  storage "$w" run "$raw"
  space "$w" run
done

reset_data
echo
echo "wrote $OUT"
