#!/usr/bin/env bash
# Sweep zu2 across its memory bound and print what each setting costs.
#
# The memory column in bench-engine.sh said zu2 holds its whole database
# resident: 230 MiB of engine memory against sqlite's 20 at 200000
# records, and the gap widens with the database (tamnd/zu#636). The cause
# turned out to be small. zu2 heap allocates every 4 MiB log page and
# frees one only when eviction drops it, eviction is off unless a bound
# is set, and the bound never reached the C API so no host could set it.
# It reaches it now, as zu2.memory_pages.
#
# A bound existing is not the same as a bound being free. Below the
# bound a read misses in memory and preads, so the question this script
# answers is what that trade looks like: throughput and peak resident set
# against the number of pages kept, with sqlite measured on the same host
# in the same minutes as the line to beat.
#
# Read the output as a curve and not as a set of rows. What matters is
# where the throughput starts falling off relative to how much memory was
# given back, and whether there is a setting that is under sqlite's
# resident set while still ahead of it on operations. That point, if it
# exists, is the answer to the resource half of the milestone.
#
# Five is the smallest setting that does anything. The engine clamps the
# bound up to one more than its mutable window, which is four pages by
# default, because a window wider than what is kept would evict a page
# still being appended to.
#
# usage: scripts/bench-memory.sh [records] [threads] [pages...]
#
#   scripts/bench-memory.sh
#   scripts/bench-memory.sh 200000 4 5 8 16 32 64 0

set -uo pipefail

cd "$(dirname "$0")/.."

RECORDS="${1:-200000}"
THREADS="${2:-4}"
shift 2 2>/dev/null || shift $#
PAGES=("$@")
# 5 is the floor, 0 is the engine's default of no bound at all, and the
# points between are doublings so the curve has a shape rather than two
# ends. At 4 MiB a page that is 20 MiB up to 1 GiB.
[ ${#PAGES[@]} -eq 0 ] && PAGES=(5 8 16 32 64 128 256 0)

WORKLOAD="${WORKLOAD:-workloadc}"
WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

# The same lock every other sweep takes. Two benchmarks sharing $WORK
# measure each other.
if command -v flock >/dev/null 2>&1; then
  exec 9>"$WORK/.bench.lock"
  if ! flock -n 9; then
    echo "# another benchmark holds $WORK/.bench.lock, waiting for it" >&2
    flock 9
  fi
else
  echo "# no flock on this host, so nothing is stopping two runs sharing $WORK" >&2
fi

OUT="${OUT:-$WORK/zu2-memory-r$RECORDS-t$THREADS.tsv}"

TIME_BIN=""
if /usr/bin/time -v true >/dev/null 2>&1; then
  TIME_BIN=/usr/bin/time
fi

field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | head -1; }
last_per_op() { awk '{ seen[$1] = $0 } END { for (op in seen) print seen[op] }' | sort; }

{
  echo "# workload: $WORKLOAD records: $RECORDS threads: $THREADS"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
  echo "# cores: $(nproc 2>/dev/null || echo unknown)"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null)"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  [ -z "$TIME_BIN" ] && echo "# no /usr/bin/time -v here, so there is no memory column in this file"
  echo "# pages 0 means the engine default, which is no bound"
} | tee "$OUT"

printf 'engine\tpages\tmem_mib\tops\tavg_us\tp99_us\trss_mib\tdisk_mib\n' | tee -a "$OUT"

# One row. $1 is the engine, $2 the page setting, and the remaining
# arguments are whatever else that engine needs told.
row() {
  local engine="$1" pages="$2"; shift 2
  local data="$WORK/mem-$engine"
  case "$engine" in
    sqlite) rm -f "$data.db" "$data.db-wal" "$data.db-shm" "$data.db-journal" ;;
    zu2)    rm -rf "$data.zu2" "$data.zu2.ckpt" "$data.zu2.ckpt.writing" \
                   "$data.zu2.cold" "$data.zu2.relink" ;;
  esac

  local bin="$WORK/ycsb-$engine"
  if [ ! -x "$bin" ]; then
    echo "no binary at $bin, run scripts/build-engine.sh $engine first" >&2
    return 1
  fi

  "$bin" load "$engine" -P "workloads/$WORKLOAD" "$@" \
    -p recordcount="$RECORDS" -p threadcount="$THREADS" >/dev/null 2>&1

  local raw
  if [ -n "$TIME_BIN" ]; then
    raw="$($TIME_BIN -v "$bin" run "$engine" -P "workloads/$WORKLOAD" "$@" \
      -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
      -p threadcount="$THREADS" 2>&1)"
  else
    raw="$("$bin" run "$engine" -P "workloads/$WORKLOAD" "$@" \
      -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
      -p threadcount="$THREADS" 2>&1)"
  fi

  local line
  line="$(grep -E '^READ ' <<<"$raw" | last_per_op | head -1)"
  local kb
  kb="$(sed -n 's/.*Maximum resident set size (kbytes): \([0-9]*\)/\1/p' <<<"$raw" | tail -1)"
  local disk
  disk="$(du -sk "$data".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')"

  awk -v e="$engine" -v p="$pages" -v ops="$(field 'OPS' "$line")" \
      -v avg="$(field 'Avg(us)' "$line")" -v p99="$(field ', 99th(us)' "$line")" \
      -v kb="${kb:-0}" -v disk="${disk:-0}" \
      'BEGIN {
         # p is a string here, not a number. awk only treats a -v
         # assignment as numeric when it looks numeric, so the "na" on
         # the sqlite row compares as a string and has to be caught
         # first. Left as a numeric compare it fell through to the
         # sprintf and printed a memory setting of 0 for the one engine
         # whose setting this sweep does not touch.
         if (p == "na") mem = "n/a"
         else if (p + 0 == 0) mem = "none"
         else mem = sprintf("%d", p * 4)
         printf "%s\t%s\t%s\t%s\t%s\t%s\t%.1f\t%.1f\n",
                e, p, mem, ops, avg, p99, kb / 1024, disk / 1024
       }' | tee -a "$OUT"
}

for p in "${PAGES[@]}"; do
  ARGS=(-p "zu2.path=$WORK/mem-zu2.zu2" -p "zu2.index_buckets=$((RECORDS / 4 + 1))")
  [ "$p" -ne 0 ] && ARGS+=(-p "zu2.memory_pages=$p")
  row zu2 "$p" "${ARGS[@]}"
done

# sqlite last and once, as the flat line the zu2 curve is read against.
# Its own cache is bounded by default and this sweep does not move it:
# the question is not what sqlite does at a different setting, it is
# where zu2 has to be to match what sqlite does at its own.
row sqlite na \
  -p "sqlite.db=$WORK/mem-sqlite.db" -p sqlite.journalmode=WAL -p sqlite.synchronous=OFF

rm -rf "$WORK/mem-zu2".* "$WORK/mem-sqlite".*
