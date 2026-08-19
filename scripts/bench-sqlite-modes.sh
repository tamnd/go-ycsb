#!/usr/bin/env bash
# Run the YCSB core workloads against SQLite at several durability and
# journal settings, so the cost of each setting is visible instead of
# being folded into one number.
#
# SQLite is the only engine in this set where the durability knob is both
# well known and cheap to sweep, which makes it the reference for what
# the knob is worth. Every other engine is run at its fastest setting and
# quoted against the fastest SQLite row.
#
# usage: scripts/bench-sqlite-modes.sh [recordcount] [threadcount]

set -euo pipefail

cd "$(dirname "$0")/.."

RECORDS="${1:-10000}"
THREADS="${2:-1}"

# Nothing lands in /tmp by default. On gamingpc the benchmark runs inside
# WSL, WSL tears the VM down when it goes idle, and /tmp is tmpfs, so a
# result written there is gone by the time it is collected. Ask me how I
# know. Everything goes under the repo instead, which is on a real disk.
WORK="${WORK:-$PWD/.bench}"
mkdir -p "$WORK"

BIN="${BIN:-$WORK/ycsb}"
DB="$WORK/ycsb-sqlite-modes.db"
OUT="${OUT:-$WORK/sqlite-modes.tsv}"

if [ ! -x "$BIN" ]; then
  echo "building $BIN"
  go build -tags sqlite -o "$BIN" ./cmd/go-ycsb
fi

# journal_mode:synchronous. WAL+OFF is the fastest configuration SQLite
# has that still writes to a file. DELETE+FULL is the conservative
# rollback journal default a user gets with no tuning at all.
MODES="WAL:OFF WAL:NORMAL WAL:FULL DELETE:FULL MEMORY:OFF"
WORKLOADS="a b c d e f"

# A number without its conditions is not a result. The stamp goes in the
# same file so the two cannot be separated by being copied somewhere.
{
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || sysctl -n machdep.cpu.brand_string 2>/dev/null)"
  echo "# cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu)"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime)"
  echo "# records: $RECORDS threads: $THREADS"
  echo "# workdir: $WORK"
  echo "# git: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
} | tee "$OUT"

# field pulls one metric out of a go-ycsb summary line.
#
# It takes the last match rather than the first because a phase slow
# enough to cross the reporting interval emits a progress line every ten
# seconds, and the last one is the summary. The DELETE/FULL sqlite runs
# are slow enough to do that, and without this the TSV came out with
# three stacked values in one cell.
field() {
  sed -n "s/.*$1: \\([0-9.]*\\).*/\\1/p" <<<"$2" | tail -1
}

printf 'journal\tsync\tworkload\tphase\tops\tp50_us\tp99_us\n' | tee -a "$OUT"

for mode in $MODES; do
  journal="${mode%%:*}"
  sync="${mode##*:}"
  for w in $WORKLOADS; do
    rm -f "$DB" "$DB"-wal "$DB"-shm "$DB"-journal

    load=$("$BIN" load sqlite -P "workloads/workload$w" \
      -p sqlite.db="$DB" -p sqlite.journalmode="$journal" \
      -p sqlite.synchronous="$sync" \
      -p recordcount="$RECORDS" -p threadcount="$THREADS" 2>&1 \
      | grep -E '^INSERT ' | tail -1 || true)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$journal" "$sync" "$w" load \
      "$(field 'OPS' "$load")" "$(field '50th(us)' "$load")" "$(field ', 99th(us)' "$load")" | tee -a "$OUT"

    run=$("$BIN" run sqlite -P "workloads/workload$w" \
      -p sqlite.db="$DB" -p sqlite.journalmode="$journal" \
      -p sqlite.synchronous="$sync" \
      -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
      -p threadcount="$THREADS" 2>&1 | grep -E '^TOTAL ' | tail -1 || true)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$journal" "$sync" "$w" run \
      "$(field 'OPS' "$run")" "$(field '50th(us)' "$run")" "$(field ', 99th(us)' "$run")" | tee -a "$OUT"
  done
done

echo
echo "wrote $OUT"
