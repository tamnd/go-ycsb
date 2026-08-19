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
BIN="${BIN:-/tmp/ycsb}"
DB=/tmp/ycsb-sqlite-modes.db
OUT="${OUT:-/tmp/sqlite-modes.tsv}"

if [ ! -x "$BIN" ]; then
  echo "building $BIN"
  go build -tags sqlite -o "$BIN" ./cmd/go-ycsb
fi

# journal_mode:synchronous. WAL+OFF is the fastest configuration SQLite
# has that still writes to a file. DELETE+FULL is the conservative
# rollback journal default a user gets with no tuning at all.
MODES="WAL:OFF WAL:NORMAL WAL:FULL DELETE:FULL MEMORY:OFF"
WORKLOADS="a b c d e f"

printf 'journal\tsync\tworkload\tphase\tops\tp50_us\tp99_us\n' | tee "$OUT"

for mode in $MODES; do
  journal="${mode%%:*}"
  sync="${mode##*:}"
  for w in $WORKLOADS; do
    rm -f "$DB" "$DB"-wal "$DB"-shm "$DB"-journal

    load=$("$BIN" load sqlite -P "workloads/workload$w" \
      -p sqlite.db="$DB" -p sqlite.journalmode="$journal" \
      -p sqlite.synchronous="$sync" \
      -p recordcount="$RECORDS" -p threadcount="$THREADS" 2>&1 \
      | grep -E '^INSERT ' || true)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$journal" "$sync" "$w" load \
      "$(sed -n 's/.*OPS: \([0-9.]*\).*/\1/p' <<<"$load")" \
      "$(sed -n 's/.*50th(us): \([0-9]*\).*/\1/p' <<<"$load")" \
      "$(sed -n 's/.*, 99th(us): \([0-9]*\).*/\1/p' <<<"$load")" | tee -a "$OUT"

    run=$("$BIN" run sqlite -P "workloads/workload$w" \
      -p sqlite.db="$DB" -p sqlite.journalmode="$journal" \
      -p sqlite.synchronous="$sync" \
      -p recordcount="$RECORDS" -p operationcount="$RECORDS" \
      -p threadcount="$THREADS" 2>&1 | grep -E '^TOTAL ' || true)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$journal" "$sync" "$w" run \
      "$(sed -n 's/.*OPS: \([0-9.]*\).*/\1/p' <<<"$run")" \
      "$(sed -n 's/.*50th(us): \([0-9]*\).*/\1/p' <<<"$run")" \
      "$(sed -n 's/.*, 99th(us): \([0-9]*\).*/\1/p' <<<"$run")" | tee -a "$OUT"
  done
done

echo
echo "wrote $OUT"
