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

# One benchmark at a time on this host.
#
# It started as one per work directory, because every engine here writes
# to a fixed path under $WORK and two runs started by mistake share the
# same database: one loads while the other reads, and the numbers that
# come out are not of anything. That happened on server2 and server3 and
# it announced itself as a data integrity failure, which is the only
# reason it was noticed.
#
# Per work directory is the wrong scope and the reason is that the
# resource being fought over is not the database, it is the machine. Two
# sweeps of mine ran at once on server1 out of ~/bench/wt-go-ycsb and
# ~/bench/go-ycsb, took a lock each, waited for nothing, and put two 32
# thread benchmarks on four cores. Neither number describes an engine.
# Running loaded is fine and is the standing rule for these boxes, but
# that is somebody else's load and it is roughly steady; this is one of
# my runs measuring the other. tamnd/zu#376.
#
# So the lock is one per host and per user by default, and BENCH_LOCK
# overrides it for a caller who really does want two at once and has a
# reason. flock is not on every host, so a host without it says so and
# carries on rather than refusing to run.
mkdir -p "$WORK"
LOCK="${BENCH_LOCK:-$HOME/.go-ycsb-bench.lock}"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$LOCK"
  if ! flock -n 9; then
    echo "# another benchmark on this host holds $LOCK, waiting for it" >&2
    flock 9
  fi
else
  echo "# no flock on this host, so nothing is stopping two runs sharing $WORK" >&2
fi

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
  mongodb)
    # w=1 and no j, which is acknowledge on the primary and let the
    # journal flush on its own interval. The journal cannot be turned
    # off at all since 6.0, so this is as far as MongoDB goes, and it is
    # the same bargain sqlite makes at synchronous=OFF and pg at
    # synchronous_commit=off.
    ENGINE_ARGS=(-p mongodb.url="${MONGO_URL:-mongodb://127.0.0.1:57017/ycsb?w=1}")
    ;;
  redis|valkey|keydb|garnet)
    # Two engines out of one adapter. Valkey is the fork the Linux
    # Foundation took on when Redis changed its licence, it speaks the
    # same wire protocol, and go-redis talks to it without knowing the
    # difference, so the only thing that separates them here is which
    # port the client dials. They get separate ports and separate
    # containers so both can be up at once and neither is measured on
    # the other's data.
    #
    # datatype=hash rather than the adapter's default of string. On
    # string the whole record is one JSON document, so a read is a GET
    # plus a JSON unmarshal on the client and an update is a read,
    # modify and write. On hash the record is a Redis hash and a read is
    # one HMGET, which is the shape YCSB has meant by a Redis record
    # since the original Java harness. String came out a little ahead in
    # a first pass over a Docker bridge on macOS, which is a measurement
    # of the bridge, so this is being taken again on the Linux hosts and
    # both modes go in the table the way sqlite's modes do.
    case "$ENGINE" in
      redis)  port=56379 ;;
      valkey) port=56380 ;;
      keydb)  port=56381 ;;
      garnet) port=56382 ;;
    esac
    ENGINE_ARGS=(-p redis.addr="${REDIS_ADDR:-127.0.0.1:$port}"
                 -p redis.datatype="${REDIS_DATATYPE:-hash}")
    ;;
  badger)
    # Badger's value log is the thing that separates it from every other
    # LSM here: values above a threshold live in their own log and the
    # tree carries a pointer, which is the WiscKey design. A YCSB record
    # at ten fields of a hundred bytes is about a kilobyte encoded, so it
    # sits right on the adapter's threshold, and that is the interesting
    # place to measure it rather than a number picked to keep every value
    # inline or push every value out.
    ENGINE_ARGS=(-p "badger.dir=$DATA.badger")
    ;;
  lmdb)
    # map_size is the only thing that has to be set from out here, and
    # only because the default has to cover a sweep at any record count
    # this script is given. Everything else takes the adapter's
    # defaults, which are no fsync and a writable map.
    ENGINE_ARGS=(-p "lmdb.dir=$DATA.lmdb")
    ;;
  boltdb)
    # A single mmap'd file holding one B+ tree, one writer at a time and
    # readers in MVCC snapshots, which is the oldest and simplest shape
    # in this set and the reason it is worth a row: it is the cost of a
    # page tree with no LSM and no log structure at all.
    #
    # no_sync because every engine here is measured at its fastest
    # setting and this is the only durability knob Bolt has. Left on, a
    # load is one fsync a record and ten thousand records take 78
    # seconds, which is a measurement of the disk and not of the B+
    # tree. The one thing to keep in mind reading the row is that Bolt
    # with no_sync still writes and mmaps every page, so this buys it
    # less than synchronous=OFF buys sqlite.
    ENGINE_ARGS=(-p "bolt.path=$DATA.bolt" -p "bolt.no_sync=${BOLT_NO_SYNC:-true}")
    ;;
  pebble)
    # Everything else takes the adapter's defaults, which are Pebble's
    # own except for the block cache. Eight MiB is the library default
    # and is a read off the device for nearly every miss at any record
    # count worth measuring, so the adapter raises it, the same way the
    # sqlite adapter raises the page cache and for the same reason.
    ENGINE_ARGS=(-p "pebble.dir=$DATA.pebble")
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
    # Ladybug keeps a write ahead log, a checkpoint of it and two lock
    # files beside the database, and it refuses to open a path where any
    # of them is still lying about. Removing the database alone is what
    # made workload f fail to load on gamingpc at a million records with
    # nothing but "database init failed" to show for it, on a path whose
    # database file was already gone.
    ladybug) rm -rf "$DATA.lbug" "$DATA.lbug.wal" "$DATA.lbug.wal.checkpoint" \
                   "$DATA.lbug.checkpoint.apply.lock" \
                   "$DATA.lbug.checkpoint.intent.lock" ;;
    badger)  rm -rf "$DATA.badger" ;;
    lmdb)    rm -rf "$DATA.lmdb" ;;
    boltdb)  rm -f "$DATA.bolt" ;;
    pebble)  rm -rf "$DATA.pebble" ;;
    zu)      rm -rf "$DATA.zu1" "$DATA.zu1.wal" ;;
    # A zu2 database is a log and three sidecars beside it, and removing
    # the log alone leaves the previous workload's checkpoint and cold
    # file where the next one's log lands. tamnd/zu#610. The engine takes
    # them away itself now, and they are named here as well because the
    # space column counts every path under $DATA.* and a leftover file
    # would be charged to the workload that did not write it.
    zu2)     rm -rf "$DATA.zu2" "$DATA.zu2.ckpt" "$DATA.zu2.ckpt.writing" \
                   "$DATA.zu2.cold" "$DATA.zu2.relink" ;;
    pg|neo4j|mongodb|redis|valkey|keydb|garnet) : ;;  # nothing on this side, see LOAD_ARGS below
  esac
  # The reset knows one path per engine and the engines keep more than
  # one, which is how ladybug ran a whole sweep beside an orphaned write
  # ahead log (tamnd/zu#605), how zu1 did the same silently, and how zu2
  # left a checkpoint for the next database at that path to adopt
  # (tamnd/zu#610). All three would have said so here the first time they
  # ran. A leftover is not fatal on its own so the run carries on, and
  # the line is in the TSV where a reader of the numbers will meet it.
  local left
  left="$(ls -d "$DATA".* 2>/dev/null | tr '\n' ' ')"
  [ -n "$left" ] && echo "# WRONG, the reset left $left behind" | tee -a "$OUT"
  return 0
}

# The file backed engines start each workload from an empty path, so the
# load phase is a load into nothing. The two server side engines have no
# path to delete, so they get told to drop instead. dropdata defaults to
# false, and without this the second workload would be loading on top of
# the first and every insert after A would be a duplicate key.
LOAD_ARGS=()
case "$ENGINE" in
  pg|neo4j|mongodb|redis|valkey|keydb|garnet) LOAD_ARGS=(-p dropdata=true) ;;
esac

field() { sed -n "s/.*$1: \([0-9.]*\).*/\1/p" <<<"$2" | head -1; }

# Probed here, at the top level, rather than lazily on first use. Every
# phase runs `timed` inside a command substitution, so a variable set in
# there is set in a subshell and gone by the next phase, and the version
# that cached the answer lazily re-probed and reprinted the notice once
# per phase, twelve times in a sweep.
#
# The shell keyword cannot report rss, so this wants the binary. BSD time
# takes -l and reports bytes where GNU takes -v and reports kbytes; the
# benchmark hosts are Linux and WSL, so -v is what is supported and
# anything else drops the memory column rather than printing a number in
# the wrong unit.
TIME_BIN=""
if /usr/bin/time -v true >/dev/null 2>&1; then
  TIME_BIN=/usr/bin/time
fi

{
  echo "# engine: $ENGINE"
  echo "# host: $(hostname)"
  echo "# kernel: $(uname -sr)"
  echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || sysctl -n machdep.cpu.brand_string 2>/dev/null)"
  echo "# cores: $(nproc 2>/dev/null || sysctl -n hw.ncpu)"
  echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime)"
  echo "# records: $RECORDS threads: $THREADS batch: $BATCH"
  [ -n "${YCSB_EXTRA:-}" ] && echo "# extra: $YCSB_EXTRA"
  [ -z "$TIME_BIN" ] && echo "# no /usr/bin/time -v here, so there is no memory column in this file"
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
# The engine's own version, said once. sqlite and duckdb link their
# library in, so there is nothing on the host to ask and a go.mod line
# is the driver's version and not the engine's. A sweep that claims to
# run the latest of everything has to be able to show which one it ran,
# and the adapters print it now, so this lifts it into the header block
# where the host and the core count are.
SAID_VERSION=""
version() {  # version <output>
  [ -n "$SAID_VERSION" ] && return 0
  local v
  v="$(grep -m1 -E '^[a-z0-9]+ version: ' <<<"$1")"
  [ -z "$v" ] && return 0
  SAID_VERSION=yes
  echo "# $v" | tee -a "$OUT"
  return 0
}

storage() {  # storage <workload> <phase> <output>
  local note
  # Every line the adapter offers and not just the first. The scan plane
  # costs memory and the promotions cost writes, and both are printed
  # beside the disk number for the same reason the disk number is
  # printed beside the throughput: a result that reports one resource is
  # picking whichever one reads better.
  #
  # The tier line is the same argument about the row above it. How much
  # of a zu2 database has settled into the cold tier depends on how much
  # of the background schedule the run overlapped, it came out anywhere
  # from 2 to 35 percent across otherwise identical runs, and a latency
  # compared across two runs that settled differently is a comparison of
  # two storage layouts (tamnd/zu#600).
  #
  # `scan rows` is the harness's own line rather than an adapter's, and
  # it is here because a scan that returns nothing is the fastest scan in
  # any sweep and raises no error (tamnd/zu#560). A mean well under the
  # asked for length is a row to throw away.
  while IFS= read -r note; do
    [ -n "$note" ] && echo "# $1 $2: $note" | tee -a "$OUT"
  done < <(grep -E '^([a-z0-9]+ (storage|index|tier|scan plane|promotion|recovery)|scan rows): ' <<<"$3")
  return 0
}

# One way to reach whichever of the two servers this run is measuring.
# redis-cli on the host if there is one, and the container's own
# otherwise, which is the same fallback the mongodb row count uses and
# for the same reason: the benchmark hosts have psql and sqlite3 and none
# of them has redis-cli. Either way this is the server being asked rather
# than the client being believed.
#
# KeyDB ships keydb-cli in its own image and Garnet ships no client at
# all, so the fallback for those two is a throwaway redis-cli container
# sharing the server's network namespace. That is one container start
# per question, which is why it is only the fallback and only used for
# the space and row count lines, never inside a timed phase.
redis_cli() {
  local port container
  case "$ENGINE" in
    valkey) port=56380; container=valkey-ycsb ;;
    keydb)  port=56381; container=keydb-ycsb ;;
    garnet) port=56382; container=garnet-ycsb ;;
    *)      port=56379; container=redis-ycsb ;;
  esac
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -h 127.0.0.1 -p "$port" "$@"
  elif [ "$ENGINE" = keydb ]; then
    docker exec "$container" keydb-cli "$@"
  elif [ "$ENGINE" = garnet ]; then
    docker run --rm --network "container:$container" redis:latest redis-cli "$@"
  else
    docker exec "$container" redis-cli "$@"
  fi
}

# The same question asked of every engine by the filesystem rather than
# by the engine. du counts blocks, so a file with holes punched in it by
# a compaction is counted at what it really costs and a file with a free
# list inside it is counted at what it really holds. The engines with no
# local path print nothing. This runs after the phase, with the process
# gone, and before the next workload wipes the path.
space() {  # space <workload> <phase>
  local kb

  # The three engines that keep their data inside a container have no
  # $DATA path to measure, so they used to print a sentence saying so and
  # drop out of the storage comparison entirely. That left the compact
  # storage question answered for four engines out of seven. Each one can
  # be asked directly, and what each one answers with is not quite the
  # same thing, so the line says which:
  #
  #   pg       pg_total_relation_size, the table with its indexes and toast
  #   mongodb  the collection's storage size with its indexes
  #   neo4j    du of the database directory, which is the store, the
  #            indexes and the transaction logs together
  #
  # None of them is directly comparable to sqlite's file to the byte, and
  # all three are the closest thing that engine has to one.
  # Redis and Valkey keep the whole dataset in memory and are configured
  # here with persistence off, so there is nothing on the device to
  # measure and du would report a zero that reads like a failed load.
  # What they have instead is used_memory, which is the allocator's
  # account of the dataset, and used_memory_rss, which is what the kernel
  # has given the server. Both go in, because the gap between them is
  # fragmentation and an in memory engine compared on the smaller of the
  # two is being flattered.
  case "$ENGINE" in
    redis|valkey|keydb|garnet)
      local ru rr
      ru=$(redis_cli info memory 2>/dev/null | sed -n 's/^used_memory:\([0-9]*\).*/\1/p' | tr -d '[:space:]')
      rr=$(redis_cli info memory 2>/dev/null | sed -n 's/^used_memory_rss:\([0-9]*\).*/\1/p' | tr -d '[:space:]')
      if [[ "$ru" =~ ^[0-9]+$ ]] && [ "$ru" -gt 0 ]; then
        awk -v w="$1" -v p="$2" -v e="$ENGINE" -v b="$ru" -v n="$RECORDS" \
          'BEGIN { printf "# %s %s: %s in memory %.1f MiB, %.0f bytes a record (dataset, nothing on the device)\n", w, p, e, b / 1048576, b / n }' \
          | tee -a "$OUT"
        [[ "$rr" =~ ^[0-9]+$ ]] && awk -v w="$1" -v p="$2" -v e="$ENGINE" -v b="$rr" \
          'BEGIN { printf "# %s %s: %s server rss %.1f MiB\n", w, p, e, b / 1048576 }' | tee -a "$OUT"
      else
        echo "# $1 $2: $ENGINE would not say how much memory it is using" | tee -a "$OUT"
      fi
      return 0 ;;
  esac

  case "$ENGINE" in
    pg|mongodb|neo4j)
      # Each one is asked to get what it is holding onto out to the device
      # first. Without that, what comes back is whatever has landed so
      # far: WiredTiger's storageSize is the size of the file on disk and
      # it does not move until a checkpoint, which by default is a minute
      # away, so a collection with 2000 records freshly loaded reported
      # 8000 bytes and read as 4 bytes a record. The engines measured with
      # du are read after their writes have gone down, and this is the
      # same thing asked of the ones that answer for themselves.
      local bytes="" what="" parts=""
      case "$ENGINE" in
        pg)
          docker exec pg-ycsb psql -U postgres -d ycsb -tAc 'checkpoint' >/dev/null 2>&1
          bytes=$(docker exec pg-ycsb psql -U postgres -d ycsb -tAc \
                    "select pg_total_relation_size('usertable')" 2>/dev/null | tr -d '[:space:]')
          parts=$(docker exec pg-ycsb psql -U postgres -d ycsb -tAF' ' -c \
                    "select 'table', pg_table_size('usertable') union all select 'indexes', pg_indexes_size('usertable')" 2>/dev/null)
          what="table with indexes and toast" ;;
        mongodb)
          docker exec mongo-ycsb mongosh --quiet --eval 'db.adminCommand({fsync:1})' >/dev/null 2>&1
          parts=$(docker exec mongo-ycsb mongosh "mongodb://127.0.0.1:27017/ycsb" --quiet \
                    --eval "const s = db.getCollection('usertable').aggregate([{\$collStats:{storageStats:{}}}]).toArray()[0].storageStats; print('collection ' + s.storageSize); print('indexes ' + s.totalIndexSize)" 2>/dev/null)
          bytes=$(awk '{ s += $2 } END { print s + 0 }' <<<"$parts")
          what="collection with indexes" ;;
        neo4j)
          # The transaction log directory is preallocated to a fixed size
          # and does not track the data at all: a thousand records came
          # to 10.9 MiB of store beside 514 MiB of log. The total is what
          # is on the device and stays the headline, the same as every
          # other engine here, and the split below is what stops it being
          # read as half a megabyte a record.
          parts=$(docker exec neo4j-ycsb du -sk /data/databases /data/transactions 2>/dev/null \
                    | awk '{ printf "%s %d\n", ($2 ~ /transactions/ ? "transactionlogs" : "store"), $1 * 1024 }')
          bytes=$(awk '{ s += $2 } END { print s + 0 }' <<<"$parts")
          what="store, indexes and transaction logs" ;;
      esac
      if [[ "$bytes" =~ ^[0-9]+$ ]] && [ "$bytes" -gt 0 ]; then
        awk -v w="$1" -v p="$2" -v e="$ENGINE" -v b="$bytes" -v n="$RECORDS" -v what="$what" \
          'BEGIN { printf "# %s %s: %s on device %.1f MiB, %.0f bytes a record (%s)\n", w, p, e, b / 1048576, b / n, what }' \
          | tee -a "$OUT"
        # The same breakdown the local engines get per file, because a
        # single figure cannot say which part of it moved.
        awk -v w="$1" -v p="$2" -v e="$ENGINE" \
          'NF == 2 && $2 ~ /^[0-9]+$/ { printf "# %s %s: %s %s %.1f MiB\n", w, p, e, $1, $2 / 1048576 }' \
          <<<"$parts" | tee -a "$OUT"
      else
        echo "# $1 $2: $ENGINE would not say how much space it is using" | tee -a "$OUT"
      fi
      return 0 ;;
  esac

  kb="$(du -sk "$DATA".* 2>/dev/null | awk '{ s += $1 } END { print s + 0 }')"
  # A phase that reported throughput and left nothing on the device is not
  # a missing measurement, it is a result, and staying quiet about it reads
  # in the file as if the engine had no local path at all. duckdb did this
  # on gamingpc for workload b and the log said nothing.
  if [ "${kb:-0}" -le 0 ]; then
    case "$ENGINE" in
      sqlite|duckdb|ladybug|badger|pebble|lmdb|boltdb|zu|zu2)
        echo "# $1 $2: WRONG, $ENGINE left nothing at $DATA.*" | tee -a "$OUT"
        ;;
    esac
    return 0
  fi
  awk -v w="$1" -v p="$2" -v e="$ENGINE" -v kb="$kb" -v n="$RECORDS" \
    'BEGIN { printf "# %s %s: %s on device %.1f MiB, %.0f bytes a record\n", w, p, e, kb / 1024, kb * 1024 / n }' \
    | tee -a "$OUT"
  # And the same total broken out per file, because a single figure that
  # disagrees with the engine's own cannot say which file the difference
  # is in. #631 is 27.9 MiB of disagreement at a million records and it
  # has not reproduced in process, so the next time it appears the log
  # should already carry enough to name it.
  du -k "$DATA".* 2>/dev/null | sort -k2 | awk -v w="$1" -v p="$2" -v e="$ENGINE" \
    '{ printf "# %s %s: %s file %s %.1f MiB\n", w, p, e, $2, $1 / 1024 }' \
    | tee -a "$OUT"
  return 0
}

# Peak resident memory, asked of the kernel rather than of the engine.
#
# The storage lines an adapter prints are the engine's own account of
# itself, and only zu2 prints them, so they can rank zu2 against zu2 and
# nothing else. This is the same question put to every engine in the same
# words, and it is the third resource: the sweep has had a disk column
# since Y3 and a result that reports one resource is picking whichever
# one reads better.
#
# It costs nothing to collect. Every phase already captures the process's
# combined output, and GNU time writes its report to stderr, tab
# indented, where none of the row patterns in this file can match it.
#
# pg and neo4j are the exception and get a sentence instead of a number.
# Their data lives in a server process this script never started, so what
# time sees is the client, and a client's footprint printed in a memory
# column beside four embedded engines is worse than a blank.
# Proportional set size, sampled while the phase runs.
#
# Maximum resident set size counts a shared file backed page once per
# mapping and not once per page, so an engine that maps the same file
# from several places has every resident page of it counted several
# times. sqlite does exactly that: the adapter sets PRAGMA mmap_size per
# connection and opens one connection per thread, so at 32 threads the
# database is mapped 32 times and a 1.3 GiB database reported a 34.7 GiB
# peak. That is not a leak and it is not a rival losing, it is the
# column being wrong, and it was wrong in our favour, which is the worst
# direction for a number to be wrong in. tamnd/zu#695.
#
# Pss divides each page by the number of mappings that hold it, so a
# page mapped 32 times inside one process contributes its full size once
# across those 32 mappings. Anonymous is the part that is not backed by
# a file at all, which is the heap and the stacks and is the number to
# look at when asking what an engine costs beyond the page cache it
# shares with the kernel.
#
# Sampled rather than read at the end, because smaps_rollup only exists
# while the process does. A tenth of a second is far finer than the
# phases here, which run for seconds at least, and the sampler costs one
# read of one small file per tick.
#
# Linux only. macOS has no smaps_rollup and no equivalent that is cheap
# to sample, so there the memory line stays the RSS one it always was
# and says so.
PSS_FILE="$WORK/.pss-$ENGINE"

pss_sample() {
  local pid k v pss anon maxp=0 maxa=0
  while :; do
    # By process name and not by command line. /usr/bin/time carries the
    # binary's path in its own arguments, so a -f match finds the wrapper
    # as well as the process, and the wrapper's footprint is not the
    # measurement.
    pid="$(pgrep -x "ycsb-$ENGINE" 2>/dev/null | head -1)"
    if [ -n "$pid" ] && [ -r "/proc/$pid/smaps_rollup" ]; then
      pss=0; anon=0
      while read -r k v _; do
        case "$k" in
          Pss:)       pss="$v" ;;
          Anonymous:) anon="$v" ;;
        esac
      done < "/proc/$pid/smaps_rollup"
      [ "${pss:-0}" -gt "$maxp" ] && maxp="$pss"
      [ "${anon:-0}" -gt "$maxa" ] && maxa="$anon"
      printf '%s %s\n' "$maxp" "$maxa" > "$PSS_FILE"
    fi
    sleep 0.1
  done
}

timed() {  # timed <argv...>
  case "$ENGINE" in
    pg|neo4j|mongodb|redis|valkey|keydb|garnet) "$@" ; return ;;
  esac

  local sampler="" rc=0
  rm -f "$PSS_FILE"
  if [ -r /proc/self/smaps_rollup ]; then
    # Output redirected away from the caller's. timed runs inside a
    # command substitution, and a background job that keeps the write
    # end of that pipe open holds the substitution open with it, so a
    # sampler inheriting stdout hangs the phase rather than measuring
    # it.
    pss_sample >/dev/null 2>&1 &
    sampler=$!
  fi

  if [ -n "$TIME_BIN" ]; then "$TIME_BIN" -v "$@"; else "$@"; fi
  rc=$?

  # The sampler writes its running maximum every tick, so killing it
  # leaves the highest value it saw rather than losing the last one.
  [ -n "$sampler" ] && kill "$sampler" 2>/dev/null
  return $rc
}

maxrss() {  # maxrss <workload> <phase> <output>
  case "$ENGINE" in
    pg|neo4j|mongodb|redis|valkey|keydb|garnet)
      echo "# $1 $2: $ENGINE keeps its data in a server process, no memory figure from this side" \
        | tee -a "$OUT"
      return 0
      ;;
  esac
  local kb
  kb="$(sed -n 's/.*Maximum resident set size (kbytes): \([0-9]*\)/\1/p' <<<"${3:-}" | tail -1)"
  [ -z "$kb" ] && return 0
  # A high water mark for the whole process, so the harness's own
  # allocation is in it. That floor is the same for every engine at a
  # given workload and record count, which makes the column fair for
  # ranking engines against each other and wrong for any absolute claim
  # about what one of them costs on its own.
  awk -v w="$1" -v p="$2" -v e="$ENGINE" -v kb="$kb" -v n="$RECORDS" \
    'BEGIN { printf "# %s %s: %s peak rss %.1f MiB, %.0f bytes a record\n", w, p, e, kb / 1024, kb * 1024 / n }' \
    | tee -a "$OUT"

  # And the same phase measured the way #695 says it has to be. Pss is
  # the line to compare engines on. RSS stays above it rather than being
  # replaced, because the two disagreeing is itself the signal that an
  # engine maps something more than once, and a reader of an old file
  # needs the old column to compare against.
  local pss anon
  if [ -s "$PSS_FILE" ]; then
    read -r pss anon < "$PSS_FILE"
    awk -v w="$1" -v p="$2" -v e="$ENGINE" -v pss="${pss:-0}" -v anon="${anon:-0}" \
        -v kb="$kb" -v n="$RECORDS" \
      'BEGIN {
         if (pss <= 0) exit
         printf "# %s %s: %s peak pss %.1f MiB, %.0f bytes a record, anonymous %.1f MiB\n", \
           w, p, e, pss / 1024, pss * 1024 / n, anon / 1024
         if (kb > pss * 1.5)
           printf "# %s %s: %s rss is %.1fx its pss, so it maps something more than once, see tamnd/zu#695\n", \
             w, p, e, kb / pss
         # And the warning that matters more than the double counting
         # one. A resident set that is mostly page cache is not a
         # property of the engine, it is a measure of how much room the
         # kernel had on this host in these minutes. Measured: two lmdb
         # loads of the same million records on server2, forty minutes
         # apart, gave 838.2 and 1640.5 MiB of peak rss while their
         # anonymous figures were 17.6 and 17.2. Quoting either rss
         # without saying this invites a comparison that does not hold.
         if (pss > 0 && anon < pss * 0.25)
           printf "# %s %s: %s is %.0f%% page cache, so this rss is a fact about the host and not about %s, quote the anonymous figure, see tamnd/zu#695\n", \
             w, p, e, (pss - anon) * 100 / pss, e
       }' | tee -a "$OUT"
  elif [ ! -r /proc/self/smaps_rollup ]; then
    echo "# $1 $2: no smaps_rollup on this host, so the memory figure is rss and over counts shared mappings" \
      | tee -a "$OUT"
  fi
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
rows() {  # rows <workload> <phase> [output]
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
    mongodb)
      # mongosh on the host if there is one, and the container's own
      # otherwise, because the hosts that run these sweeps have psql and
      # sqlite3 installed and none of them has mongosh. Either way this
      # is the server being asked, not the client being believed.
      if command -v mongosh >/dev/null 2>&1; then
        have=yes
        n=$(mongosh "${MONGO_URL:-mongodb://127.0.0.1:57017/ycsb}" --quiet \
              --eval "db.getCollection('usertable').countDocuments({})" 2>/dev/null | tail -1)
      elif docker exec mongo-ycsb mongosh --quiet --eval 'db.version()' >/dev/null 2>&1; then
        have=yes
        n=$(docker exec mongo-ycsb mongosh "mongodb://127.0.0.1:27017/ycsb" --quiet \
              --eval "db.getCollection('usertable').countDocuments({})" 2>/dev/null | tail -1)
      fi
      ;;
    neo4j)
      command -v cypher-shell >/dev/null 2>&1 && have=yes &&
        n=$(cypher-shell -a "${NEO4J_URI:-bolt://127.0.0.1:7687}" \
              -u "${NEO4J_USER:-neo4j}" -p "${NEO4J_PASSWORD:-benchpass}" \
              --format plain 'match (r:usertable) return count(r)' 2>/dev/null | tail -1)
      ;;
    redis|valkey|keydb|garnet)
      # DBSIZE is the number of keys in the database, and with one key a
      # record and a flush before each load that is the record count.
      redis_cli ping >/dev/null 2>&1 && have=yes && n=$(redis_cli dbsize 2>/dev/null)
      ;;
    zu2)
      # No client on the host to ask, so the engine answers in its own
      # output and this reads it back out. The line is keys and not the
      # slot count on the index line, which is legitimately lower than
      # the number of keys because a displaced key lives on somebody
      # else's chain (tamnd/zu#486), and comparing that to a record
      # count reports data loss on a healthy database.
      have=yes
      n=$(sed -n 's/^zu2 rows: \([0-9]*\) keys$/\1/p' <<<"${3:-}" | tail -1)
      ;;
    zu)
      # zu1 has no such line, and it is not the engine under test.
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
# Except for Redis and Valkey, which have no ordered scan to skip with.
# Redis keys live in a hash table and SCAN walks it in whatever order the
# table happens to be in, so there is no answer to "the next fifty
# records after this key" that is the same answer another engine would
# give. The adapter says so rather than guessing, and workload E is left
# out of their rows instead of being filled with an error or, worse, with
# a fast number for a cheaper question.
case "$ENGINE" in
  redis|valkey|keydb|garnet) SKIP="e" ;;
esac

# One small self check before the sweep, because throughput is worth
# nothing if the rows are not there. YCSB's own data integrity mode
# writes a value derived from the key and the field name and compares it
# on the way back, so a wrong value, a missing field or an empty row
# stops the run instead of being reported as a fast one. duckdb spent
# several sweeps storing keys with no values at all (tamnd/zu#551) and
# nothing here noticed, and this is what would have.
#
# Ten thousand records rather than the sweep's count, since this is a
# correctness check and not a measurement, and it costs a few seconds.
verify() {
  local records=10000 raw got
  reset_data
  raw=$("$BIN" load "$ENGINE" -P workloads/workloadc "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}"     -p dataintegrity=true -p recordcount="$records" -p threadcount=1 2>&1) || true
  if ! grep -qE '^(INSERT|TOTAL) ' <<<"$raw"; then
    echo "# verify: WRONG, $ENGINE could not load the data integrity set" | tee -a "$OUT"
    return 0
  fi
  raw=$("$BIN" run "$ENGINE" -P workloads/workloadc "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}"     -p dataintegrity=true -p recordcount="$records" -p operationcount="$records" -p threadcount=1 2>&1) || true
  if grep -qE '^(READ|TOTAL) ' <<<"$raw"; then
    echo "# verify: $ENGINE gave back every field of every row it was asked for" | tee -a "$OUT"
  else
    echo "# verify: WRONG, $ENGINE failed the data integrity read" | tee -a "$OUT"
    grep -iE 'unexpected|no fields|error' <<<"$raw" | head -3 | while IFS= read -r line; do
      echo "# verify: $line" | tee -a "$OUT"
    done
  fi

  # And the same for the scan path, which is a separate check because it
  # was a separate hole: dataintegrity only ever looked at what a read
  # returned, so no value workload E handed back was examined by anything
  # for any engine. A scanned row names its own key inside its value, so
  # this checks the values, that the keys climb from the one asked for,
  # and that no row carries a field belonging to another. That last one
  # is what a driver reusing buffers or maps between rows gets wrong.
  local scan=()
  [ "$ENGINE" = zu2 ] && scan=(-p "zu2.ordered=true")
  case " $SKIP " in
    *" e "*)
      echo "# verify: $ENGINE has no scan to check" | tee -a "$OUT"
      return 0 ;;
  esac
  reset_data
  raw=$("$BIN" load "$ENGINE" -P workloads/workloade "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${scan[@]+"${scan[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}"     -p dataintegrity=true -p recordcount="$records" -p threadcount=1 2>&1) || true
  if ! grep -qE '^(INSERT|TOTAL) ' <<<"$raw"; then
    echo "# verify: WRONG, $ENGINE could not load the scan integrity set" | tee -a "$OUT"
    return 0
  fi
  raw=$("$BIN" run "$ENGINE" -P workloads/workloade "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${scan[@]+"${scan[@]}"}"     -p dataintegrity=true -p recordcount="$records" -p operationcount=$((records / 2)) -p threadcount=1 2>&1) || true
  # A run that returns no rows at all passes every value check there is
  # by having nothing to check, so the row count is part of the verdict.
  got=$(grep -oE '^scan rows: [0-9]+' <<<"$raw" | grep -oE '[0-9]+' || true)
  if grep -qE '^(SCAN|TOTAL) ' <<<"$raw" && [ "${got:-0}" -gt 0 ]; then
    echo "# verify: $ENGINE scanned $got rows and every one of them held up" | tee -a "$OUT"
  else
    echo "# verify: WRONG, $ENGINE failed the data integrity scan" | tee -a "$OUT"
    grep -iE 'unexpected|no fields|mixes rows|do not climb|before the start|carries no key|error' <<<"$raw" | head -3 | while IFS= read -r line; do
      echo "# verify: $line" | tee -a "$OUT"
    done
  fi
  return 0
}

verify

# All six by default, which is what a sweep runs. WORKLOADS narrows it,
# for the case where one workload is being re-run on its own after a fix
# rather than the whole set again: WORKLOADS="b" scripts/bench-engine.sh
# sqlite. A narrowed run writes the same TSV, so it overwrites the one a
# full sweep left, and it is on the caller to point OUT somewhere else.
for w in ${WORKLOADS:-a b c d e f}; do
  # The load average again, once a workload. The header records what the
  # host was carrying when the sweep started and a sweep takes hours, so
  # on a machine that is doing its own work at the same time the header
  # is out of date by workload b. A row is only comparable with the rows
  # taken under the same load, and this is where a reader finds out.
  echo "# $w loadavg: $(cut -d' ' -f1-3 /proc/loadavg 2>/dev/null || uptime)" | tee -a "$OUT"

  if [[ " $SKIP " == *" $w "* ]]; then
    echo "# workload $w skipped: $ENGINE does not support scan" | tee -a "$OUT"
    continue
  fi

  # Per workload arguments, appended after ENGINE_ARGS so they win.
  EXTRA=()
  if [ "$ENGINE" = zu2 ] && [ "$w" = e ]; then
    EXTRA=(-p "zu2.ordered=true")
  fi
  # Whatever the caller wants to vary, appended last so it wins over
  # everything above. Word split on purpose: this is a string of
  # arguments, YCSB_EXTRA="-p fieldcount=1 -p fieldlength=1000", and a
  # run that uses it says so in its own header below.
  if [ -n "${YCSB_EXTRA:-}" ]; then
    # shellcheck disable=SC2206
    EXTRA+=($YCSB_EXTRA)
  fi

  reset_data

  raw=$(timed "$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}" "${BATCH_ARGS[@]+"${BATCH_ARGS[@]}"}" \
    -p recordcount="$RECORDS" -p threadcount="$THREADS" 2>&1)
  load_out=$(grep -E '^(INSERT|TOTAL) ' <<<"$raw")
  if [ -z "$load_out" ]; then
    echo "# load failed for workload $w, see $WORK/$ENGINE-fail-$w.log" | tee -a "$OUT"
    "$BIN" load "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" "${LOAD_ARGS[@]+"${LOAD_ARGS[@]}"}" "${BATCH_ARGS[@]+"${BATCH_ARGS[@]}"}" \
      -p recordcount="$RECORDS" -p threadcount="$THREADS" > "$WORK/$ENGINE-fail-$w.log" 2>&1
    continue
  fi
  emit "$w" load "$load_out"
  version "$raw"
  storage "$w" load "$raw"
  space "$w" load
  maxrss "$w" load "$raw"
  rows "$w" load "$raw"

  raw=$(timed "$BIN" run "$ENGINE" -P "workloads/workload$w" "${ENGINE_ARGS[@]+"${ENGINE_ARGS[@]}"}" "${EXTRA[@]+"${EXTRA[@]}"}" \
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
  maxrss "$w" run "$raw"
done

reset_data
echo
echo "wrote $OUT"
