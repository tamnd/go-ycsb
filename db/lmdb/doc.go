// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package lmdb drives Symas LMDB, the memory mapped copy on write B+tree
// that OpenLDAP was rewritten around and that a great many things since
// have embedded.
//
// It is in the comparison because it is the opposite end of the design
// space from every other embedded engine here. Pebble, Badger, RocksDB
// and zu2 are all log structured: a write appends and a background
// compaction sorts it out later, which buys write throughput and pays
// for it on the read path with several places a key might be. LMDB
// writes in place into a copy on write B+tree, has no compaction, no
// background thread and no cache of its own, and a read is a walk down
// the tree through pages the kernel already has mapped, with no copy at
// any point between the map and the decoder. That makes it the read
// latency floor of this sweep and the number worth beating, and the
// reason to want it in the table is precisely that it is not the same
// shape of engine as the rest.
//
// Its cost is on the other side. LMDB takes one writer lock for the
// whole database, so a write transaction is serialised against every
// other write transaction no matter how many threads the harness runs,
// and the write rows in the table should be read with that in mind. It
// is the engine's design and not something the adapter works around.
//
// The driver needs nothing installed. lmdb-go vendors mdb.c and builds
// it through cgo, so there is no library to fetch and no version to pin
// on a host, only a build tag:
//
//	go build -tags lmdb -o ycsb-lmdb ./cmd/go-ycsb
//	./ycsb-lmdb load lmdb -P workloads/workloadc -p lmdb.dir=/tmp/lmdb
//
// Two properties are worth knowing about. lmdb.write_map is off by
// default and turning it on is worth 3.2x on a load and costs 164x on
// random updates once the dataset outgrows the page cache, which is
// measured in tamnd/zu#709 and explained in the adapter. The other is
// lmdb.map_size. LMDB reserves
// its address space up front and a database that grows past the map
// fails the write rather than growing, so the adapter reserves sixteen
// GiB by default. The file is sparse and the space column measures what
// was written, not what was reserved.
package lmdb
