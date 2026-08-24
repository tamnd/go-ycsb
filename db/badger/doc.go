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

// Package badger drives Dgraph's Badger, the pure Go log structured merge
// tree that implements WiscKey: values above a threshold are written to a
// separate value log and the tree holds only a pointer to them, so a
// compaction moves keys and pointers rather than whole records.
//
// It is in the comparison beside Pebble because the two are the same
// class of engine built on opposite sides of one design decision. Pebble
// stores values inline the way LevelDB and RocksDB do, Badger separates
// them, and a YCSB record of ten hundred byte fields lands close enough
// to the default threshold that the choice shows up in both the write
// throughput and the space column. Both are pure Go, so neither pays the
// cgo crossing that every other embedded engine here pays, and the pair
// of them is what says how much of zu2's margin is the engine and how
// much is the border.
//
// The driver needs nothing installed. It builds and runs from the module
// cache, so there is no library to fetch and no version to pin on a host:
//
//	go build -o ycsb-badger ./cmd/go-ycsb
//	./ycsb-badger load badger -P workloads/workloadc -p badger.dir=/tmp/badger
//
// Properties are named after the Badger option fields so a run's argument
// list can be read against Badger's own documentation. The names changed
// between v1 and v4 and this adapter follows v4: base_table_size was
// MaxTableSize, base_level_size was LevelOneSize, and the two loading
// mode options are gone because Badger stopped choosing between mmap and
// file IO per table after v1.
package badger
