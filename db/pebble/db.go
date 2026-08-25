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

//go:build pebble

// Package pebble drives CockroachDB's Pebble, which is the log structured
// merge tree Cockroach wrote to replace RocksDB and now runs on.
//
// It is here because it is the closest thing in the comparison to a
// modern embedded engine written the way the papers of the last few years
// say to write one, and because it is pure Go. Every other embedded
// engine in this harness reaches its storage through cgo, which costs
// tens of microseconds a call at the crossing and is a real part of what
// the sweep times. Pebble has no crossing at all, so it is the row that
// says how much of the gap between zu2 and the rest is engine and how
// much is the border.
package pebble

import (
	"context"
	"os"

	"github.com/cockroachdb/pebble/v2"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	pebbleDir           = "pebble.dir"
	pebbleSync          = "pebble.sync"
	pebbleCacheSize     = "pebble.cache_size"
	pebbleMemTableSize  = "pebble.memtable_size"
	pebbleMaxOpenFiles  = "pebble.max_open_files"
	pebbleL0Compaction  = "pebble.l0_compaction_threshold"
	pebbleMaxConcurrent = "pebble.max_concurrent_compactions"
)

type pebbleCreator struct{}

type pebbleDB struct {
	p  *properties.Properties
	db *pebble.DB

	// Whether a write waits for the device. Off, because that is the
	// bargain every engine in this comparison is held to: sqlite at
	// synchronous=OFF, pg at synchronous_commit=off, MongoDB at w=1 with
	// no j. None of these numbers is a durability claim.
	wo *pebble.WriteOptions

	r *util.RowCodec
}

type ctxKey struct{}

// state is the per thread scratch this adapter keeps, and it is here for
// the reason the lmdb one is: zu2's adapter reused its maps and its
// buffers because zu2 was the engine under test and somebody profiled
// it, and pebble's allocated a map per row and formatted its key with
// fmt.Sprintf per operation because nobody read it. Comparing those two
// compares the adapters as much as the engines. tamnd/zu#726.
type pebbleState struct {
	// table:key scratch, and the table; upper bound for a scan, which is
	// invariant for the run and used to be formatted once per scan.
	key   []byte
	upper []byte
	// Encode scratch for Insert and Update.
	buf []byte

	// A read copies the record before the closer runs, because the closer
	// releases the block back to the cache and the decode sub-slices
	// rather than copying (ca572fb). This is where it copies to, once per
	// thread rather than once per read.
	row  []byte
	vals map[string][]byte
	// Update's own row buffer and map, kept separate from row and vals
	// rather than shared with them. The read modify write path calls Read,
	// then Update on the same key, then verifies what the Read returned,
	// so an Update that wrote through the buffer Read's map points into
	// would corrupt a row that is still being looked at. Sharing them
	// passed workloads a, b, c and e and failed f, which is the only one
	// that reads and writes in that order.
	updRow  []byte
	updVals map[string][]byte

	// A scan copies every row it will return into one buffer and records
	// where each started, then decodes afterwards. Appending can move the
	// buffer, so a row decoded during the walk would point into the old
	// array as soon as a later row grew it. Offsets survive a move.
	scanBuf  []byte
	scanOff  []int
	scanVals []map[string][]byte
	scanRows []map[string][]byte
}

func (s *pebbleState) rowKey(table, key string) []byte {
	s.key = append(s.key[:0], table...)
	s.key = append(s.key, ':')
	s.key = append(s.key, key...)
	return s.key
}

// upperBound is the table's own prefix with the byte after the separator,
// so an iterator stops at the end of this table rather than walking into
// whatever is stored after it.
func (s *pebbleState) upperBound(table string) []byte {
	s.upper = append(s.upper[:0], table...)
	s.upper = append(s.upper, ';')
	return s.upper
}

func newState() *pebbleState {
	return &pebbleState{
		vals:    make(map[string][]byte, 16),
		updVals: make(map[string][]byte, 16),
	}
}

func (db *pebbleDB) state(ctx context.Context) *pebbleState {
	if s, ok := ctx.Value(ctxKey{}).(*pebbleState); ok {
		return s
	}
	return newState()
}

func (c pebbleCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(pebbleDir, "/tmp/pebble")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
	}

	opts := &pebble.Options{
		MaxOpenFiles:          p.GetInt(pebbleMaxOpenFiles, 1024),
		MemTableSize:          uint64(p.GetInt64(pebbleMemTableSize, 64<<20)),
		L0CompactionThreshold: p.GetInt(pebbleL0Compaction, 4),
		// A [lower, upper] range rather than a single number in this
		// version. Pebble runs one compaction and adds more up to the
		// upper bound as it falls behind, so the lower bound stays at
		// one and only the ceiling is set here.
		CompactionConcurrencyRange: func() (int, int) { return 1, p.GetInt(pebbleMaxConcurrent, 4) },
	}
	// The block cache is shared across the whole store and defaults to 8
	// MiB, which for any database worth benchmarking means a read off the
	// device for nearly every miss. The number here is deliberately not a
	// gigabyte, for the reason the sqlite adapter gives: a run that swaps
	// is a benchmark of the swap.
	opts.Cache = pebble.NewCache(p.GetInt64(pebbleCacheSize, 256<<20))
	defer opts.Cache.Unref()

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	wo := pebble.NoSync
	if p.GetBool(pebbleSync, false) {
		wo = pebble.Sync
	}

	return &pebbleDB{
		p:  p,
		db: db,
		wo: wo,
		r:  util.NewRowCodec(p),
	}, nil
}

func (db *pebbleDB) Close() error {
	return db.db.Close()
}

func (db *pebbleDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return context.WithValue(ctx, ctxKey{}, newState())
}

func (db *pebbleDB) CleanupThread(_ context.Context) {}

// Read copies the record out before running the closer and decodes the
// copy.
//
// Decoding before the close is not enough, which is what this used to do:
// the decode sub-slices rather than copying (decodeBytes ends `return
// remain[n:], remain[:n], nil`) and the closer is what releases the block
// back to the cache, so the map handed to the caller pointed into memory
// pebble had taken back. That was ca572fb, and it reproduces with
// dataintegrity on at eight kilobyte values.
//
// ca572fb took the copy into a fresh allocation. This takes it into a
// buffer the thread already owns, so it is a memcpy and nothing else,
// which is what zu2's adapter has always done.
//
// The map and the buffer both belong to the thread and both last until
// its next Read, which is the same contract zu2's Read gives.
func (db *pebbleDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	s := db.state(ctx)
	value, closer, err := db.db.Get(s.rowKey(table, key))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.row = append(s.row[:0], value...)
	closer.Close()
	return db.r.DecodeInto(s.row, fields, s.vals)
}

// Scan copies every row it will return into one thread owned buffer as
// it walks, then decodes them all after.
//
// The two passes are not tidiness. `it.Value()` is good only until the
// next `it.Next()` and the decode sub-slices it, so decoding during the
// walk left every row but the last pointing at bytes the iterator had
// moved off (ca572fb). Copying during the walk fixes that, but appending
// can move the buffer, so a row decoded during the walk would then point
// into the old array as soon as a later row grew it. Offsets survive a
// move, pointers do not.
//
// The maps are one per row position and reused across scans, because the
// caller gets all fifty rows of a workload e scan at once and they have
// to be live together. That is fifty makemaps an operation this adapter
// used to do and zu2's never did.
func (db *pebbleDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	s := db.state(ctx)
	// The bound is invariant for the run and used to be formatted once per
	// scan. Without it a scan near the last key of the table returns rows
	// from another one, and the scan integrity check reads that as keys
	// that do not climb.
	upper := s.upperBound(table)
	lower := s.rowKey(table, startKey)
	it, err := db.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}

	s.scanBuf = s.scanBuf[:0]
	s.scanOff = s.scanOff[:0]
	for it.First(); it.Valid() && len(s.scanOff) < count; it.Next() {
		s.scanOff = append(s.scanOff, len(s.scanBuf))
		s.scanBuf = append(s.scanBuf, it.Value()...)
	}
	err = it.Error()
	it.Close()
	if err != nil {
		return nil, err
	}

	n := len(s.scanOff)
	for len(s.scanVals) < n {
		s.scanVals = append(s.scanVals, make(map[string][]byte, 16))
	}
	// Appended rather than sized to count, so a short scan is short rather
	// than padded with empty rows nothing downstream can tell apart from
	// rows that were really empty.
	res := s.scanRows[:0]
	for i := 0; i < n; i++ {
		end := len(s.scanBuf)
		if i+1 < n {
			end = s.scanOff[i+1]
		}
		m, err := db.r.DecodeInto(s.scanBuf[s.scanOff[i]:end], fields, s.scanVals[i])
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	s.scanRows = res
	return res, nil
}

func (db *pebbleDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)
	rowKey := s.rowKey(table, key)

	// Read, merge, write. Pebble has no partial update of a stored value,
	// and neither does any other key value engine here, so this is the
	// same read modify write the badger, rocksdb and zu2 adapters do.
	//
	// The copy before the close is the ca572fb fix a third time. This
	// function decoded the value, closed the block, and then read the
	// decoded map again to encode it, which is reading pebble's memory
	// after pebble took it back, exactly like Read and Scan were.
	value, closer, err := db.db.Get(rowKey)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	data := s.updVals
	clear(data)
	if err == nil {
		s.updRow = append(s.updRow[:0], value...)
		closer.Close()
		if data, err = db.r.DecodeInto(s.updRow, nil, data); err != nil {
			return err
		}
	}
	for field, v := range values {
		data[field] = v
	}

	s.buf, err = db.r.Encode(s.buf[:0], data)
	if err != nil {
		return err
	}
	return db.db.Set(rowKey, s.buf, db.wo)
}

func (db *pebbleDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)

	buf, err := db.r.Encode(s.buf[:0], values)
	if err != nil {
		return err
	}
	s.buf = buf
	// Encoded before the key is built, because both live in this thread's
	// scratch and rowKey writes a different field. pebble's Set copies the
	// value into the memtable before it returns, so the buffer is free
	// again straight after, which is not true of badger or bolt (00d9f3e,
	// 8b96620).
	return db.db.Set(s.rowKey(table, key), buf, db.wo)
}

func (db *pebbleDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(db.state(ctx).rowKey(table, key), db.wo)
}

func init() {
	ycsb.RegisterDBCreator("pebble", pebbleCreator{})
}
