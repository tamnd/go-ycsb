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
	"fmt"
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

	r       *util.RowCodec
	bufPool *util.BufPool
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
		p:       p,
		db:      db,
		wo:      wo,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func (db *pebbleDB) Close() error {
	return db.db.Close()
}

func (db *pebbleDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *pebbleDB) CleanupThread(_ context.Context) {}

func (db *pebbleDB) rowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *pebbleDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	// Get hands back a slice that stays valid only until the closer runs,
	// so the decode happens before it and the closer is not deferred past
	// the point where the row is still being read.
	value, closer, err := db.db.Get(db.rowKey(table, key))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m, err := db.r.Decode(value, fields)
	closer.Close()
	return m, err
}

func (db *pebbleDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// UpperBound is the table's own prefix followed by the byte after the
	// separator, so the iterator stops at the end of this table rather
	// than walking into whatever is stored after it. Without it a scan
	// near the last key of the table returns rows from another one, and
	// the scan integrity check reads that as keys that do not climb.
	lower := db.rowKey(table, startKey)
	upper := util.Slice(fmt.Sprintf("%s;", table))
	it, err := db.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	// Sized rather than filled. Filling it to count and leaving the tail
	// nil, which is what the badger adapter here does, reports a short
	// scan as a full one with empty rows in it.
	res := make([]map[string][]byte, 0, count)
	for it.First(); it.Valid() && len(res) < count; it.Next() {
		m, err := db.r.Decode(it.Value(), fields)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, it.Error()
}

func (db *pebbleDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.rowKey(table, key)

	// Read, merge, write. Pebble has no partial update of a stored value,
	// and neither does any other key value engine here, so this is the
	// same read modify write the badger, rocksdb and zu2 adapters do.
	value, closer, err := db.db.Get(rowKey)
	if err != nil && err != pebble.ErrNotFound {
		return err
	}
	data := make(map[string][]byte, len(values))
	if err == nil {
		data, err = db.r.Decode(value, nil)
		closer.Close()
		if err != nil {
			return err
		}
	}
	for field, v := range values {
		data[field] = v
	}

	buf := db.bufPool.Get()
	defer func() { db.bufPool.Put(buf) }()
	buf, err = db.r.Encode(buf, data)
	if err != nil {
		return err
	}
	return db.db.Set(rowKey, buf, db.wo)
}

func (db *pebbleDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer func() { db.bufPool.Put(buf) }()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	return db.db.Set(db.rowKey(table, key), buf, db.wo)
}

func (db *pebbleDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(db.rowKey(table, key), db.wo)
}

func init() {
	ycsb.RegisterDBCreator("pebble", pebbleCreator{})
}
