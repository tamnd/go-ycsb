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

//go:build lmdb

package lmdb

import (
	"context"
	"fmt"
	"os"

	"github.com/PowerDNS/lmdb-go/lmdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	lmdbDir         = "lmdb.dir"
	lmdbMapSize     = "lmdb.map_size"
	lmdbSync        = "lmdb.sync"
	lmdbWriteMap    = "lmdb.write_map"
	lmdbNoReadahead = "lmdb.no_readahead"
	lmdbMaxReaders  = "lmdb.max_readers"
)

type lmdbCreator struct{}

type lmdbDB struct {
	p   *properties.Properties
	env *lmdb.Env
	dbi lmdb.DBI

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c lmdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(lmdbDir, "/tmp/lmdb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		if err := os.RemoveAll(dir); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}

	// The map is the address space LMDB reserves, not the space it uses,
	// and a database that grows past it fails the write rather than
	// growing the map, so this has to be set ahead of the run and set
	// generously. The file is sparse, so the space column still reports
	// what was written and not what was reserved. Sixteen GiB is enough
	// for a ten million record sweep with room over it.
	if err := env.SetMapSize(p.GetInt64(lmdbMapSize, 16<<30)); err != nil {
		return nil, err
	}
	if err := env.SetMaxDBs(1); err != nil {
		return nil, err
	}
	// One reader slot a thread, and the slot is held for the length of a
	// read transaction. The default is 126, which a 128 thread sweep
	// walks straight past into MDB_READERS_FULL.
	if err := env.SetMaxReaders(p.GetInt(lmdbMaxReaders, 1024)); err != nil {
		return nil, err
	}

	// NoSync and NoMetaSync, which is the same bargain every other engine
	// in this comparison is held to: sqlite at synchronous=OFF, pg at
	// synchronous_commit=off, pebble and badger with sync off, MongoDB at
	// w=1 with no j. None of these numbers is a durability claim.
	//
	// WriteMap off by default, which is the opposite of what this said
	// when it was written and is the result of measuring it. Mapping the
	// database writable lets a write go straight into the map instead of
	// through a private page copy, and on a load, which appends in
	// roughly ascending key order, that is worth 3.2x: 159405 inserts a
	// second against 49149 at a million records and 32 threads.
	//
	// On random updates over a dataset that no longer fits in the page
	// cache it collapses. The same host, the same store, workload a at a
	// million records and 32 threads: 523 operations a second with it on
	// and 85818 with it off, which is 164x. Reads are 2 microseconds in
	// both, so it is entirely the write path. Every thread but one sits
	// in a futex while the one holding the writer lock is down in the
	// filesystem journal, because a copy on write B+tree scatters its
	// dirty pages across the file and each one landing in a hole is a
	// block allocation taken in the fault path rather than by the
	// writeback thread. Raising map_size to something near the working
	// set helps by 3.3x and does not fix it.
	//
	// So the default is the one that is merely slower rather than the
	// one that is sometimes catastrophic, and the fast path stays
	// available to anyone who knows their working set fits. tamnd/zu#709.
	flags := uint(0)
	if !p.GetBool(lmdbSync, false) {
		flags |= lmdb.NoSync | lmdb.NoMetaSync
	}
	if p.GetBool(lmdbWriteMap, false) {
		flags |= lmdb.WriteMap
	}
	if p.GetBool(lmdbNoReadahead, false) {
		flags |= lmdb.NoReadahead
	}

	if err := env.Open(dir, flags, 0644); err != nil {
		return nil, err
	}

	// Reader slots survive a process that died holding one, and this
	// harness kills runs, so a stale slot from a previous sweep would
	// otherwise sit there pinning an old snapshot and keeping every page
	// it referenced from being reused.
	if _, err := env.ReaderCheck(); err != nil {
		env.Close()
		return nil, err
	}

	db := &lmdbDB{
		p:       p,
		env:     env,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}

	// One unnamed database for everything, with the table name in the key
	// the way the badger, pebble and rocksdb adapters here do it. A named
	// database per table would be closer to how LMDB is normally used and
	// would also mean each table's keys live in their own B+tree, which
	// is a different measurement from the one every other key value
	// engine in this sweep is giving.
	if err := env.Update(func(txn *lmdb.Txn) error {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		db.dbi = dbi
		return nil
	}); err != nil {
		env.Close()
		return nil, err
	}

	return db, nil
}

func (db *lmdbDB) Close() error {
	return db.env.Close()
}

func (db *lmdbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *lmdbDB) CleanupThread(_ context.Context) {}

func (db *lmdbDB) rowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *lmdbDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.env.View(func(txn *lmdb.Txn) error {
		// This used to set RawRead, on the reasoning that the decode
		// happens inside the transaction and "the decode copies what it
		// keeps". It does not. `DecodeInto` goes through `EachColumn` to
		// `decodeBytes`, which ends `return remain[n:], remain[:n], nil`,
		// so every value in the map returned here was a slice into the
		// read snapshot, and the snapshot is gone by the time the caller
		// looks at it. Under b and c nothing writes and no page is ever
		// recycled, so it always worked. Under a, e and f a writer takes
		// those pages back and the read quietly returns the wrong bytes.
		//
		// Leaving RawRead off makes lmdb-go copy the record out of the map
		// before handing it over, which is a copy and an allocation a read
		// that this adapter did not used to pay. It is what correctness
		// costs. tamnd/zu#726 puts the copy into a buffer the thread owns
		// instead, which gets most of it back without the bug.
		//
		// This is not a theoretical lifetime argument, it reproduces. Load
		// and run workload a with dataintegrity=true, fieldcount=1 and
		// fieldlength=8000 at 50000 records and 32 threads: eight kilobyte
		// values go on overflow pages, which a write frees whole and the
		// next write takes straight back, so the window is wide enough to
		// hit. Four runs out of four failed with RawRead on and four out
		// of four passed with it off. The failure is not a garbled record,
		// it is a whole clean record belonging to a different key, which
		// is what reading a recycled page looks like.
		v, err := txn.Get(db.dbi, db.rowKey(table, key))
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		m, err = db.r.Decode(v, fields)
		return err
	})
	return m, err
}

func (db *lmdbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, 0, count)
	prefix := fmt.Sprintf("%s:", table)
	err := db.env.View(func(txn *lmdb.Txn) error {
		// No RawRead here either, and for the same reason as Read: the
		// decoded values are slices into the record, so with RawRead on
		// they are slices into the snapshot and they outlive it. A scan
		// returns fifty of them at once, so this one was fifty dangling
		// rows an operation rather than one.
		cur, err := txn.OpenCursor(db.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		op := uint(lmdb.SetRange)
		for k, v, err := cur.Get(db.rowKey(table, startKey), nil, op); ; k, v, err = cur.Get(nil, nil, lmdb.Next) {
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			// The keyspace is one tree shared by every table, so the walk
			// has to stop at the end of this table's prefix rather than
			// carrying on into the next one's rows.
			if len(k) < len(prefix) || string(k[:len(prefix)]) != prefix {
				return nil
			}
			m, err := db.r.Decode(v, fields)
			if err != nil {
				return err
			}
			res = append(res, m)
			if len(res) >= count {
				return nil
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (db *lmdbDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.rowKey(table, key)

	// Read, merge and write inside one write transaction. LMDB takes a
	// single writer lock for the whole transaction, so this is a read
	// under the writer lock rather than the read then write two other
	// adapters here do, and it is both correct and slower for it. The
	// single writer is the engine's design and not something to work
	// around in the adapter.
	return db.env.Update(func(txn *lmdb.Txn) error {
		// RawRead is kept here and dropped in Read and Scan, and the
		// difference is not an oversight. Nothing decoded here leaves the
		// transaction: the merged map is encoded into buf and written on
		// the line below, all of it before the closure returns. Read and
		// Scan hand their maps to the caller, which is what made the same
		// flag a bug there. tamnd/zu#726.
		txn.RawRead = true
		data := make(map[string][]byte, len(values))
		v, err := txn.Get(db.dbi, rowKey)
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		if err == nil {
			if data, err = db.r.Decode(v, nil); err != nil {
				return err
			}
		}
		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() { db.bufPool.Put(buf) }()
		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}
		return txn.Put(db.dbi, rowKey, buf, 0)
	})
}

func (db *lmdbDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer func() { db.bufPool.Put(buf) }()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	rowKey := db.rowKey(table, key)
	return db.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(db.dbi, rowKey, buf, 0)
	})
}

func (db *lmdbDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.rowKey(table, key)
	return db.env.Update(func(txn *lmdb.Txn) error {
		err := txn.Del(db.dbi, rowKey, nil)
		if lmdb.IsNotFound(err) {
			return nil
		}
		return err
	})
}

func init() {
	ycsb.RegisterDBCreator("lmdb", lmdbCreator{})
}
