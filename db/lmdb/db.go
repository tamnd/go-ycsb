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
	// WriteMap on by default. It maps the database writable and a write
	// then goes straight into the map instead of through a private page
	// copy, which is most of what LMDB's write path costs, and the price
	// is that a stray pointer in the process can corrupt the database.
	// That is a real price for a server and not one for a benchmark
	// process that does nothing else.
	flags := uint(0)
	if !p.GetBool(lmdbSync, false) {
		flags |= lmdb.NoSync | lmdb.NoMetaSync
	}
	if p.GetBool(lmdbWriteMap, true) {
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
		// RawRead hands back the bytes in the map rather than a copy of
		// them. They are only valid until the transaction ends, which is
		// why the decode happens inside it, and the decode copies what it
		// keeps. Without this every read allocates and copies the whole
		// record before looking at a single field of it.
		txn.RawRead = true
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
		txn.RawRead = true
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
