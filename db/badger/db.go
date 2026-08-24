// Copyright 2018 PingCAP, Inc.
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

package badger

import (
	"context"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	badgerDir                     = "badger.dir"
	badgerValueDir                = "badger.valuedir"
	badgerSyncWrites              = "badger.sync_writes"
	badgerNumVersionsToKeep       = "badger.num_versions_to_keep"
	badgerBaseTableSize           = "badger.base_table_size"
	badgerLevelSizeMultiplier     = "badger.level_size_multiplier"
	badgerMaxLevels               = "badger.max_levels"
	badgerValueThreshold          = "badger.value_threshold"
	badgerNumMemtables            = "badger.num_memtables"
	badgerNumLevelZeroTables      = "badger.num_level0_tables"
	badgerNumLevelZeroTablesStall = "badger.num_level0_tables_stall"
	badgerBaseLevelSize           = "badger.base_level_size"
	badgerValueLogFileSize        = "badger.value_log_file_size"
	badgerValueLogMaxEntries      = "badger.value_log_max_entries"
	badgerNumCompactors           = "badger.num_compactors"
	badgerBlockCacheSize          = "badger.block_cache_size"
	badgerIndexCacheSize          = "badger.index_cache_size"
	badgerCompression             = "badger.compression"
)

type badgerCreator struct {
}

type badgerDB struct {
	p *properties.Properties

	db *badger.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

type contextKey string

const stateKey = contextKey("badgerDB")

type badgerState struct {
}

func (c badgerCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Dir)
		os.RemoveAll(opts.ValueDir)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) badger.Options {
	// v4 takes the directory as an argument rather than as a field on a
	// package level default, and several of the tuning fields this
	// adapter used to set were renamed or removed along the way:
	// MaxTableSize became BaseTableSize, LevelOneSize became
	// BaseLevelSize, and TableLoadingMode, ValueLogLoadingMode and
	// DoNotCompact are gone because v2 stopped choosing between mmap and
	// file IO per table. The property names here follow the fields so a
	// reader of a run's arguments can find them in Badger's own docs.
	dir := p.GetString(badgerDir, "/tmp/badger")
	opts := badger.DefaultOptions(dir)
	opts.ValueDir = p.GetString(badgerValueDir, dir)

	// Quiet. Badger logs every table build and every compaction at INFO
	// to standard error by default, and this harness reads the process
	// output looking for rows, so a chatty engine is an engine whose
	// numbers are harder to find and whose logging is being timed.
	opts.Logger = nil

	opts.SyncWrites = p.GetBool(badgerSyncWrites, false)
	opts.NumVersionsToKeep = p.GetInt(badgerNumVersionsToKeep, 1)
	opts.BaseTableSize = p.GetInt64(badgerBaseTableSize, 64<<20)
	opts.LevelSizeMultiplier = p.GetInt(badgerLevelSizeMultiplier, 10)
	opts.MaxLevels = p.GetInt(badgerMaxLevels, 7)
	opts.ValueThreshold = p.GetInt64(badgerValueThreshold, 1<<10)
	opts.NumMemtables = p.GetInt(badgerNumMemtables, 5)
	opts.NumLevelZeroTables = p.GetInt(badgerNumLevelZeroTables, 5)
	opts.NumLevelZeroTablesStall = p.GetInt(badgerNumLevelZeroTablesStall, 10)
	opts.BaseLevelSize = p.GetInt64(badgerBaseLevelSize, 256<<20)
	opts.ValueLogFileSize = p.GetInt64(badgerValueLogFileSize, 1<<30)
	opts.ValueLogMaxEntries = uint32(p.GetUint64(badgerValueLogMaxEntries, 1000000))
	opts.NumCompactors = p.GetInt(badgerNumCompactors, 4)

	// Both caches are off by default in v4 and both matter here for the
	// reason the sqlite and pebble adapters give: a read that misses
	// every cache goes to the device, and an engine measured entirely on
	// device reads is being measured against the device. The numbers are
	// deliberately not gigabytes, since a run that swaps is a benchmark
	// of the swap.
	opts.BlockCacheSize = p.GetInt64(badgerBlockCacheSize, 256<<20)
	opts.IndexCacheSize = p.GetInt64(badgerIndexCacheSize, 128<<20)

	return opts
}

func (db *badgerDB) Close() error {
	return db.db.Close()
}

func (db *badgerDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *badgerDB) CleanupThread(_ context.Context) {
}

func (db *badgerDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *badgerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}
		// Value hands the stored bytes to a callback and they are only
		// valid inside it, and this used to decode in there and keep the
		// map, on the assumption that decoding inside the callback was
		// enough. It is not: the decode sub-slices rather than copying,
		// `decodeBytes` ends `return remain[n:], remain[:n], nil`, so the
		// map that escaped the callback was full of pointers into badger's
		// value log mapping.
		//
		// ValueCopy takes the copy the decode does not, which is also what
		// Scan below has always done. It costs an allocation a read.
		// tamnd/zu#726 covers giving that back into a thread owned buffer.
		row, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		m, err = db.r.Decode(row, fields)
		return err
	})

	if err == badger.ErrKeyNotFound {
		// A key that is not there is not an error to YCSB, it is an empty
		// result, and reporting it as an error makes a workload that
		// reads a key it never wrote fail the run instead of counting a
		// miss.
		return nil, nil
	}
	return m, err
}

func (db *badgerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// Appended rather than filled to count. Sizing the slice to count and
	// writing into it left every position a short scan did not reach as a
	// nil map, so a scan that found ten rows returned ten rows and forty
	// nils and the caller could not tell the difference between a row
	// that was not there and a row that was empty. The scan integrity
	// check reads those nils as rows carrying no key.
	res := make([]map[string][]byte, 0, count)
	err := db.db.View(func(txn *badger.Txn) error {
		rowStartKey := db.getRowKey(table, startKey)
		// Prefix bound so the walk stops at the end of this table rather
		// than carrying on into whatever is stored after it.
		opts := badger.DefaultIteratorOptions
		opts.Prefix = util.Slice(fmt.Sprintf("%s:", table))
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(rowStartKey); it.Valid() && len(res) < count; it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res = append(res, m)
		}

		return nil
	})

	return res, err
}

func (db *badgerDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Taken here and released here, outside the closure, for the reason
	// Insert gives: txn.Set keeps the slice rather than copying it and the
	// commit happens after the closure returns, so releasing it inside is
	// releasing a buffer badger has not finished with. The encode itself
	// has to stay inside, because it needs the row the transaction read.
	// The deferred Put reads this variable after Update has returned, so
	// it hands back the grown buffer rather than the one Get produced.
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	err := db.db.Update(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}

		// ValueCopy rather than Value, because the map decoded here is
		// read again below when it is encoded, which is outside the
		// callback, and the decode sub-slices rather than copying. Same
		// defect as Read had.
		row, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		data, err := db.r.Decode(row, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, buf)
	})
	return err
}

func (db *badgerDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// The encode buffer is released after the commit, not inside the
	// closure. txn.Set does not copy the value, it keeps the slice until
	// the transaction commits, and db.Update commits after the closure
	// returns. So a deferred Put inside the closure, which is what this
	// used to do, handed the buffer back to the pool while badger was
	// still holding a pointer into it, another thread's Get took the same
	// backing array (BufPool.Get only reslices to zero length) and its
	// Encode appended over the top of a value that had not been written
	// yet.
	//
	// This is not theoretical either. A load of thirty thousand records at
	// sixteen threads, then workload e reading them back with
	// dataintegrity on, reports a row that came back with no fields at
	// all, on a store the run never wrote to. The load wrote it that way.
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(rowKey, buf)
	})
}

func (db *badgerDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(db.getRowKey(table, key))
	})

	return err
}

func init() {
	ycsb.RegisterDBCreator("badger", badgerCreator{})
}
