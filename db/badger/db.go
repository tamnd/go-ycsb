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

	r *util.RowCodec
}

type contextKey string

const stateKey = contextKey("badgerDB")

// badgerState is the per thread scratch this adapter keeps, and it is
// here for the reason the lmdb and pebble ones are (458af54, 83b4de7):
// zu2's adapter reused its maps and buffers because zu2 was the engine
// under test and somebody profiled it, and badger's allocated a map per
// row, a value copy per row and a fmt.Sprintf per operation because
// nobody read it. Comparing those two compares the adapters as much as
// the engines. tamnd/zu#726.
//
// It also happens to fix the badger specific hazard 00d9f3e was about.
// txn.Set keeps the slice it is given until the commit, and the commit
// happens after the closure returns, so a buffer released back to a pool
// inside the closure is released too early. A buffer that belongs to the
// thread and is only reused on that thread's next write is released
// after the commit by construction.
type badgerState struct {
	// table:key scratch and table: scratch for the scan prefix, which is
	// invariant for the whole run and used to be formatted once per scan.
	key    []byte
	prefix []byte
	// Encode scratch for Insert and Update.
	buf []byte

	// A point read copies the value out, because badger only lends it for
	// the length of the callback and the decode sub-slices rather than
	// copying (ca572fb). This is where it copies to, once per thread.
	row  []byte
	vals map[string][]byte
	// Update's own buffer and map rather than Read's. The read modify
	// write path calls Read, then Update on the same key, then verifies
	// what the Read returned, so an Update writing through the buffer
	// Read's map points into would corrupt a row still being looked at.
	// That is not hypothetical, it is what the pebble version of this
	// change failed workload f on before the two were separated.
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

func (s *badgerState) rowKey(table, key string) []byte {
	s.key = append(s.key[:0], table...)
	s.key = append(s.key, ':')
	s.key = append(s.key, key...)
	return s.key
}

func (s *badgerState) tablePrefix(table string) []byte {
	s.prefix = append(s.prefix[:0], table...)
	s.prefix = append(s.prefix, ':')
	return s.prefix
}

func newState() *badgerState {
	return &badgerState{
		vals:    make(map[string][]byte, 16),
		updVals: make(map[string][]byte, 16),
	}
}

func (db *badgerDB) state(ctx context.Context) *badgerState {
	if s, ok := ctx.Value(stateKey).(*badgerState); ok {
		return s
	}
	return newState()
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
		p:  p,
		db: db,
		r:  util.NewRowCodec(p),
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
	return context.WithValue(ctx, stateKey, newState())
}

func (db *badgerDB) CleanupThread(_ context.Context) {
}

// Read copies the value out of badger's callback and decodes the copy.
//
// Decoding inside the callback is not enough, which is what this used to
// do: the decode sub-slices rather than copying, so the map that escaped
// the callback was full of pointers into badger's own memory. That was
// ca572fb. It took the copy with ValueCopy into a fresh allocation, and
// this takes it into a buffer the thread already owns.
//
// The map and the buffer both belong to the thread and both last until
// its next Read, which is the same contract zu2's Read gives.
func (db *badgerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	s := db.state(ctx)
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.rowKey(table, key))
		if err != nil {
			return err
		}
		s.row, err = item.ValueCopy(s.row[:0])
		return err
	})

	if err == badger.ErrKeyNotFound {
		// A key that is not there is not an error to YCSB, it is an empty
		// result, and reporting it as an error makes a workload that
		// reads a key it never wrote fail the run instead of counting a
		// miss.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return db.r.DecodeInto(s.row, fields, s.vals)
}

// Scan copies every row it will return into one thread owned buffer as it
// walks, then decodes them all after.
//
// Two passes, because appending can move the buffer and a row decoded
// during the walk would then point into the old array as soon as a later
// row grew it. Offsets survive a move, pointers do not.
//
// The copy goes through item.Value rather than ValueCopy, which is the
// one place this differs from the lmdb and pebble versions. ValueCopy
// resolves to append(dst[:0], src...), so it truncates whatever it is
// given, which is exactly wrong for a buffer being accumulated across
// fifty rows. Value hands the bytes to a callback that appends them,
// which is safe because the copy happens inside the callback.
//
// The maps are one per row position and reused across scans, because the
// caller gets all fifty rows of a workload e scan at once and they have
// to be live together. That is fifty makemaps an operation this adapter
// used to do, on top of the fifty allocations ValueCopy(nil) was doing.
func (db *badgerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	s := db.state(ctx)
	s.scanBuf = s.scanBuf[:0]
	s.scanOff = s.scanOff[:0]

	err := db.db.View(func(txn *badger.Txn) error {
		// Prefix bound so the walk stops at the end of this table rather
		// than carrying on into whatever is stored after it. Invariant for
		// the whole run, and it used to be formatted once per scan.
		opts := badger.DefaultIteratorOptions
		opts.Prefix = s.tablePrefix(table)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(s.rowKey(table, startKey)); it.Valid() && len(s.scanOff) < count; it.Next() {
			s.scanOff = append(s.scanOff, len(s.scanBuf))
			if err := it.Item().Value(func(v []byte) error {
				s.scanBuf = append(s.scanBuf, v...)
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	n := len(s.scanOff)
	for len(s.scanVals) < n {
		s.scanVals = append(s.scanVals, make(map[string][]byte, 16))
	}
	// Appended rather than sized to count, so a short scan is short rather
	// than padded with nils nothing downstream can tell apart from rows
	// that were really empty.
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

func (db *badgerDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)

	// Everything this touches belongs to the thread and is only reused on
	// the thread's next write, which is after this commit. That is what
	// makes it safe to hand s.buf to txn.Set: badger keeps the slice until
	// the transaction commits rather than copying it, and db.Update
	// commits after the closure returns, which is what made the pooled
	// buffer this used to take a data corruption bug (00d9f3e).
	return db.db.Update(func(txn *badger.Txn) error {
		rowKey := s.rowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}

		// ValueCopy rather than Value, because the map decoded here is
		// read again below when it is encoded, which is outside the
		// callback, and the decode sub-slices rather than copying. Into
		// Update's own buffer rather than Read's, because the read modify
		// write path verifies the row Read returned after this has run.
		s.updRow, err = item.ValueCopy(s.updRow[:0])
		if err != nil {
			return err
		}
		data, err := db.r.DecodeInto(s.updRow, nil, s.updVals)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		s.buf, err = db.r.Encode(s.buf[:0], data)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, s.buf)
	})
}

func (db *badgerDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)

	// Encoded before the key is built, because both live in this thread's
	// scratch and rowKey writes a different field.
	//
	// The buffer is the thread's own and is only reused on this thread's
	// next write, so it is still intact when the transaction commits.
	// That matters here more than anywhere else in this file: txn.Set does
	// not copy the value, it keeps the slice until the commit, and
	// db.Update commits after the closure returns. Releasing a pooled
	// buffer inside the closure, which is what this used to do, handed it
	// back while badger still pointed into it, and corrupted rows during
	// the load (00d9f3e).
	buf, err := db.r.Encode(s.buf[:0], values)
	if err != nil {
		return err
	}
	s.buf = buf

	rowKey := s.rowKey(table, key)
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(rowKey, buf)
	})
}

func (db *badgerDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(db.state(ctx).rowKey(table, key))
	})

	return err
}

func init() {
	ycsb.RegisterDBCreator("badger", badgerCreator{})
}
