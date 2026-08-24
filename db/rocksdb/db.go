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

//go:build rocksdb

package rocksdb

import (
	"bytes"
	"context"
	"os"

	gorocksdb "github.com/linxGnu/grocksdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// properties
const (
	rocksdbDir = "rocksdb.dir"
	// DBOptions
	rocksdbAllowConcurrentMemtableWrites   = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads                   = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites                 = "rocksdb.allow_mmap_writes"
	rocksdbArenaBlockSize                  = "rocksdb.arena_block_size"
	rocksdbDBWriteBufferSize               = "rocksdb.db_write_buffer_size"
	rocksdbHardPendingCompactionBytesLimit = "rocksdb.hard_pending_compaction_bytes_limit"
	rocksdbLevel0FileNumCompactionTrigger  = "rocksdb.level0_file_num_compaction_trigger"
	rocksdbLevel0SlowdownWritesTrigger     = "rocksdb.level0_slowdown_writes_trigger"
	rocksdbLevel0StopWritesTrigger         = "rocksdb.level0_stop_writes_trigger"
	rocksdbMaxBytesForLevelBase            = "rocksdb.max_bytes_for_level_base"
	rocksdbMaxBytesForLevelMultiplier      = "rocksdb.max_bytes_for_level_multiplier"
	rocksdbMaxTotalWalSize                 = "rocksdb.max_total_wal_size"
	rocksdbMemtableHugePageSize            = "rocksdb.memtable_huge_page_size"
	rocksdbNumLevels                       = "rocksdb.num_levels"
	rocksdbUseDirectReads                  = "rocksdb.use_direct_reads"
	rocksdbUseFsync                        = "rocksdb.use_fsync"
	rocksdbWriteBufferSize                 = "rocksdb.write_buffer_size"
	rocksdbMaxWriteBufferNumber            = "rocksdb.max_write_buffer_number"
	// TableOptions/BlockBasedTable
	rocksdbBlockSize                        = "rocksdb.block_size"
	rocksdbBlockSizeDeviation               = "rocksdb.block_size_deviation"
	rocksdbCacheIndexAndFilterBlocks        = "rocksdb.cache_index_and_filter_blocks"
	rocksdbNoBlockCache                     = "rocksdb.no_block_cache"
	rocksdbPinL0FilterAndIndexBlocksInCache = "rocksdb.pin_l0_filter_and_index_blocks_in_cache"
	rocksdbWholeKeyFiltering                = "rocksdb.whole_key_filtering"
	rocksdbBlockRestartInterval             = "rocksdb.block_restart_interval"
	rocksdbFilterPolicy                     = "rocksdb.filter_policy"
	rocksdbIndexType                        = "rocksdb.index_type"
	rocksdbWALDir                           = "rocksdb.wal_dir"
	// TODO: add more configurations
)

type rocksDBCreator struct{}

type rocksDB struct {
	p *properties.Properties

	db *gorocksdb.DB

	r *util.RowCodec

	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

type ctxKey struct{}

// state is the per thread scratch this adapter keeps, and it is the same
// shape the lmdb, pebble and badger ones grew for tamnd/zu#726. zu2's
// adapter reused its maps and its buffers because zu2 was the engine
// under test and it was the one that got profiled, and every other
// adapter allocated a map per row and built its row key with
// fmt.Sprintf per operation. Comparing those two compares the adapters
// as much as the engines.
//
// This adapter did not have the dangling read half of #726: it already
// copied through cloneValue before decoding. What it had was an
// allocation per copy, a map per row and a Sprintf per operation, plus
// two scan bugs of its own noted on Scan.
type rocksState struct {
	// table:key scratch, and the table prefix an iterator has to stop at.
	key    []byte
	prefix []byte
	// Encode scratch for Insert and Update.
	buf []byte

	// Read's copy of the record and the map it decodes into.
	row  []byte
	vals map[string][]byte
	// Update's own, kept separate from Read's for the reason the pebble
	// adapter gives: the read modify write path calls Read, then Update
	// on the same key, then verifies what the Read returned, so an
	// Update writing through the buffer Read's map points into corrupts
	// a row still being looked at.
	updRow  []byte
	updVals map[string][]byte

	// A scan copies every row into one buffer and records where each
	// started, then decodes after the buffer has stopped growing.
	// Appending can move the buffer, so a row decoded during the walk
	// points into the old array as soon as a later row grows it.
	scanBuf  []byte
	scanOff  []int
	scanVals []map[string][]byte
	scanRows []map[string][]byte
}

func (s *rocksState) rowKey(table, key string) []byte {
	s.key = append(s.key[:0], table...)
	s.key = append(s.key, ':')
	s.key = append(s.key, key...)
	return s.key
}

func (s *rocksState) tablePrefix(table string) []byte {
	s.prefix = append(s.prefix[:0], table...)
	s.prefix = append(s.prefix, ':')
	return s.prefix
}

func newState() *rocksState {
	return &rocksState{
		vals:    make(map[string][]byte, 16),
		updVals: make(map[string][]byte, 16),
	}
}

func (db *rocksDB) state(ctx context.Context) *rocksState {
	if s, ok := ctx.Value(ctxKey{}).(*rocksState); ok {
		return s
	}
	return newState()
}

func (c rocksDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(rocksdbDir, "/tmp/rocksdb")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dir)
	}

	opts := getOptions(p)

	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	return &rocksDB{
		p:         p,
		db:        db,
		r:         util.NewRowCodec(p),
		readOpts:  gorocksdb.NewDefaultReadOptions(),
		writeOpts: gorocksdb.NewDefaultWriteOptions(),
	}, nil
}

func getTableOptions(p *properties.Properties) *gorocksdb.BlockBasedTableOptions {
	tblOpts := gorocksdb.NewDefaultBlockBasedTableOptions()

	tblOpts.SetBlockSize(p.GetInt(rocksdbBlockSize, 4<<10))
	tblOpts.SetBlockSizeDeviation(p.GetInt(rocksdbBlockSizeDeviation, 10))
	tblOpts.SetCacheIndexAndFilterBlocks(p.GetBool(rocksdbCacheIndexAndFilterBlocks, false))
	tblOpts.SetNoBlockCache(p.GetBool(rocksdbNoBlockCache, false))
	tblOpts.SetPinL0FilterAndIndexBlocksInCache(p.GetBool(rocksdbPinL0FilterAndIndexBlocksInCache, false))
	tblOpts.SetWholeKeyFiltering(p.GetBool(rocksdbWholeKeyFiltering, true))
	tblOpts.SetBlockRestartInterval(p.GetInt(rocksdbBlockRestartInterval, 16))

	if b := p.GetString(rocksdbFilterPolicy, ""); len(b) > 0 {
		if b == "rocksdb.BuiltinBloomFilter" {
			const defaultBitsPerKey = 10
			tblOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(defaultBitsPerKey))
		}
	}

	indexType := p.GetString(rocksdbIndexType, "kBinarySearch")
	if indexType == "kBinarySearch" {
		tblOpts.SetIndexType(gorocksdb.KBinarySearchIndexType)
	} else if indexType == "kHashSearch" {
		tblOpts.SetIndexType(gorocksdb.KHashSearchIndexType)
	} else if indexType == "kTwoLevelIndexSearch" {
		tblOpts.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
	}

	return tblOpts
}

func getOptions(p *properties.Properties) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	opts.SetAllowConcurrentMemtableWrites(p.GetBool(rocksdbAllowConcurrentMemtableWrites, true))
	opts.SetAllowMmapReads(p.GetBool(rocsdbAllowMmapReads, false))
	opts.SetAllowMmapWrites(p.GetBool(rocksdbAllowMmapWrites, false))
	opts.SetArenaBlockSize(p.GetInt(rocksdbArenaBlockSize, 0))
	opts.SetDbWriteBufferSize(p.GetInt(rocksdbDBWriteBufferSize, 0))
	opts.SetHardPendingCompactionBytesLimit(p.GetUint64(rocksdbHardPendingCompactionBytesLimit, 256<<30))
	opts.SetLevel0FileNumCompactionTrigger(p.GetInt(rocksdbLevel0FileNumCompactionTrigger, 4))
	opts.SetLevel0SlowdownWritesTrigger(p.GetInt(rocksdbLevel0SlowdownWritesTrigger, 20))
	opts.SetLevel0StopWritesTrigger(p.GetInt(rocksdbLevel0StopWritesTrigger, 36))
	opts.SetMaxBytesForLevelBase(p.GetUint64(rocksdbMaxBytesForLevelBase, 256<<20))
	opts.SetMaxBytesForLevelMultiplier(p.GetFloat64(rocksdbMaxBytesForLevelMultiplier, 10))
	opts.SetMaxTotalWalSize(p.GetUint64(rocksdbMaxTotalWalSize, 0))
	opts.SetMemtableHugePageSize(p.GetInt(rocksdbMemtableHugePageSize, 0))
	opts.SetNumLevels(p.GetInt(rocksdbNumLevels, 7))
	opts.SetUseDirectReads(p.GetBool(rocksdbUseDirectReads, false))
	opts.SetUseFsync(p.GetBool(rocksdbUseFsync, false))
	opts.SetWriteBufferSize(p.GetInt(rocksdbWriteBufferSize, 64<<20))
	opts.SetMaxWriteBufferNumber(p.GetInt(rocksdbMaxWriteBufferNumber, 2))
	opts.SetWalDir(p.GetString(rocksdbWALDir, ""))

	opts.SetBlockBasedTableFactory(getTableOptions(p))

	return opts
}

func (db *rocksDB) Close() error {
	db.db.Close()
	return nil
}

func (db *rocksDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return context.WithValue(ctx, ctxKey{}, newState())
}

func (db *rocksDB) CleanupThread(_ context.Context) {
}

// Read copies the record into a buffer the thread owns and decodes that.
//
// The copy is not new, this adapter already had one. What is new is
// where it goes: it used to be append([]byte(nil), ...) per read, which
// is an allocation and a garbage collection for every operation in the
// run, and it is now a memcpy into scratch. The copy itself is still
// required, because Free hands the block back to RocksDB and the decode
// sub-slices rather than copying.
func (db *rocksDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	s := db.state(ctx)
	value, err := db.db.Get(db.readOpts, s.rowKey(table, key))
	if err != nil {
		return nil, err
	}
	if value.Data() == nil {
		value.Free()
		return nil, nil
	}
	s.row = append(s.row[:0], value.Data()...)
	value.Free()

	return db.r.DecodeInto(s.row, fields, s.vals)
}

// Scan walks from the start key, copying every row it will return into
// one thread owned buffer, and decodes them after the walk.
//
// Three things were wrong here and only one of them is the #726 pattern.
//
// The iterator had no upper bound and no prefix check, so it walked
// straight out of the table it was given and into whatever is stored
// after it. A scan near the last key of a table returned rows belonging
// to another one, which the scan integrity check reads as keys that do
// not climb.
//
// The result slice was sized to count and written by index, so a scan
// that found ten rows returned ten rows and forty nil maps, and nothing
// downstream can tell a row that was not there from a row that was
// empty. Appended instead.
//
// And it decoded each row as it walked, out of a fresh allocation per
// row. The allocation is what the offsets replace. Decoding during the
// walk would be wrong here even with the copies, because appending can
// move the buffer.
func (db *rocksDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	s := db.state(ctx)
	it := db.db.NewIterator(db.readOpts)
	defer it.Close()

	prefix := s.tablePrefix(table)
	s.scanBuf = s.scanBuf[:0]
	s.scanOff = s.scanOff[:0]
	for it.Seek(s.rowKey(table, startKey)); it.Valid() && len(s.scanOff) < count; it.Next() {
		k := it.Key()
		inTable := bytes.HasPrefix(k.Data(), prefix)
		k.Free()
		if !inTable {
			break
		}
		v := it.Value()
		s.scanOff = append(s.scanOff, len(s.scanBuf))
		s.scanBuf = append(s.scanBuf, v.Data()...)
		v.Free()
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	n := len(s.scanOff)
	for len(s.scanVals) < n {
		s.scanVals = append(s.scanVals, make(map[string][]byte, 16))
	}
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

func (db *rocksDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)
	rowKey := s.rowKey(table, key)

	// The read half is inline rather than a call to db.Read, because
	// Read decodes into this thread's read map and the read modify write
	// path is still holding the map its own Read returned.
	value, err := db.db.Get(db.readOpts, rowKey)
	if err != nil {
		return err
	}
	data := s.updVals
	clear(data)
	if value.Data() != nil {
		s.updRow = append(s.updRow[:0], value.Data()...)
		value.Free()
		if data, err = db.r.DecodeInto(s.updRow, nil, data); err != nil {
			return err
		}
	} else {
		value.Free()
	}

	for field, v := range values {
		data[field] = v
	}

	s.buf, err = db.r.Encode(s.buf[:0], data)
	if err != nil {
		return err
	}
	// rowKey is still good here: everything between it and this line
	// wrote updRow, updVals or buf, and none of those is the key buffer.
	return db.db.Put(db.writeOpts, rowKey, s.buf)
}

func (db *rocksDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)

	buf, err := db.r.Encode(s.buf[:0], values)
	if err != nil {
		return err
	}
	s.buf = buf
	// Encoded before the key is built, because both live in this thread's
	// scratch. Put copies the value into the memtable before it returns,
	// as lmdb and pebble do and as badger and bolt do not, so the buffer
	// is free again straight after (00d9f3e, 8b96620).
	return db.db.Put(db.writeOpts, s.rowKey(table, key), buf)
}

func (db *rocksDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(db.writeOpts, db.state(ctx).rowKey(table, key))
}

func init() {
	ycsb.RegisterDBCreator("rocksdb", rocksDBCreator{})
}
