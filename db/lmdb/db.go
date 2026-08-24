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
	"bytes"
	"context"
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

	r *util.RowCodec
}

type ctxKey struct{}

// state is the per thread scratch this adapter keeps.
//
// It exists because zu2's adapter had one and this one did not, which
// meant the two engines were not being compared on the same terms. zu2
// reused its maps and its buffers because it was the engine under test
// and somebody profiled it (tamnd/zu#678 put makemap at 16.4 percent of a
// thirty two thread run), and lmdb allocated a map per row and formatted
// its key with fmt.Sprintf per operation because nobody ever looked. On
// a fifty row workload e scan that is fifty makemaps an operation on one
// side and none on the other, on the workload that decides the claim.
//
// Nothing here is a trick. It is the same four things zu2 already does:
// a scratch key, a scratch row, a map reused across reads, and one map
// per row position reused across scans. tamnd/zu#726.
type lmdbState struct {
	// table:key scratch, so building a row key costs no allocation.
	key []byte
	// table: scratch for the scan bound, which is invariant for the whole
	// run and used to be formatted once per scan.
	prefix []byte
	// Encode scratch for Insert and Update.
	buf []byte

	// A point read copies the record out of the map before the read
	// transaction ends, because the decode sub-slices rather than copying
	// and the snapshot goes away (a1f84f2). This is where it copies to,
	// once per thread rather than once per read, and vals is the map it
	// decodes into.
	row  []byte
	vals map[string][]byte
	// The map Update decodes the existing row into. Its own rather than
	// vals, because an Update is allowed to happen between a Read and the
	// caller looking at what the Read returned.
	updVals map[string][]byte

	// A scan copies every row it is going to return into one buffer and
	// records where each started, then decodes afterwards. The two passes
	// are not stylistic: appending can move the buffer, so a row decoded
	// during the walk would be left pointing at the old array as soon as a
	// later row grew it. Offsets survive that, pointers do not.
	scanBuf  []byte
	scanOff  []int
	scanVals []map[string][]byte
	scanRows []map[string][]byte
}

func (s *lmdbState) rowKey(table, key string) []byte {
	s.key = append(s.key[:0], table...)
	s.key = append(s.key, ':')
	s.key = append(s.key, key...)
	return s.key
}

func (s *lmdbState) tablePrefix(table string) []byte {
	s.prefix = append(s.prefix[:0], table...)
	s.prefix = append(s.prefix, ':')
	return s.prefix
}

func newState() *lmdbState {
	return &lmdbState{
		vals:    make(map[string][]byte, 16),
		updVals: make(map[string][]byte, 16),
	}
}

// state returns the calling thread's scratch. The fallback is for callers
// that reach an operation without having gone through InitThread, which
// the harness does not do but the interface does not forbid, and taking
// the allocation there is better than writing through a shared one.
func (db *lmdbDB) state(ctx context.Context) *lmdbState {
	if s, ok := ctx.Value(ctxKey{}).(*lmdbState); ok {
		return s
	}
	return newState()
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
		p:   p,
		env: env,
		r:   util.NewRowCodec(p),
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
	return context.WithValue(ctx, ctxKey{}, newState())
}

func (db *lmdbDB) CleanupThread(_ context.Context) {}

// Read takes the record out of the map inside the read transaction and
// decodes it after.
//
// RawRead is on, and the copy on the line below it is what makes that
// legal. The pair has to stay together: RawRead hands back a slice into
// the snapshot, the decode sub-slices rather than copying (decodeBytes
// ends `return remain[n:], remain[:n], nil`), so without a copy of its
// own the map handed to the caller points into a snapshot that has been
// aborted. That was a1f84f2, and it reproduces: workload a with
// dataintegrity=true, fieldcount=1 and fieldlength=8000 at 50000 records
// and 32 threads puts values on overflow pages, which a write frees
// whole and the next write takes straight back, and four runs out of
// four came back with a complete record belonging to a different key.
//
// a1f84f2 fixed it by dropping RawRead and letting lmdb-go take the
// copy, which costs an allocation a read. This takes the copy into a
// buffer the thread already owns instead, so it is a memcpy and nothing
// else, which is what zu2's adapter has always done.
//
// The map and the buffer both belong to the thread and both last until
// its next Read. That is the same contract zu2's Read gives, and it is
// safe against the read modify write path for the same reason: Update
// touches neither of them.
func (db *lmdbDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	s := db.state(ctx)
	found := false
	err := db.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		v, err := txn.Get(db.dbi, s.rowKey(table, key))
		if lmdb.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		s.row = append(s.row[:0], v...)
		found = true
		return nil
	})
	if err != nil || !found {
		return nil, err
	}
	return db.r.DecodeInto(s.row, fields, s.vals)
}

// Scan copies every row it will return into one thread owned buffer
// inside the read transaction, then decodes them all after it.
//
// The two passes are not tidiness. Appending can move the buffer, so a
// row decoded during the walk would be left pointing into the old array
// the moment a later row grew it, which is the same class of bug as
// a1f84f2 only self inflicted. Offsets survive a move, pointers do not,
// so the walk records where each row started and the decode resolves
// those offsets once the buffer has stopped growing.
//
// The maps are one per row position and are reused across scans, because
// the caller gets all fifty rows of a workload e scan at once and they
// have to be live together. That is fifty makemaps an operation this
// adapter used to do and zu2's never did.
func (db *lmdbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	s := db.state(ctx)
	// The prefix is invariant for the whole run and used to be formatted
	// once per scan.
	prefix := s.tablePrefix(table)
	s.scanBuf = s.scanBuf[:0]
	s.scanOff = s.scanOff[:0]

	err := db.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		cur, err := txn.OpenCursor(db.dbi)
		if err != nil {
			return err
		}
		defer cur.Close()

		op := uint(lmdb.SetRange)
		for k, v, err := cur.Get(s.rowKey(table, startKey), nil, op); ; k, v, err = cur.Get(nil, nil, lmdb.Next) {
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			// The keyspace is one tree shared by every table, so the walk
			// has to stop at the end of this table's prefix rather than
			// carrying on into the next one's rows.
			if !bytes.HasPrefix(k, prefix) {
				return nil
			}
			s.scanOff = append(s.scanOff, len(s.scanBuf))
			s.scanBuf = append(s.scanBuf, v...)
			if len(s.scanOff) >= count {
				return nil
			}
		}
	})
	if err != nil {
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

func (db *lmdbDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)
	rowKey := s.rowKey(table, key)

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
		data := s.updVals
		clear(data)
		v, err := txn.Get(db.dbi, rowKey)
		if err != nil && !lmdb.IsNotFound(err) {
			return err
		}
		if err == nil {
			if data, err = db.r.DecodeInto(v, nil, data); err != nil {
				return err
			}
		}
		for field, value := range values {
			data[field] = value
		}

		s.buf, err = db.r.Encode(s.buf[:0], data)
		if err != nil {
			return err
		}
		return txn.Put(db.dbi, rowKey, s.buf, 0)
	})
}

func (db *lmdbDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := db.state(ctx)

	buf, err := db.r.Encode(s.buf[:0], values)
	if err != nil {
		return err
	}
	s.buf = buf
	// Encoded before the key is built, because both live in this thread's
	// scratch and rowKey writes a different field. mdb_put copies the
	// value into the map before it returns, so the buffer is free again as
	// soon as the transaction does, which is not true of badger or bolt
	// (00d9f3e, 8b96620).
	rowKey := s.rowKey(table, key)
	return db.env.Update(func(txn *lmdb.Txn) error {
		return txn.Put(db.dbi, rowKey, buf, 0)
	})
}

func (db *lmdbDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.state(ctx).rowKey(table, key)
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
