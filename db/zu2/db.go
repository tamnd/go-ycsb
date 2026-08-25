// Copyright 2026 tamnd.
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

//go:build zu2

// Package zu2 is the YCSB adapter for zu2, over libzu2.
//
// zu2 is a storage engine and not a database with a query language, so
// this adapter is a key value adapter and looks like db/rocksdb rather
// than db/zu. A row is one record: the key is table:key and the value is
// the same TiDB row encoding every other key value engine here uses, so
// the bytes on the device are comparable across adapters and none of
// them is being charged for a different serialisation.
//
// The engine opens inside the harness process and every operation is a
// direct call into the library, so nothing in a timed region crosses a
// process boundary or a socket. Each worker holds its own session for
// the length of the run, because a session owns an epoch slot and the
// buffers the read path answers out of, and libzu2 answers
// ZU2_MISUSE_CONCURRENT rather than corrupting one when two threads use
// it at once.
//
// Scan runs over zu2's scan plane, which is a key ordered structure the
// engine maintains beside the hash index and holds keys only. A scan
// seeks it once, walks it, and does the ordinary point read for each key
// it lands on, so what a scan reads is exactly as fresh as what a read
// reads. The plane is off unless zu2.ordered says otherwise, because it
// is memory a workload that never scans should not be paying for, and a
// scan against a database opened without it is an error rather than an
// empty answer. The plane is rebuilt from the log at open, so the flag is
// about the process doing the scanning and not about the process that
// wrote the records, and a run whose workload scans without it refuses to
// start rather than failing every scan.
//
// The graph plane is not exercised here either. YCSB core has no
// traversal in it, and the traversal comparison lives in
// tamnd/graph-bench where the rivals are the same engines with their
// own indexes on an edge table.
//
// Build tag: zu2. The cgo lines default to a sibling zu checkout built
// in release mode, which is the layout the repos are developed in. cgo
// cannot read the environment, so anything else overrides at build time:
//
//	CGO_CFLAGS="-I$ZU2_INCLUDE" \
//	CGO_LDFLAGS="-L$ZU2_LIB -lzu2 -Wl,-rpath,$ZU2_LIB" \
//	  go build -tags zu2 ./cmd/go-ycsb
//
// Build libzu2 first with cargo build --release -p zu2-capi in the zu
// repo.
//
// Windows links zu2.dll through its import library, named explicitly
// with -l:zu2.dll.lib rather than left to -lzu2. The zu repo pins an
// MSVC toolchain in rust-toolchain.toml, so the static archive next to
// the DLL is MSVC flavoured and a mingw cgo link cannot consume it: it
// ends in undefined references to __chkstk and to the MSVC type_info
// vtable. The DLL is a C interface and crosses that boundary fine.
// Copy zu2.dll beside the binary, or put its directory on PATH, since
// Windows has no rpath to record where it came from.
package zu2

/*
#cgo CFLAGS: -I${SRCDIR}/../../../zu/crates/zu2-capi/include
#cgo !windows LDFLAGS: -L${SRCDIR}/../../../zu/target/release -lzu2 -Wl,-rpath,${SRCDIR}/../../../zu/target/release
#cgo windows LDFLAGS: -L${SRCDIR}/../../../zu/target/release -l:zu2.dll.lib
#include <stdlib.h>
#include "zu2.h"
*/
import "C"

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"unsafe"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// zu2 properties. Everything that is not set takes the engine's own
// default, which is what zu2_options_init writes.
const (
	zu2Path = "zu2.path"
	// async or durable. async is the default because it is what the
	// engine defaults to, and a run that wants the fsync comparison
	// says so rather than getting it by accident.
	zu2Durability = "zu2.durability"
	// Sizing hints. The index doubles when it passes half full, so a
	// hint that is too small costs growths rather than correctness, and
	// a load that knows its record count still saves those by saying so.
	// max_nodes is sized once and not grown.
	zu2IndexBuckets = "zu2.index_buckets"
	zu2MaxPages     = "zu2.max_pages"
	// Pages of log held in memory, 4 MiB each. Unset means the engine
	// default, which is no bound at all: zu2 heap allocates every page
	// it appends to and frees one only when eviction drops it, so a
	// database that never evicts holds all of itself in the process. At
	// 200000 records on server3 that was 230 MiB of a 247 MiB database
	// against sqlite serving the same data out of 20 MiB, and setting
	// this is the whole of the fix. tamnd/zu#636.
	//
	// The engine clamps it up to one more than the mutable window, so
	// five is the smallest setting that means anything.
	zu2MemoryPages        = "zu2.memory_pages"
	zu2MaxNodes           = "zu2.max_nodes"
	zu2SpaceTargetPercent = "zu2.space_target_percent"
	zu2CompactBelow       = "zu2.compact_below"
	// Pin the index at index_buckets however many keys arrive. Off, so
	// a sweep gets the engine's own behaviour; on, it is the only way to
	// measure what a crowded table costs, since a table left to grow
	// stops being crowded partway through the measurement.
	zu2FixedIndex = "zu2.fixed_index"
	// Open a file with a hole in it at the prefix below the hole. Off,
	// and a run that turns it on is asking to measure a database that is
	// knowingly short, which the storage line then says out loud.
	zu2Salvage = "zu2.salvage"
	// Sessions the engine makes room for. One per worker thread, held
	// for the whole run, so the default here is threadcount with a
	// little headroom rather than the engine's own 128: go-ycsb's
	// default threadcount is 200 and a run that asks for more sessions
	// than the engine has room for is refused, not queued.
	zu2Sessions = "zu2.sessions"
	// Compact once before the storage line is printed, so the number is
	// the settled file and not the file mid write. Off by default: the
	// interesting number is usually what the run left behind.
	zu2CompactOnClose = "zu2.compact_on_close"
	zu2StorageReport  = "zu2.storage_report"
	// Maintain the scan plane, which is what a range scan runs over.
	// Off by default and it has to be a decision for the whole life of
	// the data, because the plane is built as records arrive and a
	// database that ran without it has no key order to hand a scan.
	// Workload e turns it on; the others do not, and their storage line
	// then shows what the plane is not costing them.
	zu2Ordered = "zu2.ordered"
	// Put a cold record back in the log when a read finds it there. On,
	// because the tier's rule for cold is a lap of the log without a
	// write and a read only workload never writes, so without this the
	// records a run reads most are the ones it reads from the device
	// forever. Off is the A/B, and the storage line says how many
	// records promotion moved either way.
	zu2Promote = "zu2.promote"
	// Compress the values the cold tier takes. On, the way the engine
	// has it: a record only reaches the tier by surviving a lap of the
	// log unwritten, and reading it back is a pread of a device, so the
	// decompress sits under a cost that is already there. Off is the
	// A/B for the storage column, which is where the saving shows up.
	zu2ColdCompression = "zu2.coldcompression"
)

type zu2Creator struct{}

// session is one libzu2 session plus the scratch it writes through. A
// session may move between threads but must not be in two callers'
// hands at once, which is why each worker keeps its own for the length
// of the run.
type session struct {
	s *C.zu2_session
	// Row key scratch, so building table:key costs no allocation.
	key []byte
	// And the same for the table prefix a scan tests every row against.
	// Separate from `key` on purpose: a scan holds the seek key while it
	// walks and tests the prefix a row, so one buffer for both would have
	// the second write eat the first. That is the aliasing the lmdb driver
	// keeps `s.prefix` and `s.key` apart to avoid.
	prefix []byte
	// Row value scratch for the encoder.
	buf []byte
	// Where the batch path stages, kept per session so a load of a
	// million rows allocates a handful of times and not per batch.
	arena carena
	pairs cpairs
	off   []int
	// The same for a point read: one buffer and one map a session,
	// reused, rather than a C.GoBytes and a makemap per read. Between
	// them those two were thirty percent of a thirty two thread run
	// (#678). See readOne for what the caller is promised about them.
	row  []byte
	vals map[string][]byte
	// And the same again for a scan, one map a row rather than one a
	// session because the caller gets every row of a scan at once and
	// they have to be live together. The slice and the maps in it both
	// grow to the largest scan and are then reused. Workload E returns
	// fifty rows a scan, so this is fifty makemaps an operation that
	// the collector does not have to take back afterwards. The values in
	// them point into the engine's own scan buffer, so a caller holding
	// them past the next call on this session is reading a buffer that
	// has moved on, which is the lifetime the C API states.
	scanVals []map[string][]byte
	scanRows []map[string][]byte
	// The map free scan path. One row is live at a time there, because
	// the caller sees each row inside a callback and the row is over when
	// the callback returns, so one slice a session is enough where the
	// map path needs one a row. `eachFields` is what `eachPos` was built
	// from, kept so the positions are computed once a session and not
	// once a row.
	eachPos    []int
	eachVals   [][]byte
	eachFields []string
	// And once more for a batch read, which is the same problem again:
	// every row of the batch comes back at once, so each key needs its
	// own buffer and its own map or they all end up holding the last
	// row read.
	batchRow  [][]byte
	batchVals []map[string][]byte
	batchOut  []map[string][]byte
}

type zu2DB struct {
	p *properties.Properties

	db   *C.zu2_db
	path string

	r *util.RowCodec

	durability C.zu2_durability
	report     bool
	compactEnd bool

	mu       sync.Mutex
	sessions []*session // every session opened, so Close can free them
}

type ctxKey struct{}

func (zu2Creator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(zu2DB)
	d.p = p
	d.r = util.NewRowCodec(p)
	d.path = p.GetString(zu2Path, "/tmp/ycsb.zu2")
	d.report = p.GetBool(zu2StorageReport, true)
	d.compactEnd = p.GetBool(zu2CompactOnClose, false)

	switch strings.ToLower(p.GetString(zu2Durability, "async")) {
	case "async":
		d.durability = C.ZU2_ASYNC
	case "durable":
		d.durability = C.ZU2_DURABLE
	default:
		return nil, fmt.Errorf("zu2: %s must be async or durable", zu2Durability)
	}

	// A workload that scans against a store opened without the plane
	// fails every scan, and a summary of nothing but SCAN_ERROR reads
	// like an engine that cannot scan rather than a flag that was not
	// set. It cost two hours once, so the run does not start. tamnd/zu#730.
	if p.GetFloat64(prop.ScanProportion, prop.ScanProportionDefault) > 0 &&
		!p.GetBool(zu2Ordered, false) {
		return nil, fmt.Errorf("zu2: this workload scans and %s is not set, "+
			"so the store would open without a scan plane and every scan "+
			"would fail", zu2Ordered)
	}

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(d.path)
	}

	var opt C.zu2_options
	if st := C.zu2_options_init(&opt); st != C.ZU2_OK {
		return nil, fmt.Errorf("zu2: options init failed with status %d", int(st))
	}
	opt.durability = d.durability
	if v := p.GetUint64(zu2IndexBuckets, 0); v != 0 {
		opt.index_buckets = C.uint64_t(v)
	}
	if v := p.GetUint64(zu2MaxPages, 0); v != 0 {
		opt.max_pages = C.uint64_t(v)
	}
	if v := p.GetUint64(zu2MemoryPages, 0); v != 0 {
		opt.memory_pages = C.uint64_t(v)
	}
	if v := p.GetUint64(zu2MaxNodes, 0); v != 0 {
		opt.max_nodes = C.uint64_t(v)
	}
	if v := p.GetUint64(zu2SpaceTargetPercent, 0); v != 0 {
		opt.space_target_percent = C.uint32_t(v)
	}
	if v := p.GetUint64(zu2CompactBelow, 0); v != 0 {
		opt.compact_below = C.uint64_t(v)
	}
	if p.GetBool(zu2FixedIndex, false) {
		opt.fixed_index = 1
	}
	if p.GetBool(zu2Salvage, false) {
		opt.salvage = 1
	}
	if p.GetBool(zu2Ordered, false) {
		opt.ordered = 1
	}
	if !p.GetBool(zu2Promote, true) {
		opt.no_promote_reads = 1
	}
	if !p.GetBool(zu2ColdCompression, true) {
		opt.no_cold_compression = 1
	}
	threads := p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault)
	opt.sessions = C.uint64_t(p.GetInt64(zu2Sessions, threads+8))

	cpath := C.CString(d.path)
	defer C.free(unsafe.Pointer(cpath))

	var cerr *C.char
	var cerrLen C.size_t
	st := C.zu2_open(cpath, C.size_t(len(d.path)), &opt, &d.db, &cerr, &cerrLen)
	if st != C.ZU2_OK {
		msg := ""
		if cerr != nil {
			msg = ": " + C.GoStringN(cerr, C.int(cerrLen))
		}
		return nil, fmt.Errorf("zu2: open %q failed with status %d%s", d.path, int(st), msg)
	}
	return d, nil
}

// newSession opens a session at the run's durability and records it for
// Close.
func (db *zu2DB) newSession() (*session, error) {
	var s *C.zu2_session
	if st := C.zu2_session_open(db.db, &s); st != C.ZU2_OK {
		return nil, dbErr(db.db, st, "zu2: session open failed")
	}
	if st := C.zu2_set_durability(s, db.durability); st != C.ZU2_OK {
		C.zu2_session_close(s)
		return nil, fmt.Errorf("zu2: set durability failed with status %d", int(st))
	}

	out := &session{s: s, key: make([]byte, 0, 64), buf: make([]byte, 0, 1024)}
	db.mu.Lock()
	db.sessions = append(db.sessions, out)
	db.mu.Unlock()
	return out, nil
}

// InitThread gives each worker its own session.
func (db *zu2DB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	s, err := db.newSession()
	if err != nil {
		panic(err)
	}
	return context.WithValue(ctx, ctxKey{}, s)
}

// CleanupThread leaves the session to Close. A worker's context is gone
// by the time the run reports, and closing here would race the storage
// line against sessions that are still finishing.
func (db *zu2DB) CleanupThread(_ context.Context) {}

func sessionOf(ctx context.Context) *session {
	s, _ := ctx.Value(ctxKey{}).(*session)
	return s
}

// rowKey builds table:key in the session's scratch. Same shape as the
// other key value adapters here, so a record costs the same key bytes
// whichever engine is holding it.
func (s *session) rowKey(table, key string) []byte {
	s.key = append(s.key[:0], table...)
	s.key = append(s.key, ':')
	s.key = append(s.key, key...)
	return s.key
}

// tablePrefix is what every row a scan walks has to start with, in the
// session's own buffer so the check costs no allocation. Both scan paths
// were building this with []byte(table + ":") a call, which is a string
// concatenation and a conversion, so two allocations an operation on the
// path whose whole point is not to have any.
func (s *session) tablePrefix(table string) []byte {
	s.prefix = append(s.prefix[:0], table...)
	s.prefix = append(s.prefix, ':')
	return s.prefix
}

// ptr hands C the start of a Go slice. libzu2 copies what it is given
// before it returns, so nothing here is retained across the call, which
// is the rule cgo cares about.
func ptr(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}

// carena is C memory the batch path stages keys and values into.
//
// cgo's pointer rules are the reason it exists rather than an array of
// zu2_pair pointing straight at Go slices: memory handed to C may not
// itself hold Go pointers, and every entry of a batch is two of them.
// So a batch copies into C memory once and the array points inside
// that. libzu2 copies again into the log, and both copies together are
// still cheaper than what the staging buys, which is one crossing for a
// whole batch instead of one per row.
type carena struct {
	base unsafe.Pointer
	len  int
	cap  int
}

func (a *carena) reset() { a.len = 0 }

// put copies b in and returns where it landed. An offset and not a
// pointer, because the block moves when a later put grows it.
func (a *carena) put(b []byte) int {
	off := a.len
	if len(b) == 0 {
		return off
	}
	if a.len+len(b) > a.cap {
		next := a.cap*2 + len(b)
		if next < 4096 {
			next = 4096
		}
		p := C.realloc(a.base, C.size_t(next))
		if p == nil {
			panic("zu2: out of memory staging a batch")
		}
		a.base, a.cap = p, next
	}
	copy(unsafe.Slice((*byte)(a.base), a.cap)[off:], b)
	a.len += len(b)
	return off
}

// at is where an offset ended up, once the block has stopped moving.
func (a *carena) at(off int) *C.uint8_t {
	if a.base == nil {
		return nil
	}
	return (*C.uint8_t)(unsafe.Add(a.base, off))
}

func (a *carena) free() {
	if a.base != nil {
		C.free(a.base)
		a.base, a.len, a.cap = nil, 0, 0
	}
}

// cpairs is the zu2_pair array itself, in C memory for the same reason
// and grown the same way.
type cpairs struct {
	base unsafe.Pointer
	cap  int
}

func (p *cpairs) reserve(n int) []C.zu2_pair {
	if n > p.cap {
		q := C.realloc(p.base, C.size_t(n)*C.size_t(unsafe.Sizeof(C.zu2_pair{})))
		if q == nil {
			panic("zu2: out of memory staging a batch")
		}
		p.base, p.cap = q, n
	}
	return unsafe.Slice((*C.zu2_pair)(p.base), p.cap)[:n]
}

func (p *cpairs) free() {
	if p.base != nil {
		C.free(p.base)
		p.base, p.cap = nil, 0
	}
}

// sessionErr turns a failed status and the session's last error into a
// Go error.
func sessionErr(s *C.zu2_session, st C.zu2_status, fallback string) error {
	var n C.size_t
	if p := C.zu2_session_error(s, &n); p != nil && n > 0 {
		return fmt.Errorf("%s: %s", fallback, C.GoStringN(p, C.int(n)))
	}
	return fmt.Errorf("%s (status %d)", fallback, int(st))
}

func dbErr(db *C.zu2_db, st C.zu2_status, fallback string) error {
	var n C.size_t
	if p := C.zu2_db_error(db, &n); p != nil && n > 0 {
		return fmt.Errorf("%s: %s", fallback, C.GoStringN(p, C.int(n)))
	}
	return fmt.Errorf("%s (status %d)", fallback, int(st))
}

// rawRead looks one row up and hands back the engine's own bytes. The
// slice points into storage that belongs to the session and is good only
// until the next call on that session, so a caller that needs it to
// outlive that has to copy it. A nil slice with a nil error means the key
// is not there.
func (db *zu2DB) rawRead(s *session, table string, key string) ([]byte, error) {
	rk := s.rowKey(table, key)

	var val *C.uint8_t
	var valLen C.size_t
	var found C.int
	st := C.zu2_read(s.s, ptr(rk), C.size_t(len(rk)), &val, &valLen, &found)
	if st != C.ZU2_OK {
		return nil, sessionErr(s.s, st, fmt.Sprintf("zu2: read %q failed", key))
	}
	if found == 0 {
		return nil, nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(val)), int(valLen)), nil
}

// readOne is a read with the caller saying where the row bytes and the
// decoded fields land. Read points both at storage the session keeps and
// reuses, since it hands back one row at a time. BatchRead gives every
// key of the batch its own, since it hands back all of them together.
//
// The copy stays here, unlike in Scan, and the two are not inconsistent.
// zu2_scan documents the lifetime of what it hands back, the pair array
// is the session's own and is good until the next call on this session,
// so decoding out of it is decoding out of something with a stated
// contract. zu2_read documents no lifetime at all. Its buffer happens to
// be a different one that only another read disturbs, so skipping the
// copy happens to work today, and read-modify-write is the case that
// shows why happening to work is not enough: it reads, then updates on
// the same session, then looks at the row it read. A copy is a thousand
// bytes and a point read is microseconds, so what that buys is a couple
// of percent against a correctness cliff that moves the day somebody
// makes an upsert reuse that buffer. Not worth it. If zu2_read grows the
// promise zu2_scan already makes, this comes out.
//
// The buffer comes back out because appending to it may move it.
func (db *zu2DB) readOne(s *session, table string, key string, fields []string, buf []byte, into map[string][]byte) (map[string][]byte, []byte, error) {
	row, err := db.rawRead(s, table, key)
	if err != nil || row == nil {
		return nil, buf, err
	}
	buf = append(buf[:0], row...)
	m, err := db.r.DecodeInto(buf, fields, into)
	return m, buf, err
}

func (db *zu2DB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	s := sessionOf(ctx)
	if s.vals == nil {
		s.vals = make(map[string][]byte, 16)
	}
	m, buf, err := db.readOne(s, table, key, fields, s.row, s.vals)
	s.row = buf
	return m, err
}

// Scan hands back up to count records at or after table:startKey in key
// order.
//
// The row key is table:key, which orders every row of one table together
// and puts the tables themselves in name order, so a walk that runs off
// the end of this table lands in the next one. That is what the prefix
// check is for: the caller asked for rows of a table and gets rows of
// that table or fewer, never rows of another one.
//
// One crossing for the whole scan rather than one per row. The engine
// fills a buffer of key and value pairs that belongs to the session and
// lasts exactly until the next call on it, and this decodes out of that
// buffer rather than out of a copy of it, the same as Read.
func (db *zu2DB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	s := sessionOf(ctx)
	rk := s.rowKey(table, startKey)

	var pairs *C.zu2_pair
	var returned C.size_t
	st := C.zu2_scan(s.s, ptr(rk), C.size_t(len(rk)), C.size_t(count), &pairs, &returned)
	if st != C.ZU2_OK {
		return nil, sessionErr(s.s, st, fmt.Sprintf("zu2: scan from %q failed", startKey))
	}
	if returned == 0 || pairs == nil {
		return nil, nil
	}

	// The decoded values point straight into the engine's own scan
	// buffer rather than into a copy of it. That used to be a copy into
	// a buffer this session kept, and the comment justifying it conceded
	// the thing that makes it unnecessary: both buffers last exactly
	// until the next call on this session, so the copy bought no
	// lifetime that the caller did not already have. The C API says so
	// itself, `*pairs` is the session's own array and is good until the
	// next call on this session. Fifty rows of a thousand bytes is fifty
	// kilobytes of memcpy a scan, and workload e is nothing but scans.
	all := unsafe.Slice(pairs, int(returned))

	prefix := s.tablePrefix(table)
	got := s.scanRows[:0]
	for _, pair := range all {
		key := unsafe.Slice((*byte)(unsafe.Pointer(pair.key)), int(pair.key_len))
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		row := unsafe.Slice((*byte)(unsafe.Pointer(pair.value)), int(pair.value_len))
		// One map per row position, kept between scans. The map at
		// position i belongs to row i of this scan and DecodeInto
		// clears it, so nothing of the previous scan's row i is left
		// in it.
		if len(s.scanVals) <= len(got) {
			s.scanVals = append(s.scanVals, make(map[string][]byte, 16))
		}
		values, err := db.r.DecodeInto(row, fields, s.scanVals[len(got)])
		if err != nil {
			return nil, err
		}
		got = append(got, values)
	}
	s.scanRows = got
	return got, nil
}

// ScanEach is Scan with the maps taken out, the ycsb.EachScanner path.
//
// Same engine call and same borrowed scan buffer as Scan, and the only
// difference is what is built on top: one slice of column values that the
// session keeps, refilled a row and handed to the callback, instead of a
// map a row. A pooled map decode is 166 ns a row against 77 for the bare
// parse and a scan decodes fifty of them inside the call the harness
// times, so this is the larger half of workload E's driver cost. See
// tamnd/zu#750.
//
// The values point into the engine's scan buffer, so they are good until
// the callback returns and no longer, which is stricter than what Scan
// promises and is what lets one slice do the work of fifty maps.
func (db *zu2DB) ScanEach(ctx context.Context, table string, startKey string, count int, fields []string, fn func(values [][]byte) error) error {
	if count <= 0 {
		return nil
	}
	s := sessionOf(ctx)
	rk := s.rowKey(table, startKey)

	var pairs *C.zu2_pair
	var returned C.size_t
	st := C.zu2_scan(s.s, ptr(rk), C.size_t(len(rk)), C.size_t(count), &pairs, &returned)
	if st != C.ZU2_OK {
		return sessionErr(s.s, st, fmt.Sprintf("zu2: scan from %q failed", startKey))
	}
	if returned == 0 || pairs == nil {
		return nil
	}

	// The field set is fixed for the length of a run, so the positions
	// are built on the first scan of a session and then only when the
	// caller changes its mind.
	// The nil check is not redundant: an empty fields slice means every
	// field, and an empty one matches the empty slice a fresh session
	// starts with, so without it the first scan of a run that asks for
	// every field would find a cache that was never filled.
	if s.eachVals == nil || !sameFields(s.eachFields, fields) {
		s.eachPos = db.r.Positions(fields, s.eachPos)
		s.eachFields = append(s.eachFields[:0], fields...)
		n := len(fields)
		if n == 0 {
			n = len(s.eachPos)
		}
		s.eachVals = make([][]byte, n)
	}

	prefix := s.tablePrefix(table)
	for _, pair := range unsafe.Slice(pairs, int(returned)) {
		key := unsafe.Slice((*byte)(unsafe.Pointer(pair.key)), int(pair.key_len))
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		row := unsafe.Slice((*byte)(unsafe.Pointer(pair.value)), int(pair.value_len))
		values, err := db.r.DecodeEach(row, s.eachPos, s.eachVals)
		if err != nil {
			return err
		}
		if err := fn(values); err != nil {
			return err
		}
	}
	return nil
}

// sameFields is equality on the two slices without allocating, which is
// all the positions cache needs to know.
func sameFields(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (db *zu2DB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}
	if m == nil {
		m = make(map[string][]byte, len(values))
	}
	for field, value := range values {
		m[field] = value
	}
	return db.write(ctx, table, key, m)
}

func (db *zu2DB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.write(ctx, table, key, values)
}

// write encodes a row and upserts it. Insert and Update are the same
// call underneath, because zu2 has one write and it does not care
// whether the key was there.
func (db *zu2DB) write(ctx context.Context, table string, key string, values map[string][]byte) error {
	s := sessionOf(ctx)

	buf, err := db.r.Encode(s.buf[:0], values)
	if err != nil {
		return err
	}
	s.buf = buf

	rk := s.rowKey(table, key)
	st := C.zu2_upsert(s.s, ptr(rk), C.size_t(len(rk)), ptr(buf), C.size_t(len(buf)))
	if st != C.ZU2_OK {
		return sessionErr(s.s, st, fmt.Sprintf("zu2: upsert %q failed", key))
	}
	return nil
}

// BatchInsert stages n rows and writes them in one call.
//
// Two things are being saved. One is the crossing: cgo charges for
// every call into C, and a row at a time load pays it a million times
// for a million rows. The other is the wait, since a durable session
// waits for the device on every upsert, and a loader wants the batch on
// disk rather than each row on disk before the next one starts.
// zu2_upsert_many waits once for the whole array.
//
// The staging is not free, but it is the price of cgo's pointer rules
// rather than a choice: see carena.
func (db *zu2DB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	s := sessionOf(ctx)
	s.arena.reset()
	pairs := s.pairs.reserve(len(keys))
	off := s.off[:0]

	for i, key := range keys {
		buf, err := db.r.Encode(s.buf[:0], values[i])
		if err != nil {
			return err
		}
		s.buf = buf
		rk := s.rowKey(table, key)
		off = append(off, s.arena.put(rk), s.arena.put(buf))
		pairs[i].key_len = C.size_t(len(rk))
		pairs[i].value_len = C.size_t(len(buf))
	}
	s.off = off

	// Pointers last. The block moves while it fills, so anything taken
	// during the loop above would point into memory realloc has freed.
	for i := range keys {
		pairs[i].key = s.arena.at(off[2*i])
		pairs[i].value = s.arena.at(off[2*i+1])
	}

	var written C.size_t
	st := C.zu2_upsert_many(s.s, (*C.zu2_pair)(s.pairs.base), C.size_t(len(keys)), &written)
	if st != C.ZU2_OK {
		return sessionErr(s.s, st, fmt.Sprintf("zu2: batch of %d rows stopped after %d", len(keys), int(written)))
	}
	return nil
}

// The rest of BatchDB, a row at a time. Reads and deletes have nothing
// to batch here, since each one is its own probe, and an update is a
// read then a write. They are here because BatchDB is one interface and
// the harness asserts against the whole of it.

// BatchRead reads each key in turn. There is nothing to batch in the
// engine, since every key is its own probe, but the rows still all come
// back together, so each one needs a buffer and a map of its own. This
// used to call Read in a loop, and once Read started handing back
// storage the session reuses, that returned the same map len(keys) times
// with the last row in it. Nothing in the sweeps goes through here, since
// batch.size is set on the load and a load only inserts, but a read
// workload run with batch.size does.
func (db *zu2DB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	s := sessionOf(ctx)
	for len(s.batchVals) < len(keys) {
		s.batchRow = append(s.batchRow, nil)
		s.batchVals = append(s.batchVals, make(map[string][]byte, 16))
	}

	out := s.batchOut[:0]
	for i, k := range keys {
		row, buf, err := db.readOne(s, table, k, fields, s.batchRow[i], s.batchVals[i])
		s.batchRow[i] = buf
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	s.batchOut = out
	return out, nil
}

func (db *zu2DB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for i, k := range keys {
		if err := db.Update(ctx, table, k, values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (db *zu2DB) BatchDelete(ctx context.Context, table string, keys []string) error {
	for _, k := range keys {
		if err := db.Delete(ctx, table, k); err != nil {
			return err
		}
	}
	return nil
}

func (db *zu2DB) Delete(ctx context.Context, table string, key string) error {
	s := sessionOf(ctx)
	rk := s.rowKey(table, key)

	var existed C.int
	st := C.zu2_delete(s.s, ptr(rk), C.size_t(len(rk)), &existed)
	if st != C.ZU2_OK {
		return sessionErr(s.s, st, fmt.Sprintf("zu2: delete %q failed", key))
	}
	return nil
}

func (db *zu2DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Sessions first. They hold epoch slots, and the storage numbers
	// below are only honest once nothing is still writing.
	for _, s := range db.sessions {
		C.zu2_session_close(s.s)
		s.arena.free()
		s.pairs.free()
	}
	db.sessions = nil

	// An async run leaves a tail in memory, and a file measured before
	// that tail lands is smaller than the data it is holding.
	if st := C.zu2_sync(db.db); st != C.ZU2_OK {
		err := dbErr(db.db, st, "zu2: sync failed")
		C.zu2_close(db.db)
		db.db = nil
		return err
	}
	if db.compactEnd {
		var reclaimed C.uint64_t
		if st := C.zu2_compact(db.db, &reclaimed); st != C.ZU2_OK {
			err := dbErr(db.db, st, "zu2: compact failed")
			C.zu2_close(db.db)
			db.db = nil
			return err
		}
	}
	if db.report {
		db.printStorage()
	}

	C.zu2_close(db.db)
	db.db = nil
	return nil
}

// printStorage writes the two lines that make this run comparable on
// space as well as speed. Disk bytes and not file length: compaction
// punches holes, and a holed file still reports the length that counts
// them. Resident pages beside it, because the filesystem is only half
// the space question and reporting one without the other is picking
// whichever number reads better.
//
// The index count is slots in use and not keys stored. A zu2 bucket is
// eight slots with no overflow pointer, so a key that arrives at a full
// bucket takes a slot over and chains behind it in the log, and it stops
// owning a slot of its own. The line used to call them entries, which
// reads as records having gone missing: a load of 100000 printed 99793
// and the 207 were displaced rather than lost, which took a million
// reads over the same keys to establish (tamnd/zu#486). It says slots
// now, and it says how many of them carry more than one key, which is
// the crowding a read actually pays for.
//
// The index line is the one that explains a read number. Slots against
// buckets times eight is the load factor, and a crowded table is why a
// read walks a chain; growths say whether the table doubled under the
// run, which costs and which a sweep could previously only infer from
// the shape of a curve; resizing says a phase ended mid migration, which
// is a real state to be in and not the steady one.
func (db *zu2DB) printStorage() {
	var disk C.uint64_t
	if st := C.zu2_disk_bytes(db.db, &disk); st != C.ZU2_OK {
		fmt.Printf("zu2 storage: unavailable, %v\n", dbErr(db.db, st, "disk bytes failed"))
		return
	}
	written := uint64(C.zu2_log_bytes(db.db))
	span := uint64(C.zu2_log_span(db.db))
	occupancy := uint64(C.zu2_index_occupancy(db.db))
	foreign := uint64(C.zu2_index_foreign(db.db))
	buckets := uint64(C.zu2_index_buckets(db.db))
	grows := uint64(C.zu2_index_grows(db.db))
	resizing := uint32(C.zu2_index_resizing(db.db))
	resident := uint64(C.zu2_resident_pages(db.db))
	discarded := uint64(C.zu2_discarded(db.db))

	const mib = 1 << 20
	const pageBytes = 4 << 20
	fmt.Printf("zu2 storage: disk %.1f MiB, log span %.1f MiB, written %.1f MiB, resident %.1f MiB (%d pages)\n",
		float64(disk)/mib, float64(span)/mib, float64(written)/mib,
		float64(resident*pageBytes)/mib, resident)

	// The tier's share of that disk number, which is the context every
	// row measured with the tier on needs (tamnd/zu#600). How much of a
	// database has settled down there depends on how much of the
	// background compaction schedule the run happened to overlap, and it
	// came out anywhere from 2 to 35 percent across otherwise identical
	// runs. Two rows that settled differently were measured against two
	// different storage layouts and their latencies do not compare, so
	// this says which layout the row above it was taken on rather than
	// leaving it to be assumed.
	//
	// Migrated is the bytes a pass moved down there since the open and
	// span is what is down there now, and they are printed together
	// because a cold pass can take back everything it was given: a run
	// that migrated a great deal and ends with a small span did the work
	// and does not look like it.
	var cold C.uint64_t
	if st := C.zu2_cold_disk_bytes(db.db, &cold); st != C.ZU2_OK {
		fmt.Printf("zu2 tier: unavailable, %v\n", dbErr(db.db, st, "cold disk bytes failed"))
	} else if migrated := uint64(C.zu2_migrated(db.db)); migrated > 0 || cold > 0 {
		share := 0.0
		if disk > 0 {
			share = 100 * float64(cold) / float64(disk)
		}
		fmt.Printf("zu2 tier: cold %.1f MiB of %.1f MiB on disk (%.0f%%), span %.1f MiB, migrated %.1f MiB\n",
			float64(cold)/mib, float64(disk)/mib, share,
			float64(C.zu2_cold_span(db.db))/mib, float64(migrated)/mib)

		// What the coder bought on this run's data, which is the only
		// honest way to report it: a ratio measured on the values the
		// tier actually took rather than one quoted from the coder's
		// own corpus. Given is what the records were, stored is what
		// went on the device for them, and records reclaimed since are
		// in both, so this is a property of the workload and not of the
		// moment the line is printed. tamnd/zu#725.
		var given, stored C.uint64_t
		if st := C.zu2_cold_value_bytes(db.db, &given, &stored); st == C.ZU2_OK && given > 0 {
			fmt.Printf("zu2 tier coder: %.1f MiB of values stored in %.1f MiB, ratio %.4f\n",
				float64(given)/mib, float64(stored)/mib,
				float64(stored)/float64(given))
		}
	}

	slots := buckets * 8
	load := 0.0
	if slots > 0 {
		load = float64(occupancy) / float64(slots)
	}
	fmt.Printf("zu2 index: %d of %d slots in use, load %.2f, %d carrying more than one key, growths %d, resizing %t\n",
		occupancy, slots, load, foreign, grows, resizing != 0)

	// The count the sweep checks a load against, which the index line
	// above cannot give it. Slots are not keys: a displaced key lives on
	// somebody else's chain, so a load of 100000 prints 99793 slots with
	// nothing missing (tamnd/zu#486), and a harness comparing that to the
	// record count would report data loss on a healthy database. This is
	// keys, and it is the only engine side answer to the question
	// tamnd/zu#560 asks of every other engine here: the loader said it
	// wrote them, did they arrive. Deletes do not take it back down, so
	// it belongs to the load phase rather than to a run with deletes.
	fmt.Printf("zu2 rows: %d keys\n", uint64(C.zu2_index_keys(db.db)))

	// Only when there is a plane, and it is memory rather than disk: the
	// plane is rebuilt from the log at open and never written to it, so
	// it costs the process and not the device. Keys here is every key
	// the plane was ever told about, which is above the live count by
	// however many have been deleted, and saying so is the point of
	// printing both numbers next to each other.
	if planeBytes := uint64(C.zu2_ordered_bytes(db.db)); planeBytes > 0 {
		keys := uint64(C.zu2_ordered_keys(db.db))
		each := 0.0
		if keys > 0 {
			each = float64(planeBytes) / float64(keys)
		}
		fmt.Printf("zu2 scan plane: %.1f MiB of memory over %d keys, %.1f bytes a key\n",
			float64(planeBytes)/mib, keys, each)
	}

	// What the reads did to the cold tier. Zero on a run with no tier,
	// on a run with promotion turned off, and on a run whose reads never
	// reached the tier, and the three are different things: the first
	// two are settings this line does not repeat and the third is a
	// result. See zu2.promote.
	if promoted := uint64(C.zu2_promoted(db.db)); promoted > 0 {
		fmt.Printf("zu2 promotion: %d records moved out of the cold tier by a read\n", promoted)
	}

	// Only when there is something to say. A file with a hole in it does
	// not open at all unless the adapter asked to salvage it, so this is
	// zero on every ordinary run and printing a zero every time would
	// train a reader to skip the line that matters.
	if discarded > 0 {
		fmt.Printf("zu2 recovery: salvaged, %.1f MiB above a hole discarded\n",
			float64(discarded)/mib)
	}
}

func init() {
	ycsb.RegisterDBCreator("zu2", zu2Creator{})
}

var _ ycsb.DB = (*zu2DB)(nil)
var _ ycsb.BatchDB = (*zu2DB)(nil)
