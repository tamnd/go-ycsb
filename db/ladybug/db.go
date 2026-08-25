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

//go:build ladybug

// Package ladybug is the YCSB adapter for LadybugDB, the maintained fork
// of Kuzu. It runs in process through the C API, so there is no network
// hop in any measurement here.
//
// This is the rival that matters most. LadybugDB shares zu's entire
// design: embedded, columnar, factorized, property graph, single file,
// driven through a C API. Every other engine in the set differs from zu
// in architecture or plane, which gives a gap an easy excuse. There is
// no excuse available against this one.
//
// Build tag: ladybug. The cgo directives below cover a Homebrew keg on
// macOS and a release tarball unpacked into /usr/local on Linux. Any
// other layout is handed in at build time, since cgo cannot read the
// environment itself:
//
//	CGO_CFLAGS="-I$LBUG_INCLUDE" CGO_LDFLAGS="-L$LBUG_LIB -llbug" \
//	  go build -tags ladybug ./cmd/go-ycsb
package ladybug

/*
#cgo darwin CFLAGS: -I/opt/homebrew/opt/ladybug/include
#cgo darwin LDFLAGS: -L/opt/homebrew/opt/ladybug/lib -llbug -Wl,-rpath,/opt/homebrew/opt/ladybug/lib
#cgo linux CFLAGS: -I/usr/local/include
#cgo linux LDFLAGS: -L/usr/local/lib -llbug -lstdc++ -lm -ldl -Wl,-rpath,/usr/local/lib
#include <stdlib.h>
#include "lbug.h"
*/
import "C"

import (
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

// LadybugDB properties. Everything the system config exposes that can
// move throughput is reachable from a property, so the fastest setting is
// something the run records rather than something compiled in.
const (
	ladybugDBPath      = "ladybug.dbpath"
	ladybugBufPool     = "ladybug.buffer_pool_size"
	ladybugThreads     = "ladybug.threads"
	ladybugCheckpoint  = "ladybug.checkpoint_threshold"
	ladybugChecksums   = "ladybug.enable_checksums"
	ladybugCompression = "ladybug.enable_compression"
	ladybugMultiWrites = "ladybug.enable_multi_writes"
	ladybugHashIndex   = "ladybug.enable_default_hash_index"
	ladybugReadShape   = "ladybug.readshape"
)

type ladybugCreator struct{}

// conn is one LadybugDB connection plus the prepared statements compiled
// on it. A connection is not safe to use from two goroutines at once, so
// each worker takes one for the length of the run and never shares it.
type conn struct {
	c    C.lbug_connection
	prep map[string]*C.lbug_prepared_statement
}

type ladybugDB struct {
	p       *properties.Properties
	verbose bool

	db C.lbug_database

	fieldCount int64
	table      string
	readShape  string

	mu    sync.Mutex
	conns []*conn // every connection handed out, so Close can free them
}

// ctxKey is the type used for the per worker connection in the context.
type ctxKey struct{}

func (c ladybugCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(ladybugDB)
	d.p = p
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	d.table = p.GetString(prop.TableName, prop.TableNameDefault)
	d.readShape = p.GetString(ladybugReadShape, "where")

	path := p.GetString(ladybugDBPath, "/tmp/ladybug.lbug")
	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		// LadybugDB wants to create the database directory itself, so it
		// has to be gone rather than empty.
		os.RemoveAll(path)
	}

	sysCfg := C.lbug_default_system_config()
	if v := p.GetInt64(ladybugBufPool, 0); v > 0 {
		sysCfg.buffer_pool_size = C.uint64_t(v)
	}
	if v := p.GetInt64(ladybugThreads, 0); v > 0 {
		sysCfg.max_num_threads = C.uint64_t(v)
	}
	if v := p.GetInt64(ladybugCheckpoint, 0); v > 0 {
		sysCfg.checkpoint_threshold = C.uint64_t(v)
	}
	// The three booleans below are left at whatever LadybugDB itself
	// chose unless the run asks for something else. Picking a value here
	// would mean quoting a number the engine's own users never see, and
	// enable_multi_writes in particular costs around a fifth of single
	// thread insert throughput, so turning it on by default would hand
	// the engine a penalty it does not deserve at one thread.
	setBool(p, ladybugChecksums, &sysCfg.enable_checksums)
	setBool(p, ladybugCompression, &sysCfg.enable_compression)
	setBool(p, ladybugMultiWrites, &sysCfg.enable_multi_writes)
	// The hash index on the primary key is the exception. It stays on
	// because without it every read is a label scan, which is not the
	// engine anybody runs, and it is the capability this comparison
	// exists to measure against.
	sysCfg.enable_default_hash_index = C.bool(p.GetBool(ladybugHashIndex, true))

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	if state := C.lbug_database_init(cpath, sysCfg, &d.db); state != C.LbugSuccess {
		// The C API gives back a state and no message on this call, so
		// the only account of why is what was asked for and what the
		// machine had. It is worth carrying: the buffer pool defaults to
		// a share of memory, and an init that fails on a machine with
		// something large already resident fails here with nothing said.
		// The gamingpc sweep of 2026-08-22 lost all three ladybug passes
		// this way, next to a vLLM server holding seven gigabytes.
		return nil, fmt.Errorf(
			"ladybug: database init %q failed, buffer pool %d bytes, %s",
			path, uint64(sysCfg.buffer_pool_size), memoryAvailable(),
		)
	}

	// The schema goes in on its own connection before any worker starts.
	setup, err := d.newConn()
	if err != nil {
		C.lbug_database_destroy(&d.db)
		return nil, err
	}
	if err := d.createTable(setup); err != nil {
		C.lbug_database_destroy(&d.db)
		return nil, err
	}

	return d, nil
}

// memoryAvailable is what the kernel says is there for the asking, for
// the init error to carry. Linux only, since that is where the sweeps
// run, and it says so rather than guessing anywhere else.
func memoryAvailable() string {
	meminfo, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return "memory available unknown"
	}
	for _, line := range strings.Split(string(meminfo), "\n") {
		if after, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			return "MemAvailable " + strings.TrimSpace(after)
		}
	}
	return "memory available unknown"
}

// setBool writes a property into a C bool only when the run actually set
// it, so an unset property leaves the engine's own default alone.
func setBool(p *properties.Properties, key string, dst *C.bool) {
	if _, ok := p.Get(key); !ok {
		return
	}
	*dst = C.bool(p.GetBool(key, false))
}

// newConn opens a connection and records it so Close can free it.
func (db *ladybugDB) newConn() (*conn, error) {
	c := &conn{prep: make(map[string]*C.lbug_prepared_statement)}
	if state := C.lbug_connection_init(&db.db, &c.c); state != C.LbugSuccess {
		return nil, fmt.Errorf("ladybug: connection init failed")
	}
	db.mu.Lock()
	db.conns = append(db.conns, c)
	db.mu.Unlock()
	return c, nil
}

func (db *ladybugDB) createTable(c *conn) error {
	// The primary key is the whole point. It is what gives LadybugDB a
	// real index on the key, so read, update and delete are seeks rather
	// than label scans. This is Kuzu's DDL dialect rather than GQL, which
	// is worth knowing when the same schema is expressed for zu.
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE NODE TABLE IF NOT EXISTS %s(ycsb_key STRING PRIMARY KEY", db.table)
	for i := int64(0); i < db.fieldCount; i++ {
		fmt.Fprintf(&b, ", field%d STRING", i)
	}
	b.WriteString(")")

	if db.verbose {
		fmt.Println(b.String())
	}
	_, err := db.query(c, b.String())
	return err
}

// query runs a statement with no parameters and discards the rows. It is
// only used for schema work, never on a timed path.
func (db *ladybugDB) query(c *conn, text string) (int, error) {
	ct := C.CString(text)
	defer C.free(unsafe.Pointer(ct))

	var qr C.lbug_query_result
	if state := C.lbug_connection_query(&c.c, ct, &qr); state != C.LbugSuccess {
		return 0, resultErr(&qr, text)
	}
	if !C.lbug_query_result_is_success(&qr) {
		return 0, resultErr(&qr, text)
	}
	n := 0
	for C.lbug_query_result_has_next(&qr) {
		var ft C.lbug_flat_tuple
		if C.lbug_query_result_get_next(&qr, &ft) != C.LbugSuccess {
			break
		}
		n++
		C.lbug_flat_tuple_destroy(&ft)
	}
	C.lbug_query_result_destroy(&qr)
	return n, nil
}

// resultErr pulls the message off a failed result and frees the result.
func resultErr(qr *C.lbug_query_result, text string) error {
	msg := C.lbug_query_result_get_error_message(qr)
	err := fmt.Errorf("ladybug: %s: %s", C.GoString(msg), text)
	C.lbug_destroy_string(msg)
	C.lbug_query_result_destroy(qr)
	return err
}

// prepared returns the compiled statement for text on this connection,
// compiling it the first time. Statements are cached because compiling
// per operation would measure the planner rather than the engine.
func (c *conn) prepared(text string) (*C.lbug_prepared_statement, error) {
	if s, ok := c.prep[text]; ok {
		return s, nil
	}
	stmt := new(C.lbug_prepared_statement)
	ct := C.CString(text)
	defer C.free(unsafe.Pointer(ct))

	if state := C.lbug_connection_prepare(&c.c, ct, stmt); state != C.LbugSuccess {
		C.lbug_prepared_statement_destroy(stmt)
		return nil, fmt.Errorf("ladybug: prepare failed: %s", text)
	}
	if !C.lbug_prepared_statement_is_success(stmt) {
		msg := C.lbug_prepared_statement_get_error_message(stmt)
		err := fmt.Errorf("ladybug: prepare: %s: %s", C.GoString(msg), text)
		C.lbug_destroy_string(msg)
		C.lbug_prepared_statement_destroy(stmt)
		return nil, err
	}
	c.prep[text] = stmt
	return stmt, nil
}

// bindString binds a named string parameter.
func bindString(stmt *C.lbug_prepared_statement, name, val string) error {
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	cv := C.CString(val)
	defer C.free(unsafe.Pointer(cv))
	if C.lbug_prepared_statement_bind_string(stmt, cn, cv) != C.LbugSuccess {
		return fmt.Errorf("ladybug: bind %s failed", name)
	}
	return nil
}

// exec runs a prepared statement and returns the rows as field maps.
// cols names the output columns in order, which the caller knows because
// it wrote the RETURN clause; asking the result for them per operation
// would be a needless allocation on a timed path.
func (db *ladybugDB) exec(c *conn, stmt *C.lbug_prepared_statement, cols []string, limit int) ([]map[string][]byte, error) {
	var qr C.lbug_query_result
	if state := C.lbug_connection_execute(&c.c, stmt, &qr); state != C.LbugSuccess {
		return nil, resultErr(&qr, "execute")
	}
	if !C.lbug_query_result_is_success(&qr) {
		return nil, resultErr(&qr, "execute")
	}
	defer C.lbug_query_result_destroy(&qr)

	if len(cols) == 0 {
		// A write. Drain and return nothing.
		for C.lbug_query_result_has_next(&qr) {
			var ft C.lbug_flat_tuple
			if C.lbug_query_result_get_next(&qr, &ft) != C.LbugSuccess {
				break
			}
			C.lbug_flat_tuple_destroy(&ft)
		}
		return nil, nil
	}

	out := make([]map[string][]byte, 0, limit)
	for C.lbug_query_result_has_next(&qr) {
		var ft C.lbug_flat_tuple
		if C.lbug_query_result_get_next(&qr, &ft) != C.LbugSuccess {
			break
		}
		m := make(map[string][]byte, len(cols))
		for i, name := range cols {
			var v C.lbug_value
			if C.lbug_flat_tuple_get_value(&ft, C.uint64_t(i), &v) != C.LbugSuccess {
				continue
			}
			if !C.lbug_value_is_null(&v) {
				var cs *C.char
				if C.lbug_value_get_string(&v, &cs) == C.LbugSuccess {
					// GoBytes copies, which matters: the tuple and the
					// string are both reused or freed underneath us.
					m[name] = []byte(C.GoString(cs))
					C.lbug_destroy_string(cs)
				}
			}
			C.lbug_value_destroy(&v)
		}
		out = append(out, m)
		C.lbug_flat_tuple_destroy(&ft)
	}
	return out, nil
}

// fieldNames returns the fields to project, defaulting to all of them
// when the workload did not name a subset.
func (db *ladybugDB) fieldNames(fields []string) []string {
	if len(fields) > 0 {
		return fields
	}
	all := make([]string, 0, db.fieldCount)
	for i := int64(0); i < db.fieldCount; i++ {
		all = append(all, fmt.Sprintf("field%d", i))
	}
	return all
}

func (db *ladybugDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, c := range db.conns {
		for _, s := range c.prep {
			C.lbug_prepared_statement_destroy(s)
		}
		c.prep = nil
		C.lbug_connection_destroy(&c.c)
	}
	db.conns = nil
	C.lbug_database_destroy(&db.db)
	return nil
}

// InitThread gives each worker its own connection. Sharing one across
// goroutines is not allowed, and serialising on a mutex would make the
// concurrency sweep measure the mutex.
func (db *ladybugDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	c, err := db.newConn()
	if err != nil {
		panic(err)
	}
	return context.WithValue(ctx, ctxKey{}, c)
}

func (db *ladybugDB) CleanupThread(_ context.Context) {}

func connOf(ctx context.Context) *conn {
	c, _ := ctx.Value(ctxKey{}).(*conn)
	return c
}

// matchPoint writes the MATCH clause for a lookup on the primary key.
//
// Two shapes say the same thing and they do not cost the same. The
// property is declared as ycsb_key STRING PRIMARY KEY, so the engine has
// a hash index on it, but a scan sweep showed the WHERE form not using
// it: read p50 went 358 us at 10000 records to 6039 us at 100000, which
// is a straight line and not an index. Raising buffer_pool_size to 8 GB
// changed nothing, so it was the plan and not the cache. The pattern
// form is the other way to ask, and it is a property rather than a
// hardcoded choice so both can be measured against each other on the
// same build.
func (db *ladybugDB) matchPoint(b *strings.Builder, table string) {
	if db.readShape == "where" {
		fmt.Fprintf(b, "MATCH (u:%s) WHERE u.ycsb_key = $k ", table)
		return
	}
	fmt.Fprintf(b, "MATCH (u:%s {ycsb_key: $k}) ", table)
}

func (db *ladybugDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	c := connOf(ctx)
	cols := db.fieldNames(fields)

	var b strings.Builder
	db.matchPoint(&b, table)
	b.WriteString("RETURN ")
	for i, f := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s AS %s", f, f)
	}

	stmt, err := c.prepared(b.String())
	if err != nil {
		return nil, err
	}
	if err := bindString(stmt, "k", key); err != nil {
		return nil, err
	}
	rows, err := db.exec(c, stmt, cols, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (db *ladybugDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	c := connOf(ctx)
	cols := db.fieldNames(fields)

	// ORDER BY is required, not decorative. A YCSB scan is the next count
	// records in key order, and no engine here is obliged to return them
	// in that order unless asked. The limit is formatted in because it
	// changes per operation and a prepared statement per distinct limit
	// would defeat the statement cache; it is an int from the generator.
	var b strings.Builder
	fmt.Fprintf(&b, "MATCH (u:%s) WHERE u.ycsb_key >= $k RETURN ", table)
	for i, f := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s AS %s", f, f)
	}
	fmt.Fprintf(&b, " ORDER BY u.ycsb_key LIMIT %d", count)

	stmt, err := c.prepared(b.String())
	if err != nil {
		return nil, err
	}
	if err := bindString(stmt, "k", startKey); err != nil {
		return nil, err
	}
	return db.exec(c, stmt, cols, count)
}

func (db *ladybugDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	c := connOf(ctx)

	pairs := util.NewFieldPairs(values)
	var b strings.Builder
	db.matchPoint(&b, table)
	b.WriteString("SET ")
	for i, p := range pairs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s = $%s", p.Field, p.Field)
	}

	stmt, err := c.prepared(b.String())
	if err != nil {
		return err
	}
	if err := bindString(stmt, "k", key); err != nil {
		return err
	}
	for _, p := range pairs {
		if err := bindString(stmt, p.Field, string(p.Value)); err != nil {
			return err
		}
	}
	_, err = db.exec(c, stmt, nil, 0)
	return err
}

func (db *ladybugDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	c := connOf(ctx)

	pairs := util.NewFieldPairs(values)
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE (u:%s {ycsb_key: $k", table)
	for _, p := range pairs {
		fmt.Fprintf(&b, ", %s: $%s", p.Field, p.Field)
	}
	b.WriteString("})")

	stmt, err := c.prepared(b.String())
	if err != nil {
		return err
	}
	if err := bindString(stmt, "k", key); err != nil {
		return err
	}
	for _, p := range pairs {
		if err := bindString(stmt, p.Field, string(p.Value)); err != nil {
			return err
		}
	}
	_, err = db.exec(c, stmt, nil, 0)
	return err
}

func (db *ladybugDB) Delete(ctx context.Context, table string, key string) error {
	c := connOf(ctx)

	var b strings.Builder
	db.matchPoint(&b, table)
	b.WriteString("DELETE u")
	stmt, err := c.prepared(b.String())
	if err != nil {
		return err
	}
	if err := bindString(stmt, "k", key); err != nil {
		return err
	}
	_, err = db.exec(c, stmt, nil, 0)
	return err
}

func init() {
	ycsb.RegisterDBCreator("ladybug", ladybugCreator{})
}

var _ ycsb.DB = (*ladybugDB)(nil)
