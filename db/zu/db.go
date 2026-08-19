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

//go:build zu

// Package zu is the YCSB adapter for zu, over libzu.
//
// The database opens inside the harness process and every operation is a
// direct call into the library, so nothing in a timed region crosses a
// process boundary or a socket. That is the same plane LadybugDB runs on,
// which is what makes the two comparable at all.
//
// Every statement here is standard GQL. INSERT is the GQL node insertion
// clause, not Cypher's CREATE, and zu says so when asked: CREATE and
// MERGE come back with "not implemented yet, the v0 core is MATCH, WHERE,
// CALL, UNWIND, WITH, RETURN". Writing Cypher here and calling it GQL
// would put the adapter in the position of measuring a dialect the engine
// does not claim.
//
// Build tag: zu. The cgo lines default to a sibling zu checkout built in
// release mode, which is the layout the repos are developed in. cgo
// cannot read the environment, so anything else overrides at build time:
//
//	CGO_CFLAGS="-I$ZU_INCLUDE" \
//	CGO_LDFLAGS="-L$ZU_LIB -lzu -Wl,-rpath,$ZU_LIB" \
//	  go build -tags zu ./cmd/go-ycsb
//
// Build libzu first with cargo build --release -p zu-capi in the zu repo.
package zu

/*
#cgo CFLAGS: -I${SRCDIR}/../../../zu/crates/zu-capi/include
#cgo LDFLAGS: -L${SRCDIR}/../../../zu/target/release -lzu -Wl,-rpath,${SRCDIR}/../../../zu/target/release
#include <stdlib.h>
#include "zu.h"
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

// zu properties.
const (
	zuDBPath = "zu.dbpath"
	zuSchema = "zu.schema"
	zuGraph  = "zu.graph"
)

type zuCreator struct{}

// conn is one libzu connection plus the statements compiled on it. A
// statement belongs to the connection that compiled it, so the cache
// lives here. A connection may move between threads but must not be in
// two callers' hands at once, which is why each worker keeps its own for
// the length of the run.
type conn struct {
	c     *C.zu_conn
	stmts map[string]*C.zu_stmt
}

type zuDB struct {
	p       *properties.Properties
	verbose bool

	dbPath     string
	fieldCount int64
	table      string

	mu    sync.Mutex
	conns []*conn // every connection opened, so Close can free them
}

type ctxKey struct{}

func (zuCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(zuDB)
	d.p = p
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	d.table = p.GetString(prop.TableName, prop.TableNameDefault)
	d.dbPath = p.GetString(zuDBPath, "/tmp/ycsb.zu1")

	fresh := p.GetBool(prop.DropData, prop.DropDataDefault)
	if fresh {
		os.RemoveAll(d.dbPath)
	}
	if _, err := os.Stat(d.dbPath); os.IsNotExist(err) {
		fresh = true
	}

	c, err := d.newConn(fresh)
	if err != nil {
		return nil, err
	}
	if fresh {
		if err := d.createCatalog(c, p); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// createCatalog puts the schema and graph in place. This is the GQL
// catalog DDL from ISO/IEC 39075:2024, which is what zu speaks. There is
// no CREATE TABLE and no CREATE INDEX in GQL, and no PRIMARY KEY either,
// so unlike every other engine here the key gets no declared index and
// nothing is told that ycsb_key identifies a row.
func (db *zuDB) createCatalog(c *conn, p *properties.Properties) error {
	schema := p.GetString(zuSchema, "/ycsb")
	graph := p.GetString(zuGraph, schema+"/g")

	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema),
		fmt.Sprintf("CREATE GRAPH IF NOT EXISTS %s", graph),
	}
	stmts = append(stmts, db.declareStmts()...)

	for _, q := range stmts {
		if db.verbose {
			fmt.Println(q)
		}
		if err := c.execDiscard(q); err != nil {
			return err
		}
	}
	return nil
}

// declareStmts writes one row and takes it away again, which is how a
// node table comes into existence in zu. GQL has no table DDL, so a table
// is declared by the first INSERT that names it and its column types are
// read off the literals in that pattern. Doing it here rather than
// letting the first worker do it is not a convenience: zu_prepare_z binds
// against the catalog, so preparing an INSERT into a table that does not
// exist yet fails with "no node table is named usertable", and every
// worker prepares before it inserts.
//
// The seed row uses empty strings, which fixes every column as a string,
// and its key is one no YCSB generator produces. It is deleted on the
// next line either way, before anything is measured.
func (db *zuDB) declareStmts() []string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT (:%s {ycsb_key: ''", db.table)
	for i := int64(0); i < db.fieldCount; i++ {
		fmt.Fprintf(&b, ", field%d: ''", i)
	}
	b.WriteString("})")

	return []string{
		b.String(),
		fmt.Sprintf("MATCH (u:%s) WHERE u.ycsb_key = '' DETACH DELETE u", db.table),
	}
}

// newConn opens or creates a connection and records it for Close.
func (db *zuDB) newConn(create bool) (*conn, error) {
	cpath := C.CString(db.dbPath)
	defer C.free(unsafe.Pointer(cpath))

	var c *C.zu_conn
	var cerr *C.zu_error
	var st C.zu_status
	if create {
		st = C.zu_create_z(cpath, &c, &cerr)
	} else {
		st = C.zu_open_z(cpath, &c, &cerr)
	}
	if st != C.ZU_OK {
		return nil, takeErr(st, cerr, fmt.Sprintf("zu: open %q failed", db.dbPath))
	}

	out := &conn{c: c, stmts: make(map[string]*C.zu_stmt)}
	db.mu.Lock()
	db.conns = append(db.conns, out)
	db.mu.Unlock()
	return out, nil
}

// execDiscard runs one statement through the one-shot entry point and
// throws the result away. Catalog DDL goes through here rather than
// zu_prepare_z, which plans what it is handed as a query.
func (c *conn) execDiscard(q string) error {
	cq := C.CString(q)
	defer C.free(unsafe.Pointer(cq))

	var res *C.zu_result
	var cerr *C.zu_error
	if st := C.zu_query_z(c.c, cq, &res, &cerr); st != C.ZU_OK {
		return takeErr(st, cerr, fmt.Sprintf("zu: %q failed", q))
	}
	C.zu_result_free(res)
	return nil
}

// prepared returns this connection's compiled statement for text,
// compiling on first sighting. Compiling per operation would put the
// planner in the measurement.
func (c *conn) prepared(text string) (*C.zu_stmt, error) {
	if s, ok := c.stmts[text]; ok {
		return s, nil
	}
	ctext := C.CString(text)
	defer C.free(unsafe.Pointer(ctext))

	var stmt *C.zu_stmt
	var cerr *C.zu_error
	if st := C.zu_prepare_z(c.c, ctext, &stmt, &cerr); st != C.ZU_OK {
		return nil, takeErr(st, cerr, fmt.Sprintf("zu: prepare %q failed", text))
	}
	c.stmts[text] = stmt
	return stmt, nil
}

// bindStr binds one named string parameter. Bindings live on the
// statement across executions, so rebinding a name replaces the value.
func bindStr(stmt *C.zu_stmt, name, val string) error {
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	cv := C.CString(val)
	defer C.free(unsafe.Pointer(cv))
	if st := C.zu_bind_str_z(stmt, cn, cv); st != C.ZU_OK {
		return fmt.Errorf("zu: bind %q failed with status %d", name, int(st))
	}
	return nil
}

func bindI64(stmt *C.zu_stmt, name string, val int64) error {
	cn := C.CString(name)
	defer C.free(unsafe.Pointer(cn))
	if st := C.zu_bind_i64_z(stmt, cn, C.int64_t(val)); st != C.ZU_OK {
		return fmt.Errorf("zu: bind %q failed with status %d", name, int(st))
	}
	return nil
}

// exec runs a prepared statement and decodes string columns into field
// maps. cols names the output columns in order, which the caller knows
// because it wrote the RETURN clause. Passing nil means the statement is
// a write and the result is drained without being read.
func (c *conn) exec(stmt *C.zu_stmt, cols []string, capacity int) ([]map[string][]byte, error) {
	var res *C.zu_result
	var cerr *C.zu_error
	if st := C.zu_execute(stmt, &res, &cerr); st != C.ZU_OK {
		return nil, takeErr(st, cerr, "zu: execute failed")
	}
	defer C.zu_result_free(res)

	if cols == nil {
		return nil, nil
	}

	rows := int(C.zu_result_rows(res))
	ncols := int(C.zu_result_cols(res))
	if ncols > len(cols) {
		ncols = len(cols)
	}

	out := make([]map[string][]byte, rows)
	for r := range out {
		out[r] = make(map[string][]byte, ncols)
	}
	// Column at a time on the outside, because the string accessor is
	// per cell either way and this keeps the name lookup out of the
	// inner loop.
	for col := 0; col < ncols; col++ {
		name := cols[col]
		for r := 0; r < rows; r++ {
			var p *C.char
			var n C.size_t
			if st := C.zu_result_cell_str(res, C.uint64_t(r), C.uint32_t(col), &p, &n); st != C.ZU_OK || p == nil {
				continue // a null cell, which YCSB reads as an absent field
			}
			out[r][name] = []byte(C.GoStringN(p, C.int(n)))
		}
	}
	return out, nil
}

// close tears down the statements then the connection. Statements go
// first, the header requires it.
func (c *conn) close() {
	for _, s := range c.stmts {
		C.zu_stmt_close(s)
	}
	c.stmts = nil
	if c.c != nil {
		C.zu_conn_close(c.c)
		c.c = nil
	}
}

// takeErr turns a failed status and libzu's error handle into a Go error
// and releases the handle.
func takeErr(st C.zu_status, cerr *C.zu_error, fallback string) error {
	if cerr == nil {
		return fmt.Errorf("%s (status %d)", fallback, int(st))
	}
	defer C.zu_error_free(cerr)
	var n C.size_t
	p := C.zu_error_message(cerr, &n)
	if p == nil {
		return fmt.Errorf("%s (status %d)", fallback, int(st))
	}
	return fmt.Errorf("zu: %s", C.GoStringN(p, C.int(n)))
}

func (db *zuDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, c := range db.conns {
		c.close()
	}
	db.conns = nil
	return nil
}

// InitThread gives each worker its own connection. libzu answers
// ZU_MISUSE_CONCURRENT rather than corrupting a cache when one connection
// is used from two threads at once, so sharing is not an option, and
// serialising on a mutex would make the concurrency sweep measure the
// mutex.
func (db *zuDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	c, err := db.newConn(false)
	if err != nil {
		panic(err)
	}
	return context.WithValue(ctx, ctxKey{}, c)
}

func (db *zuDB) CleanupThread(_ context.Context) {}

func connOf(ctx context.Context) *conn {
	c, _ := ctx.Value(ctxKey{}).(*conn)
	return c
}

// fieldNames returns the fields to project, defaulting to all of them.
func (db *zuDB) fieldNames(fields []string) []string {
	if len(fields) > 0 {
		return fields
	}
	all := make([]string, 0, db.fieldCount)
	for i := int64(0); i < db.fieldCount; i++ {
		all = append(all, fmt.Sprintf("field%d", i))
	}
	return all
}

// returnClause builds "u.field0 AS field0, u.field1 AS field1".
func returnClause(fields []string) string {
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s AS %s", f, f)
	}
	return b.String()
}

func (db *zuDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	c := connOf(ctx)
	cols := db.fieldNames(fields)
	q := fmt.Sprintf("MATCH (u:%s) WHERE u.ycsb_key = $k RETURN %s", table, returnClause(cols))

	stmt, err := c.prepared(q)
	if err != nil {
		return nil, err
	}
	if err := bindStr(stmt, "k", key); err != nil {
		return nil, err
	}
	rows, err := c.exec(stmt, cols, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (db *zuDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	c := connOf(ctx)
	cols := db.fieldNames(fields)
	// The limit is a parameter rather than formatted in, so the statement
	// cache holds one entry for every scan instead of one per distinct
	// count. ORDER BY is part of the operation: a YCSB scan is the next
	// count records in key order and nothing is obliged to return them
	// that way unless asked.
	q := fmt.Sprintf("MATCH (u:%s) WHERE u.ycsb_key >= $k RETURN %s ORDER BY u.ycsb_key LIMIT $n",
		table, returnClause(cols))

	stmt, err := c.prepared(q)
	if err != nil {
		return nil, err
	}
	if err := bindStr(stmt, "k", startKey); err != nil {
		return nil, err
	}
	if err := bindI64(stmt, "n", int64(count)); err != nil {
		return nil, err
	}
	return c.exec(stmt, cols, count)
}

func (db *zuDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	c := connOf(ctx)

	pairs := util.NewFieldPairs(values)
	var b strings.Builder
	fmt.Fprintf(&b, "MATCH (u:%s) WHERE u.ycsb_key = $k SET ", table)
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
	if err := bindStr(stmt, "k", key); err != nil {
		return err
	}
	for _, p := range pairs {
		if err := bindStr(stmt, p.Field, string(p.Value)); err != nil {
			return err
		}
	}
	_, err = c.exec(stmt, nil, 0)
	return err
}

func (db *zuDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	c := connOf(ctx)

	// INSERT, not CREATE. INSERT is the GQL node insertion clause and it
	// is what zu implements; CREATE is Cypher and comes back as not
	// implemented, which is the correct answer for a GQL engine.
	pairs := util.NewFieldPairs(values)
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT (:%s {ycsb_key: $k", table)
	for _, p := range pairs {
		fmt.Fprintf(&b, ", %s: $%s", p.Field, p.Field)
	}
	b.WriteString("})")

	stmt, err := c.prepared(b.String())
	if err != nil {
		return err
	}
	if err := bindStr(stmt, "k", key); err != nil {
		return err
	}
	for _, p := range pairs {
		if err := bindStr(stmt, p.Field, string(p.Value)); err != nil {
			return err
		}
	}
	_, err = c.exec(stmt, nil, 0)
	return err
}

func (db *zuDB) Delete(ctx context.Context, table string, key string) error {
	c := connOf(ctx)

	q := fmt.Sprintf("MATCH (u:%s) WHERE u.ycsb_key = $k DETACH DELETE u", table)
	stmt, err := c.prepared(q)
	if err != nil {
		return err
	}
	if err := bindStr(stmt, "k", key); err != nil {
		return err
	}
	_, err = c.exec(stmt, nil, 0)
	return err
}

func init() {
	ycsb.RegisterDBCreator("zu", zuCreator{})
}

var _ ycsb.DB = (*zuDB)(nil)
