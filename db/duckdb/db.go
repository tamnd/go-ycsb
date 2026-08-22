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

//go:build duckdb

package duckdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/magiconair/properties"
	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// DuckDB properties.
const (
	duckdbDBPath         = "duckdb.dbpath"
	duckdbMemory         = "duckdb.memory"
	duckdbThreads        = "duckdb.threads"
	duckdbMemoryLimit    = "duckdb.memory_limit"
	duckdbMaxOpenConns   = "duckdb.maxopenconns"
	duckdbMaxIdleConns   = "duckdb.maxidleconns"
	duckdbRetries        = "duckdb.retries"
	duckdbRetryBackoffMs = "duckdb.retry_backoff_ms"
)

type duckdbCreator struct{}

type duckdbDB struct {
	p       *properties.Properties
	db      *sql.DB
	verbose bool

	retries   int
	backoffMs int

	bufPool *util.BufPool
}

func (c duckdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(duckdbDB)
	d.p = p

	dbPath := p.GetString(duckdbDBPath, "/tmp/duckdb.db")
	if p.GetBool(duckdbMemory, false) {
		// An in-memory database is a useful upper bound on what the engine
		// can do with the storage layer removed. It is a labelled row, not
		// the default, because the other engines are measured on a file.
		dbPath = ""
	} else if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dbPath)
		os.RemoveAll(dbPath + ".wal")
	}

	// DuckDB takes its configuration in the DSN. Only settings that were
	// asked for are sent, so the engine keeps its own defaults otherwise,
	// which is what the fairness rule in the spec requires.
	var opts []string
	if v := p.GetString(duckdbThreads, ""); v != "" {
		opts = append(opts, "threads="+v)
	}
	if v := p.GetString(duckdbMemoryLimit, ""); v != "" {
		opts = append(opts, "memory_limit="+v)
	}
	dsn := dbPath
	if len(opts) > 0 {
		dsn = dbPath + "?" + strings.Join(opts, "&")
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}

	// DuckDB is a single writer engine with optimistic concurrency: two
	// transactions touching the same row make one of them fail rather than
	// block, which is why the retry loop below exists.
	//
	// The pool used to default to one connection to keep conflict retries
	// out of the numbers. That was the wrong trade. A pool of one does not
	// remove concurrency from the measurement, it moves the serialisation
	// into database/sql where it is invisible, and it costs a great deal:
	// at 10000 records on workload C, read ops/s at 1, 4 and 16 threads
	// went 1549, 1514, 1516 with one connection and 1560, 5823, 14908 with
	// the pool at threadcount. A flat line across four doublings looks
	// like an engine that does not scale and it was the adapter.
	threads := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxOpenConns(p.GetInt(duckdbMaxOpenConns, threads))
	db.SetMaxIdleConns(p.GetInt(duckdbMaxIdleConns, threads))

	d.db = db
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.retries = p.GetInt(duckdbRetries, 10)
	d.backoffMs = p.GetInt(duckdbRetryBackoffMs, 2)
	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		db.Close()
		return nil, err
	}

	return d, nil
}

func (db *duckdbDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	buf := new(bytes.Buffer)
	// The primary key is what gives DuckDB an ART index on the key, so
	// reads are a seek rather than a scan. Without it this measures a
	// columnar full scan per operation and says nothing useful.
	fmt.Fprintf(buf, "CREATE TABLE IF NOT EXISTS %s (YCSB_KEY VARCHAR PRIMARY KEY", tableName)
	for i := int64(0); i < fieldCount; i++ {
		fmt.Fprintf(buf, ", FIELD%d VARCHAR", i)
	}
	buf.WriteString(")")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.Exec(buf.String())
	return err
}

func (db *duckdbDB) Close() error {
	if db.db == nil {
		return nil
	}
	return db.db.Close()
}

func (db *duckdbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *duckdbDB) CleanupThread(_ context.Context) {}

// isConflict reports whether an error is DuckDB rejecting a transaction
// that lost an optimistic concurrency race, as opposed to a real failure.
// The driver surfaces these as text, so this matches on the message.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "TransactionContext Error") ||
		strings.Contains(s, "Conflict on tuple deletion") ||
		strings.Contains(s, "write-write conflict")
}

func (db *duckdbDB) tx(ctx context.Context, f func(tx *sql.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt <= db.retries; attempt++ {
		tx, err := db.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if err = f(tx); err != nil {
			tx.Rollback()
			if isConflict(err) {
				lastErr = err
				time.Sleep(time.Duration(db.backoffMs) * time.Millisecond)
				continue
			}
			return err
		}

		if err = tx.Commit(); err != nil {
			if isConflict(err) {
				lastErr = err
				time.Sleep(time.Duration(db.backoffMs) * time.Millisecond)
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func (db *duckdbDB) doQueryRows(ctx context.Context, tx *sql.Tx, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vs := make([]map[string][]byte, 0, count)
	for rows.Next() {
		m := make(map[string][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range cols {
			dest[i] = new([]byte)
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		for i, v := range dest {
			m[cols[i]] = *v.(*[]byte)
		}
		vs = append(vs, m)
	}

	return vs, rows.Err()
}

func (db *duckdbDB) doRead(ctx context.Context, tx *sql.Tx, table string, key string, fields []string) (map[string][]byte, error) {
	sel := "*"
	if len(fields) > 0 {
		sel = strings.Join(fields, ",")
	}
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = ?`, sel, table)

	rows, err := db.doQueryRows(ctx, tx, query, 1, key)
	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

func (db *duckdbDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var out map[string][]byte
	err := db.tx(ctx, func(tx *sql.Tx) error {
		res, err := db.doRead(ctx, tx, table, key, fields)
		out = res
		return err
	})
	return out, err
}

func (db *duckdbDB) doScan(ctx context.Context, tx *sql.Tx, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	sel := "*"
	if len(fields) > 0 {
		sel = strings.Join(fields, ",")
	}
	// ORDER BY is not decoration here. YCSB's scan is defined as the next
	// count records in key order, and a columnar engine has no obligation
	// to return them in that order without being asked. The count is
	// formatted in rather than bound because DuckDB does not take a
	// parameter in LIMIT, and it is an int from the workload generator.
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT %d`, sel, table, count)

	return db.doQueryRows(ctx, tx, query, count, startKey)
}

func (db *duckdbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var out []map[string][]byte
	err := db.tx(ctx, func(tx *sql.Tx) error {
		res, err := db.doScan(ctx, tx, table, startKey, count, fields)
		out = res
		return err
	})
	return out, err
}

func (db *duckdbDB) doUpdate(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() { db.bufPool.Put(buf.Bytes()) }()

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")

	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for i, p := range pairs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(p.Field)
		buf.WriteString(" = ?")
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")
	args = append(args, key)

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (db *duckdbDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		return db.doUpdate(ctx, tx, table, key, values)
	})
}

func (db *duckdbDB) doInsert(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() { db.bufPool.Put(buf.Bytes()) }()

	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	// A plain INSERT, and not the OR IGNORE the sqlite adapter uses, and
	// this is a workaround rather than a preference.
	//
	// DuckDB 1.4.1 matches the column list of an INSERT that carries a
	// conflict clause case sensitively, and matches it case insensitively
	// without one. The table is declared with FIELD0 upwards and the
	// workload hands its fields over as field0 upwards, so
	// INSERT OR IGNORE INTO usertable (YCSB_KEY, field0, ...) resolved
	// the key and resolved none of the fields, and the columns that were
	// not resolved took their default, which is NULL. Nothing was
	// reported: the statement prepared, the row landed, and the row was a
	// key with ten NULL columns beside it. It is the clause and not the
	// wording, so OR IGNORE, OR REPLACE and ON CONFLICT DO NOTHING all do
	// it, and it does it with literal values as readily as with
	// parameters, which is what says it is name resolution and not
	// binding. A table whose key column is NOT NULL and whose key is also
	// named in the wrong case gets a constraint error instead, which is
	// how the shape of it was found.
	//
	// That is a silent wrong answer and it invalidated every duckdb
	// number this harness has produced: a load of 100000 records of a
	// kilobyte each settled at 8.5 MiB on device, which is 89 bytes a
	// record and is the key and its index and nothing else, and every
	// read afterwards was a fast read of a row with no data in it.
	//
	// DuckDB 1.5.5 answers this correctly. go-duckdb v2.4.3 is the newest
	// there is and it carries 1.4.1, so the workaround stays until the
	// bindings carry a 1.5.
	//
	// YCSB generates each key once in a load, so a conflict clause was
	// insurance rather than a requirement. The insurance is taken here
	// instead, by treating a constraint violation as the no-op OR IGNORE
	// would have made it, which keeps the semantics the sqlite adapter
	// has without the clause that resolves its columns differently.
	buf.WriteString("INSERT INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(", ")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")
	for range pairs {
		buf.WriteString(", ?")
	}
	buf.WriteByte(')')

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	if err != nil && isDuplicate(err) {
		return nil
	}
	if err != nil && db.verbose {
		fmt.Printf("error(doInsert): %s: %+v\n", buf.String(), err)
	}
	return err
}

// isDuplicate says whether an error is the primary key already being
// there, which is the one error a load may meet and carry on from.
//
// By the text, because the driver hands back an error whose type says
// nothing about which constraint was broken. There is one constraint on
// the table and it is the primary key, so the match is as specific as it
// needs to be.
func isDuplicate(err error) bool {
	return strings.Contains(err.Error(), "Constraint Error") &&
		strings.Contains(err.Error(), "Duplicate key")
}

func (db *duckdbDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		return db.doInsert(ctx, tx, table, key, values)
	})
}

func (db *duckdbDB) doDelete(ctx context.Context, tx *sql.Tx, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = ?`, table)
	_, err := tx.ExecContext(ctx, query, key)
	return err
}

func (db *duckdbDB) Delete(ctx context.Context, table string, key string) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		return db.doDelete(ctx, tx, table, key)
	})
}

func (db *duckdbDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		for i := range keys {
			if err := db.doInsert(ctx, tx, table, keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *duckdbDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	var out []map[string][]byte
	err := db.tx(ctx, func(tx *sql.Tx) error {
		out = out[:0]
		for i := range keys {
			res, err := db.doRead(ctx, tx, table, keys[i], fields)
			if err != nil {
				return err
			}
			out = append(out, res)
		}
		return nil
	})
	return out, err
}

func (db *duckdbDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		for i := range keys {
			if err := db.doUpdate(ctx, tx, table, keys[i], values[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *duckdbDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	return db.tx(ctx, func(tx *sql.Tx) error {
		for i := range keys {
			if err := db.doDelete(ctx, tx, table, keys[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func init() {
	ycsb.RegisterDBCreator("duckdb", duckdbCreator{})
}

var _ ycsb.BatchDB = (*duckdbDB)(nil)
