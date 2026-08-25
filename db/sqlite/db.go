// Copyright 2019 PingCAP, Inc.
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

//go:build sqlite

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"
	// sqlite package
	"github.com/mattn/go-sqlite3"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Sqlite properties
const (
	sqliteDBPath              = "sqlite.db"
	sqliteMode                = "sqlite.mode"
	sqliteJournalMode         = "sqlite.journalmode"
	sqliteSynchronous         = "sqlite.synchronous"
	sqliteCache               = "sqlite.cache"
	sqliteMaxOpenConns        = "sqlite.maxopenconns"
	sqliteMaxIdleConns        = "sqlite.maxidleconns"
	sqliteBusyTimeout         = "sqlite.busy_timeout"
	sqliteOptimistic          = "sqlite.optimistic"
	sqliteOptimisticBackoffMs = "sqlite.optimistic_backoff_ms"
	sqliteCacheSize           = "sqlite.cache_size"
	sqliteMmapSize            = "sqlite.mmap_size"
	sqliteStmtCacheSize       = "sqlite.stmt_cache_size"
	sqliteReadTx              = "sqlite.readtx"
	sqliteTempStore           = "sqlite.temp_store"
)

// The driver name the adapter opens under. It is not "sqlite3", because
// two of the settings sqlite needs to run at its best are per connection
// pragmas with no DSN spelling in the driver, and a connect hook is the
// only place a pool can apply them.
const sqliteDriverName = "sqlite3_ycsb"

// The pragmas the connect hook runs, set once by Create before the pool
// opens its first connection. One process opens one database here, which
// is why a package level value is enough.
var sqliteConnPragmas []string

func init() {
	sql.Register(sqliteDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			for _, p := range sqliteConnPragmas {
				if _, err := c.Exec(p, nil); err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
			}
			return nil
		},
	})
}

type sqliteCreator struct {
}

type sqliteDB struct {
	p          *properties.Properties
	db         *sql.DB
	verbose    bool
	optimistic bool
	readTx     bool
	backoffMs  int

	bufPool *util.BufPool
}

func (c sqliteCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(sqliteDB)
	d.p = p

	dbPath := p.GetString(sqliteDBPath, "/tmp/sqlite.db")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dbPath)
	}

	mode := p.GetString(sqliteMode, "rwc")
	journalMode := p.GetString(sqliteJournalMode, "WAL")
	// Every engine here runs at its fastest configuration so the number is a
	// throughput ceiling, and the level is still an explicit property so it
	// lands in the result rather than being inherited silently. OFF is not
	// crash durable; the point of recording it is that the same choice is
	// made for every engine, and a durable row can be taken by overriding.
	synchronous := p.GetString(sqliteSynchronous, "OFF")
	// Two defaults changed from upstream, and they are the difference
	// between measuring SQLite and measuring a pool of one.
	//
	// Upstream runs cache=shared with maxopenconns=1. In WAL mode that
	// is the slow way round. Shared cache serialises readers on a table
	// lock, so opening the pool without also leaving shared cache makes
	// things worse rather than better. Measured at 10000 records on
	// workload C, ops/s at 1, 4 and 16 threads:
	//
	//	shared, 1 conn    47688   53305    41913
	//	shared, n conns   48200   27473    27940
	//	private, n conns  48092  108671   119927
	//
	// So the old defaults cost SQLite a factor of nearly three at 16
	// threads, and this benchmark exists to compare engines at their
	// fastest rather than to publish a number that a configuration
	// choice held down.
	//
	// A pool wider than one needs a busy timeout, because WAL gives
	// concurrent readers but still one writer, and the second writer
	// gets SQLITE_BUSY immediately without one. Five seconds is long
	// enough that a benchmark never sees it and short enough that a real
	// deadlock still fails.
	cache := p.GetString(sqliteCache, "private")
	threads := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	maxOpenConns := p.GetInt(sqliteMaxOpenConns, threads)
	maxIdleConns := p.GetInt(sqliteMaxIdleConns, threads)

	// Three more settings that sqlite is much slower without, and that
	// the adapter did not set at all until the latency work went looking
	// for why a read through here cost eight times what the same read
	// costs in a C loop over the same database.
	//
	// The page cache is 2 MiB by default, which for any database worth
	// benchmarking means a pread for every read. It is per connection,
	// so 64 MiB at n threads is 64n and the number below is deliberately
	// not a gigabyte: a run that swaps is a benchmark of the swap.
	//
	// The mapping is the cheaper half of the same thing. mmap pages are
	// file backed and shared between connections, so a large mmap costs
	// address space rather than memory, and it takes the copy out of the
	// read path for anything the page cache misses.
	//
	// The statement cache is the driver's, off by default, which means
	// every point read prepares and finalises a statement around one
	// step. Thirty two is more shapes than this workload has.
	cacheSize := p.GetString(sqliteCacheSize, "-65536")
	mmapSize := p.GetString(sqliteMmapSize, "8589934592")
	tempStore := p.GetString(sqliteTempStore, "MEMORY")
	sqliteConnPragmas = []string{
		fmt.Sprintf("PRAGMA mmap_size = %s;", mmapSize),
		fmt.Sprintf("PRAGMA temp_store = %s;", tempStore),
	}

	v := url.Values{}
	v.Set("cache", cache)
	v.Set("mode", mode)
	v.Set("_journal_mode", journalMode)
	v.Set("_synchronous", synchronous)
	v.Set("_busy_timeout", p.GetString(sqliteBusyTimeout, "5000"))
	v.Set("_cache_size", cacheSize)
	v.Set("_stmt_cache_size", p.GetString(sqliteStmtCacheSize, "32"))
	dsn := fmt.Sprintf("file:%s?%s", dbPath, v.Encode())
	var err error
	db, err := sql.Open(sqliteDriverName, dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	d.optimistic = p.GetBool(sqliteOptimistic, false)
	d.readTx = p.GetBool(sqliteReadTx, false)
	d.backoffMs = p.GetInt(sqliteOptimisticBackoffMs, 5)
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	// The engine version, in the output, once, for the same reason the
	// duckdb adapter prints its own: the library is linked in and there
	// is nothing on the host to ask, so without this line the TSV cannot
	// say which sqlite produced a row. The build tags decide whether
	// that is the bundled amalgamation or the system library, and those
	// are two different engines with two different numbers.
	var version string
	if err := d.db.QueryRow("select sqlite_version()").Scan(&version); err == nil {
		fmt.Printf("sqlite version: %s\n", version)
	}
	// The settings that turned out to be worth a factor of two on a
	// point read, printed for the same reason the version is: a row in a
	// TSV that does not say how the engine was configured cannot be
	// compared with anything.
	fmt.Printf("sqlite config: journal=%s synchronous=%s cache=%s cache_size=%s mmap_size=%s stmt_cache_size=%s readtx=%v conns=%d\n",
		journalMode, synchronous, cache, cacheSize, mmapSize,
		p.GetString(sqliteStmtCacheSize, "32"), d.readTx, maxOpenConns)

	return d, nil
}

func (db *sqliteDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (YCSB_KEY VARCHAR(64) PRIMARY KEY", tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR(%d)", i, fieldLength))
	}

	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.Exec(buf.String())
	return err
}

func (db *sqliteDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *sqliteDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *sqliteDB) CleanupThread(ctx context.Context) {

}

func (db *sqliteDB) optimisticTx(ctx context.Context, f func(tx *sql.Tx) error) error {
	for {
		tx, err := db.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if err = f(tx); err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err != nil && db.optimistic {
			if err, ok := err.(sqlite3.Error); ok && (err.Code == sqlite3.ErrBusy ||
				err.ExtendedCode == sqlite3.ErrIoErrUnlock) {
				time.Sleep(time.Duration(db.backoffMs) * time.Millisecond)
				continue
			}
		}
		return err
	}
}

// What doQueryRows needs of the thing it runs on, which is the same for
// a transaction and for the pool itself. A read that is one statement is
// already atomic in sqlite, so it does not have to be given a BEGIN and
// a COMMIT of its own, and those are two more statements across the cgo
// boundary for a call that steps a cursor once.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (db *sqliteDB) doQueryRows(ctx context.Context, tx querier, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
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
		for i := 0; i < len(cols); i++ {
			v := new([]byte)
			dest[i] = v
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

func (db *sqliteDB) doRead(ctx context.Context, tx querier, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = ?`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.doQueryRows(ctx, tx, query, 1, key)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *sqliteDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	if !db.readTx {
		return db.doRead(ctx, db.db, table, key, fields)
	}
	var output map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		res, err := db.doRead(ctx, tx, table, key, fields)
		output = res
		return err
	})
	return output, err
}

func (db *sqliteDB) doScan(ctx context.Context, tx querier, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// ORDER BY for the reason the duckdb driver gives. SQLite happens to
	// walk the primary key index here and so was already returning rows
	// in order, which is why the scan integrity check passes either way,
	// but that is the plan it picked and not a promise it made. Asking
	// for the order it is already producing costs nothing and stops the
	// answer depending on a plan choice.
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT ?`, table)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.doQueryRows(ctx, tx, query, count, startKey, count)

	return rows, err
}

// ScanEach is Scan with the maps taken out, the ycsb.EachScanner path.
//
// The map path here is the most wasteful of any driver in the tree: a
// map, a dest slice and a fresh []byte a column, all allocated a row, and
// a scan is fifty rows. This keeps one of each and refills them, and
// hands the callback the column values positionally in the order the
// caller asked for them, which is the order the SELECT names them in.
//
// The columns are named rather than starred even when the caller asked
// for every field. SELECT * also returns YCSB_KEY, which is not a field
// and has no slot, so naming them keeps the row and the fields slice the
// same shape and stops sqlite reading a column nobody wanted.
//
// The values are the driver's own and are good until the callback
// returns. See tamnd/zu#750.
func (db *sqliteDB) ScanEach(ctx context.Context, table string, startKey string, count int, fields []string, fn func(values [][]byte) error) error {
	if count <= 0 {
		return nil
	}
	if !db.readTx {
		return db.doScanEach(ctx, db.db, table, startKey, count, fields, fn)
	}
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		return db.doScanEach(ctx, tx, table, startKey, count, fields, fn)
	})
}

func (db *sqliteDB) doScanEach(ctx context.Context, tx querier, table string, startKey string, count int, fields []string, fn func(values [][]byte) error) error {
	eff := fields
	if len(eff) == 0 {
		eff = db.r.Fields()
	}
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= ? ORDER BY YCSB_KEY LIMIT ?`,
		strings.Join(eff, ","), table)
	if db.verbose {
		fmt.Printf("%s %v\n", query, []interface{}{startKey, count})
	}

	rows, err := tx.QueryContext(ctx, query, startKey, count)
	if err != nil {
		return err
	}
	defer rows.Close()

	values := make([][]byte, len(eff))
	dest := make([]interface{}, len(eff))
	for i := range dest {
		dest[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		if err := fn(values); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (db *sqliteDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	if !db.readTx {
		return db.doScan(ctx, db.db, table, startKey, count, fields)
	}
	var output []map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		res, err := db.doScan(ctx, tx, table, startKey, count, fields)
		output = res
		return err
	})
	return output, err
}

func (db *sqliteDB) doUpdate(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if firstField {
			firstField = false
		} else {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	args = append(args, key)

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	return err
}

func (db *sqliteDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		return db.doUpdate(ctx, tx, table, key, values)
	})
}

func (db *sqliteDB) doInsert(ctx context.Context, tx *sql.Tx, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := bytes.NewBuffer(db.bufPool.Get())
	defer func() {
		db.bufPool.Put(buf.Bytes())
	}()

	buf.WriteString("INSERT OR IGNORE INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(" ,?")
	}

	buf.WriteByte(')')

	_, err := tx.ExecContext(ctx, buf.String(), args...)
	if err != nil && db.verbose {
		fmt.Printf("error(doInsert): %s: %+v\n", buf.String(), err)
	}
	return err
}

func (db *sqliteDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error { return db.doInsert(ctx, tx, table, key, values) })
}

func (db *sqliteDB) doDelete(ctx context.Context, tx *sql.Tx, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = ?`, table)
	_, err := tx.ExecContext(ctx, query, key)
	return err
}

func (db *sqliteDB) Delete(ctx context.Context, table string, key string) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error { return db.doDelete(ctx, tx, table, key) })
}

func (db *sqliteDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doInsert(ctx, tx, table, keys[i], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *sqliteDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	var output []map[string][]byte
	err := db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			res, err := db.doRead(ctx, tx, table, keys[i], fields)
			if err != nil {
				return err
			}
			output = append(output, res)
		}
		return nil
	})
	return output, err
}

func (db *sqliteDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doUpdate(ctx, tx, table, keys[i], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *sqliteDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	return db.optimisticTx(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(keys); i++ {
			err := db.doDelete(ctx, tx, table, keys[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func init() {
	ycsb.RegisterDBCreator("sqlite", sqliteCreator{})
}

var _ ycsb.BatchDB = (*sqliteDB)(nil)
