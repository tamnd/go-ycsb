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

package client

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// DbWrapper stores the pointer to a implementation of ycsb.DB.
type DbWrapper struct {
	DB ycsb.DB
}

// The first error each operation type produces, printed once.
//
// Before this the error was counted and then dropped on the floor: a
// failing operation became a line called SCAN_ERROR in the summary with
// a latency on it and nothing else, no message, no cause. An engine
// whose scans all failed therefore looked like an engine with very fast
// scans, and the only reason it was ever noticed is that #551 added the
// rows against scans line at the end of a run. That is the same class of
// hole #551 closed on the other side of this call, and this closes it
// here.
//
// Once per operation type rather than every time, because an engine that
// is failing at all is usually failing on every operation and a million
// copies of one message is not more informative than one.
var errOnce sync.Map

func measure(ctx context.Context, start time.Time, op string, err error) {
	lan := time.Now().Sub(start)
	if err != nil {
		if _, loaded := errOnce.LoadOrStore(op, struct{}{}); !loaded {
			fmt.Fprintf(os.Stderr, "%s failed, first error of this kind: %v\n", op, err)
		}
		measurement.Measure(ctx, fmt.Sprintf("%s_ERROR", op), start, lan)
		return
	}

	measurement.Measure(ctx, op, start, lan)
	measurement.Measure(ctx, "TOTAL", start, lan)
}

func (db DbWrapper) Close() error {
	return db.DB.Close()
}

func (db DbWrapper) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return db.DB.InitThread(ctx, threadID, threadCount)
}

func (db DbWrapper) CleanupThread(ctx context.Context) {
	db.DB.CleanupThread(ctx)
}

func (db DbWrapper) Read(ctx context.Context, table string, key string, fields []string) (_ map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		measure(ctx, start, "READ", err)
	}()

	return db.DB.Read(ctx, table, key, fields)
}

func (db DbWrapper) BatchRead(ctx context.Context, table string, keys []string, fields []string) (_ []map[string][]byte, err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(ctx, start, "BATCH_READ", err)
		}()
		return batchDB.BatchRead(ctx, table, keys, fields)
	}
	for _, key := range keys {
		_, err := db.DB.Read(ctx, table, key, fields)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (db DbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) (_ []map[string][]byte, err error) {
	start := time.Now()
	defer func() {
		measure(ctx, start, "SCAN", err)
	}()

	return db.DB.Scan(ctx, table, startKey, count, fields)
}

// ScanEach is the ycsb.EachScanner path, timed as a SCAN like every
// other operation here.
//
// It has to be on the wrapper and not only on the driver. The workload
// holds a DbWrapper and nothing else, so a driver method this does not
// forward is a driver method the workload cannot reach: ScanEach shipped
// implemented on three drivers and was never once called, and the A/B
// that was supposed to show what it was worth showed nothing because
// both sides of it ran the same code.
//
// The wrapper satisfies the interface whether the driver does or not,
// which is why the workload asks Unwrap first rather than asking this.
// Reaching the fallback means the workload did not, and a fallback that
// quietly rebuilt the rows out of maps would put work nobody asked for
// inside the call being timed, which is the one thing this path exists
// to take out.
func (db DbWrapper) ScanEach(ctx context.Context, table string, startKey string, count int, fields []string, fn func(values [][]byte) error) (err error) {
	es, ok := db.DB.(ycsb.EachScanner)
	if !ok {
		return fmt.Errorf("%T does not implement ycsb.EachScanner", db.DB)
	}

	start := time.Now()
	defer func() {
		measure(ctx, start, "SCAN", err)
	}()

	return es.ScanEach(ctx, table, startKey, count, fields, fn)
}

// Unwrap is the driver underneath, for a caller asking whether it
// implements an optional interface. Asking the wrapper answers yes to
// everything the wrapper forwards, which is not the same question.
func (db DbWrapper) Unwrap() ycsb.DB {
	return db.DB
}

func (db DbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		measure(ctx, start, "UPDATE", err)
	}()

	return db.DB.Update(ctx, table, key, values)
}

func (db DbWrapper) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(ctx, start, "BATCH_UPDATE", err)
		}()
		return batchDB.BatchUpdate(ctx, table, keys, values)
	}
	for i := range keys {
		err := db.DB.Update(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) (err error) {
	start := time.Now()
	defer func() {
		measure(ctx, start, "INSERT", err)
	}()

	return db.DB.Insert(ctx, table, key, values)
}

func (db DbWrapper) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(ctx, start, "BATCH_INSERT", err)
		}()
		return batchDB.BatchInsert(ctx, table, keys, values)
	}
	for i := range keys {
		err := db.DB.Insert(ctx, table, keys[i], values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Delete(ctx context.Context, table string, key string) (err error) {
	start := time.Now()
	defer func() {
		measure(ctx, start, "DELETE", err)
	}()

	return db.DB.Delete(ctx, table, key)
}

func (db DbWrapper) BatchDelete(ctx context.Context, table string, keys []string) (err error) {
	batchDB, ok := db.DB.(ycsb.BatchDB)
	if ok {
		start := time.Now()
		defer func() {
			measure(ctx, start, "BATCH_DELETE", err)
		}()
		return batchDB.BatchDelete(ctx, table, keys)
	}
	for _, key := range keys {
		err := db.DB.Delete(ctx, table, key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db DbWrapper) Analyze(ctx context.Context, table string) error {
	if analyzeDB, ok := db.DB.(ycsb.AnalyzeDB); ok {
		return analyzeDB.Analyze(ctx, table)
	}
	return nil
}
