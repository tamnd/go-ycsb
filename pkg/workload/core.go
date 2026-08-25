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

package workload

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type contextKey string

const stateKey = contextKey("core")

type coreState struct {
	r *rand.Rand
	// fieldNames is a copy of core.fieldNames to be goroutine-local
	fieldNames []string
}

type operationType int64

const (
	read operationType = iota + 1
	update
	insert
	scan
	readModifyWrite
)

// Core is the core benchmark scenario. Represents a set of clients doing simple CRUD operations.
type core struct {
	p *properties.Properties

	table      string
	fieldCount int64
	fieldNames []string

	fieldLengthGenerator ycsb.Generator
	readAllFields        bool
	writeAllFields       bool
	dataIntegrity        bool

	keySequence                  ycsb.Generator
	operationChooser             *generator.Discrete
	keyChooser                   ycsb.Generator
	fieldChooser                 ycsb.Generator
	transactionInsertKeySequence *generator.AcknowledgedCounter
	scanLength                   ycsb.Generator
	orderedInserts               bool
	recordCount                  int64
	zeroPadding                  int64
	// The key prefix, read once at construction. It used to be read
	// out of the properties map on every key built, which is a string
	// map lookup per operation for a value that cannot change, and it
	// was five percent of the client's CPU. See tamnd/zu#645.
	keyPrefix              string
	insertionRetryLimit    int64
	insertionRetryInterval int64

	valuePool sync.Pool

	// What the scans actually handed back, which nothing used to look
	// at. `doTransactionScan` throws the rows away, so an engine that
	// returns nothing at all reports the fastest SCAN in the sweep and
	// no error anywhere. That is how the duckdb rows in every sweep so
	// far came to be wrong (tamnd/zu#551), and the same hole is open on
	// this side of the call for every engine. These two are what close
	// it: the run prints rows against scans at the end, and a mean well
	// under the mean scan length is a result to throw away.
	scans     atomic.Int64
	scanRows  atomic.Int64
	scanAsked atomic.Int64
}

func getFieldLengthGenerator(p *properties.Properties) ycsb.Generator {
	var fieldLengthGenerator ycsb.Generator
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	fieldLengthHistogram := p.GetString(prop.FieldLengthHistogramFile, prop.FieldLengthHistogramFileDefault)

	switch strings.ToLower(fieldLengthDistribution) {
	case "constant":
		fieldLengthGenerator = generator.NewConstant(fieldLength)
	case "uniform":
		fieldLengthGenerator = generator.NewUniform(1, fieldLength)
	case "zipfian":
		fieldLengthGenerator = generator.NewZipfianWithRange(1, fieldLength, generator.ZipfianConstant)
	case "histogram":
		fieldLengthGenerator = generator.NewHistogramFromFile(fieldLengthHistogram)
	default:
		util.Fatalf("unknown field length distribution %s", fieldLengthDistribution)
	}

	return fieldLengthGenerator
}

func createOperationGenerator(p *properties.Properties) *generator.Discrete {
	readProportion := p.GetFloat64(prop.ReadProportion, prop.ReadProportionDefault)
	updateProportion := p.GetFloat64(prop.UpdateProportion, prop.UpdateProportionDefault)
	insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
	scanProportion := p.GetFloat64(prop.ScanProportion, prop.ScanProportionDefault)
	readModifyWriteProportion := p.GetFloat64(prop.ReadModifyWriteProportion, prop.ReadModifyWriteProportionDefault)

	operationChooser := generator.NewDiscrete()
	if readProportion > 0 {
		operationChooser.Add(readProportion, int64(read))
	}

	if updateProportion > 0 {
		operationChooser.Add(updateProportion, int64(update))
	}

	if insertProportion > 0 {
		operationChooser.Add(insertProportion, int64(insert))
	}

	if scanProportion > 0 {
		operationChooser.Add(scanProportion, int64(scan))
	}

	if readModifyWriteProportion > 0 {
		operationChooser.Add(readModifyWriteProportion, int64(readModifyWrite))
	}

	return operationChooser
}

// Load implements the Workload Load interface.
func (c *core) Load(ctx context.Context, db ycsb.DB, totalCount int64) error {
	return nil
}

// InitThread implements the Workload InitThread interface.
func (c *core) InitThread(ctx context.Context, _ int, _ int) context.Context {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fieldNames := make([]string, len(c.fieldNames))
	copy(fieldNames, c.fieldNames)
	state := &coreState{
		r:          r,
		fieldNames: fieldNames,
	}
	return context.WithValue(ctx, stateKey, state)
}

// CleanupThread implements the Workload CleanupThread interface.
func (c *core) CleanupThread(_ context.Context) {

}

// Close implements the Workload Close interface.
func (c *core) Close() error {
	if scans := c.scans.Load(); scans > 0 {
		rows := c.scanRows.Load()
		asked := c.scanAsked.Load()
		fmt.Printf("scan rows: %d rows over %d scans, %.1f a scan, %.1f asked for\n",
			rows, scans, float64(rows)/float64(scans), float64(asked)/float64(scans))
	}
	return nil
}

// buildKeyName is the key for a key number: the prefix, then the number
// left padded with zeros to `zeroPadding` digits.
//
// This used to be one `fmt.Sprintf` with an indexed dynamic width, which
// is the slowest spelling `fmt` has, plus a properties lookup for the
// prefix. Between them they were a quarter of what the client spends on
// an operation before it reaches a driver, and the client's floor is
// most of what every published throughput number measures (tamnd/zu#645).
// Every byte this returns is the same byte the Sprintf returned, which
// `TestBuildKeyNameMatchesSprintf` holds.
func (c *core) buildKeyName(keyNum int64) string {
	if !c.orderedInserts {
		keyNum = util.Hash64(keyNum)
	}

	// Big enough for the longest int64 with its sign and for any
	// padding a sane run asks for, so the common case never allocates
	// twice. A `zeroPadding` wider than this still comes out right, the
	// append just grows the slice.
	var stack [64]byte
	buf := append(stack[:0], c.keyPrefix...)

	// `fmt` puts the sign in front of the padding, so "%05d" of -42 is
	// "-0042" and not "000-42", and the width is the whole field: the
	// sign is one of the characters it counts, so "%02d" of -1 is "-1"
	// and not "-01".
	n := keyNum
	width := int(c.zeroPadding)
	if n < 0 {
		buf = append(buf, '-')
		width--
	}
	digits := 1
	for m := n / 10; m != 0; m /= 10 {
		digits++
	}
	for pad := width - digits; pad > 0; pad-- {
		buf = append(buf, '0')
	}
	// Written from the back, because the digits come out least
	// significant first. `-n` is not taken for the negative case:
	// negating math.MinInt64 overflows, so the remainder is negated one
	// digit at a time instead.
	at := len(buf) + digits
	if cap(buf) < at {
		buf = append(buf, make([]byte, at-len(buf))...)
	}
	buf = buf[:at]
	for i := at - 1; i >= at-digits; i-- {
		d := n % 10
		if d < 0 {
			d = -d
		}
		buf[i] = byte('0' + d)
		n /= 10
	}
	return string(buf)
}

func (c *core) buildSingleValue(state *coreState, key string) map[string][]byte {
	values := make(map[string][]byte, 1)

	r := state.r
	fieldKey := state.fieldNames[c.fieldChooser.Next(r)]

	var buf []byte
	if c.dataIntegrity {
		buf = c.buildDeterministicValue(state, key, fieldKey)
	} else {
		buf = c.buildRandomValue(state)
	}

	values[fieldKey] = buf

	return values
}

func (c *core) buildValues(state *coreState, key string) map[string][]byte {
	values := make(map[string][]byte, c.fieldCount)

	for _, fieldKey := range state.fieldNames {
		var buf []byte
		if c.dataIntegrity {
			buf = c.buildDeterministicValue(state, key, fieldKey)
		} else {
			buf = c.buildRandomValue(state)
		}

		values[fieldKey] = buf
	}
	return values
}

func (c *core) getValueBuffer(size int) []byte {
	buf := c.valuePool.Get().([]byte)
	if cap(buf) >= size {
		return buf[0:size]
	}

	return make([]byte, size)
}

func (c *core) putValues(values map[string][]byte) {
	for _, value := range values {
		c.valuePool.Put(value)
	}
}

func (c *core) buildRandomValue(state *coreState) []byte {
	// TODO: use pool for the buffer
	r := state.r
	buf := c.getValueBuffer(int(c.fieldLengthGenerator.Next(r)))
	util.RandBytes(r, buf)
	return buf
}

func (c *core) buildDeterministicValue(state *coreState, key string, fieldKey string) []byte {
	// TODO: use pool for the buffer
	r := state.r
	size := c.fieldLengthGenerator.Next(r)
	buf := c.getValueBuffer(int(size + 21))
	b := bytes.NewBuffer(buf[0:0])
	b.WriteString(key)
	b.WriteByte(':')
	b.WriteString(strings.ToLower(fieldKey))
	for int64(b.Len()) < size {
		b.WriteByte(':')
		n := util.BytesHash64(b.Bytes())
		b.WriteString(strconv.FormatUint(uint64(n), 10))
	}
	b.Truncate(int(size))
	return b.Bytes()
}

// verifyScan is verifyRow for a range scan.
//
// Scan hands back rows and not the keys they came from, so there is
// nothing to derive an expected value from the way a read has. The rows
// carry it themselves: a deterministic value opens with the key of the
// row it belongs to followed by a colon, so every value names its own
// key and can be rebuilt and compared once that key is read back out of
// it.
//
// This exists because dataintegrity checked reads and left scans alone,
// so nothing in the harness ever looked at a value workload E returned.
// That is the hole tamnd/zu#551 went through on the read path, still
// open on the scan path, and it is the one place a driver that reuses
// buffers or maps between rows would show up.
func (c *core) verifyScan(state *coreState, startKey string, rows []map[string][]byte) {
	last := ""
	for i, values := range rows {
		if len(values) == 0 {
			util.Fatalf("scan from %s: row %d came back with no fields at all", startKey, i)
		}

		key := ""
		for fieldKey, value := range values {
			at := bytes.IndexByte(value, ':')
			if at < 0 {
				util.Fatalf("scan from %s: row %d field %s carries no key, got %q",
					startKey, i, fieldKey, value)
			}
			rowKey := string(value[:at])
			// Every field of one row has to name the same key. A driver
			// that hands back one map per row and fills them from a
			// buffer it reuses would leak a field of the row before into
			// this one, and this is what that looks like.
			if key == "" {
				key = rowKey
			} else if rowKey != key {
				util.Fatalf("scan from %s: row %d mixes rows, field %s belongs to %s and the rest to %s",
					startKey, i, fieldKey, rowKey, key)
			}
			expected := c.buildDeterministicValue(state, rowKey, fieldKey)
			if !bytes.Equal(expected, value) {
				util.Fatalf("scan from %s: row %d field %s, expect %q, but got %q",
					startKey, i, fieldKey, expected, value)
			}
		}

		// A scan is a range read, so what comes back starts at the key
		// asked for and climbs. The interface comment does not spell
		// that out, but every driver here implements it and workload E
		// is measuring a range scan, so rows outside the range or in no
		// order are a wrong answer being timed as a fast one.
		if key < startKey {
			util.Fatalf("scan from %s: row %d is key %s, which is before the start",
				startKey, i, key)
		}
		if last != "" && key <= last {
			util.Fatalf("scan from %s: keys do not climb, %s then %s", startKey, last, key)
		}
		last = key
	}
}

// verifyScanRow is verifyScan for one row of a scan taken through
// EachScanner, where the rows arrive one at a time and there is no slice
// to walk at the end.
//
// It checks the same three things: that the row has fields at all, that
// every field of it names the same key and carries the value that key and
// field are supposed to have, and that the keys climb from the start key.
// `last` carries the previous row's key across calls, which is the only
// state the slice version kept.
func (c *core) verifyScanRow(state *coreState, startKey string, i int, fields []string, values [][]byte, last *string) {
	found := 0
	key := ""
	for at, value := range values {
		if value == nil {
			continue
		}
		found++
		fieldKey := fields[at]
		sep := bytes.IndexByte(value, ':')
		if sep < 0 {
			util.Fatalf("scan from %s: row %d field %s carries no key, got %q",
				startKey, i, fieldKey, value)
		}
		rowKey := string(value[:sep])
		// Every field of one row has to name the same key. A driver
		// filling one slice from a buffer it reuses would leak a field
		// of the row before into this one, and this is what that looks
		// like.
		if key == "" {
			key = rowKey
		} else if rowKey != key {
			util.Fatalf("scan from %s: row %d mixes rows, field %s belongs to %s and the rest to %s",
				startKey, i, fieldKey, rowKey, key)
		}
		expected := c.buildDeterministicValue(state, rowKey, fieldKey)
		if !bytes.Equal(expected, value) {
			util.Fatalf("scan from %s: row %d field %s, expect %q, but got %q",
				startKey, i, fieldKey, expected, value)
		}
	}

	if found == 0 {
		util.Fatalf("scan from %s: row %d came back with no fields at all", startKey, i)
	}

	if key < startKey {
		util.Fatalf("scan from %s: row %d is key %s, which is before the start",
			startKey, i, key)
	}
	if *last != "" && key <= *last {
		util.Fatalf("scan from %s: keys do not climb, %s then %s", startKey, *last, key)
	}
	*last = key
}

func (c *core) verifyRow(state *coreState, key string, values map[string][]byte) {
	// An empty row is the failure this check exists to catch, not a
	// case to skip. An engine that stored the key and dropped the
	// values answers every read without an error and passes a row count
	// (tamnd/zu#551), and this used to wave it through as well.
	if len(values) == 0 {
		util.Fatalf("%s came back with no fields at all", key)
	}

	for fieldKey, value := range values {
		expected := c.buildDeterministicValue(state, key, fieldKey)
		if !bytes.Equal(expected, value) {
			util.Fatalf("unexpected deterministic value, expect %q, but got %q", expected, value)
		}
	}
}

// DoInsert implements the Workload DoInsert interface.
func (c *core) DoInsert(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(stateKey).(*coreState)
	r := state.r
	keyNum := c.keySequence.Next(r)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	numOfRetries := int64(0)

	var err error
	for {
		err = db.Insert(ctx, c.table, dbKey, values)
		if err != nil {
			break
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}

	return err
}

// DoBatchInsert implements the Workload DoBatchInsert interface.
func (c *core) DoBatchInsert(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	r := state.r
	var keys []string
	var values []map[string][]byte
	for i := 0; i < batchSize; i++ {
		keyNum := c.keySequence.Next(r)
		dbKey := c.buildKeyName(keyNum)
		keys = append(keys, dbKey)
		values = append(values, c.buildValues(state, dbKey))
	}
	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	numOfRetries := int64(0)
	var err error
	for {
		err = batchDB.BatchInsert(ctx, c.table, keys, values)
		if err != nil {
			break
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return nil
			}
		default:
		}

		// Retry if configured. Without retrying, the load process will fail
		// even if one single insertion fails. User can optionally configure
		// an insertion retry limit (default is 0) to enable retry.
		numOfRetries++
		if numOfRetries > c.insertionRetryLimit {
			break
		}

		// Sleep for a random time betweensz [0.8, 1.2)*insertionRetryInterval
		sleepTimeMs := float64((c.insertionRetryInterval * 1000)) * (0.8 + 0.4*r.Float64())

		time.Sleep(time.Duration(sleepTimeMs) * time.Millisecond)
	}
	return err
}

// DoTransaction implements the Workload DoTransaction interface.
func (c *core) DoTransaction(ctx context.Context, db ycsb.DB) error {
	state := ctx.Value(stateKey).(*coreState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doTransactionRead(ctx, db, state)
	case update:
		return c.doTransactionUpdate(ctx, db, state)
	case insert:
		return c.doTransactionInsert(ctx, db, state)
	case scan:
		return c.doTransactionScan(ctx, db, state)
	default:
		return c.doTransactionReadModifyWrite(ctx, db, state)
	}
}

// DoBatchTransaction implements the Workload DoBatchTransaction interface
func (c *core) DoBatchTransaction(ctx context.Context, batchSize int, db ycsb.DB) error {
	batchDB, ok := db.(ycsb.BatchDB)
	if !ok {
		return fmt.Errorf("the %T does't implement the batchDB interface", db)
	}
	state := ctx.Value(stateKey).(*coreState)
	r := state.r

	operation := operationType(c.operationChooser.Next(r))
	switch operation {
	case read:
		return c.doBatchTransactionRead(ctx, batchSize, batchDB, state)
	case insert:
		return c.doBatchTransactionInsert(ctx, batchSize, batchDB, state)
	case update:
		return c.doBatchTransactionUpdate(ctx, batchSize, batchDB, state)
	case scan:
		panic("The batch mode don't support the scan operation")
	default:
		return nil
	}
}

func (c *core) nextKeyNum(state *coreState) int64 {
	r := state.r
	keyNum := int64(0)
	if _, ok := c.keyChooser.(*generator.Exponential); ok {
		keyNum = -1
		for keyNum < 0 {
			keyNum = c.transactionInsertKeySequence.Last() - c.keyChooser.Next(r)
		}
	} else {
		keyNum = c.keyChooser.Next(r)
	}
	return keyNum
}

func (c *core) doTransactionRead(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	values, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyRow(state, keyName, values)
	}

	return nil
}

func (c *core) doTransactionReadModifyWrite(ctx context.Context, db ycsb.DB, state *coreState) error {
	start := time.Now()
	defer func() {
		measurement.Measure(ctx, "READ_MODIFY_WRITE", start, time.Now().Sub(start))
	}()

	r := state.r
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}
	defer c.putValues(values)

	readValues, err := db.Read(ctx, c.table, keyName, fields)
	if err != nil {
		return err
	}

	// Before the update, not after it. A driver is only promising that
	// what a read hands back is good until the next call on the same
	// connection, and the update is that next call. zu2 takes the promise
	// literally: a read borrows the engine's own pages and holds an epoch
	// over them, and the next call releases the epoch, after which a
	// compaction pass running beside this one is free to unmap them. So a
	// verify placed after the update reads freed pages. Checking what was
	// read before overwriting it is the right order for every driver
	// anyway, and it costs nothing.
	if c.dataIntegrity {
		c.verifyRow(state, keyName, readValues)
	}

	if err := db.Update(ctx, c.table, keyName, values); err != nil {
		return err
	}

	return nil
}

func (c *core) doTransactionInsert(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.transactionInsertKeySequence.Next(r)
	defer c.transactionInsertKeySequence.Acknowledge(keyNum)
	dbKey := c.buildKeyName(keyNum)
	values := c.buildValues(state, dbKey)
	defer c.putValues(values)

	return db.Insert(ctx, c.table, dbKey, values)
}

func (c *core) doTransactionScan(ctx context.Context, db ycsb.DB, state *coreState) error {
	r := state.r
	keyNum := c.nextKeyNum(state)
	startKeyName := c.buildKeyName(keyNum)

	scanLen := c.scanLength.Next(r)

	var fields []string
	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	c.scans.Add(1)
	c.scanAsked.Add(int64(scanLen))

	// The driver that can hand columns back without building a map a row
	// gets to. A scan returns fifty rows on average and the maps are
	// dropped unread unless dataintegrity is on, and at 32 threads
	// building them is at least 43 percent of what this call charges to
	// the engine. See tamnd/zu#750.
	if es, ok := db.(ycsb.EachScanner); ok {
		rows := 0
		last := ""
		err := es.ScanEach(ctx, c.table, startKeyName, int(scanLen), fields,
			func(values [][]byte) error {
				if c.dataIntegrity {
					c.verifyScanRow(state, startKeyName, rows, fields, values, &last)
				}
				rows++
				return nil
			})
		c.scanRows.Add(int64(rows))
		return err
	}

	rows, err := db.Scan(ctx, c.table, startKeyName, int(scanLen), fields)
	c.scanRows.Add(int64(len(rows)))

	if err == nil && c.dataIntegrity {
		c.verifyScan(state, startKeyName, rows)
	}

	return err
}

func (c *core) doTransactionUpdate(ctx context.Context, db ycsb.DB, state *coreState) error {
	keyNum := c.nextKeyNum(state)
	keyName := c.buildKeyName(keyNum)

	var values map[string][]byte
	if c.writeAllFields {
		values = c.buildValues(state, keyName)
	} else {
		values = c.buildSingleValue(state, keyName)
	}

	defer c.putValues(values)

	return db.Update(ctx, c.table, keyName, values)
}

func (c *core) doBatchTransactionRead(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	r := state.r
	var fields []string

	if !c.readAllFields {
		fieldName := state.fieldNames[c.fieldChooser.Next(r)]
		fields = append(fields, fieldName)
	} else {
		fields = state.fieldNames
	}

	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = c.buildKeyName(c.nextKeyNum(state))
	}

	rows, err := db.BatchRead(ctx, c.table, keys, fields)
	if err != nil {
		return err
	}

	if c.dataIntegrity {
		c.verifyBatchRead(state, keys, rows)
	}
	return nil
}

// verifyBatchRead is verifyRow for a batch.
//
// Two checks, because either one alone lets something through. The
// values are checked the way a scan's are, by reading the key back out
// of the value it is in, which works whatever order the rows arrive in.
// That alone is not enough: a driver that hands back the same row for
// every key of the batch returns rows that are each perfectly valid, and
// only where they sit gives it away. So when the driver returns one row
// per key, which every driver in this comparison does, row i is also
// required to be key i.
//
// mysql and tikv ask for the batch as one IN query with no order and
// collapse duplicate keys, so they come back with a different count and
// get the value check only. That is the most that can be said about a
// result that does not claim to be positional.
func (c *core) verifyBatchRead(state *coreState, keys []string, rows []map[string][]byte) {
	positional := len(rows) == len(keys)
	for i, values := range rows {
		if len(values) == 0 {
			util.Fatalf("batch read: row %d of %d came back with no fields at all", i, len(rows))
		}

		key := ""
		for fieldKey, value := range values {
			at := bytes.IndexByte(value, ':')
			if at < 0 {
				util.Fatalf("batch read: row %d field %s carries no key, got %q", i, fieldKey, value)
			}
			rowKey := string(value[:at])
			if key == "" {
				key = rowKey
			} else if rowKey != key {
				util.Fatalf("batch read: row %d mixes rows, field %s belongs to %s and the rest to %s",
					i, fieldKey, rowKey, key)
			}
			expected := c.buildDeterministicValue(state, rowKey, fieldKey)
			if !bytes.Equal(expected, value) {
				util.Fatalf("batch read: row %d field %s, expect %q, but got %q",
					i, fieldKey, expected, value)
			}
		}

		if positional && key != keys[i] {
			util.Fatalf("batch read: asked for %s at position %d and got %s", keys[i], i, key)
		}
	}
}

func (c *core) doBatchTransactionInsert(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	r := state.r
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.transactionInsertKeySequence.Next(r)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
		c.transactionInsertKeySequence.Acknowledge(keyNum)
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	return db.BatchInsert(ctx, c.table, keys, values)
}

func (c *core) doBatchTransactionUpdate(ctx context.Context, batchSize int, db ycsb.BatchDB, state *coreState) error {
	keys := make([]string, batchSize)
	values := make([]map[string][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keyNum := c.nextKeyNum(state)
		keyName := c.buildKeyName(keyNum)
		keys[i] = keyName
		if c.writeAllFields {
			values[i] = c.buildValues(state, keyName)
		} else {
			values[i] = c.buildSingleValue(state, keyName)
		}
	}

	defer func() {
		for _, value := range values {
			c.putValues(value)
		}
	}()

	return db.BatchUpdate(ctx, c.table, keys, values)
}

// CoreCreator creates the Core workload.
type coreCreator struct {
}

// Create implements the WorkloadCreator Create interface.
func (coreCreator) Create(p *properties.Properties) (ycsb.Workload, error) {
	c := new(core)
	c.p = p
	c.table = p.GetString(prop.TableName, prop.TableNameDefault)
	c.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	c.fieldNames = make([]string, c.fieldCount)
	for i := int64(0); i < c.fieldCount; i++ {
		c.fieldNames[i] = fmt.Sprintf("field%d", i)
	}
	c.fieldLengthGenerator = getFieldLengthGenerator(p)
	c.recordCount = p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
	if c.recordCount == 0 {
		c.recordCount = int64(math.MaxInt32)
	}

	requestDistrib := p.GetString(prop.RequestDistribution, prop.RequestDistributionDefault)
	minScanLength := p.GetInt64(prop.MinScanLength, prop.MinScanLengthDefault)
	maxScanLength := p.GetInt64(prop.MaxScanLength, prop.MaxScanLengthDefault)
	scanLengthDistrib := p.GetString(prop.ScanLengthDistribution, prop.ScanLengthDistributionDefault)

	insertStart := p.GetInt64(prop.InsertStart, prop.InsertStartDefault)
	insertCount := p.GetInt64(prop.InsertCount, c.recordCount-insertStart)
	if c.recordCount < insertStart+insertCount {
		util.Fatalf("record count %d must be bigger than insert start %d + count %d",
			c.recordCount, insertStart, insertCount)
	}
	c.zeroPadding = p.GetInt64(prop.ZeroPadding, prop.ZeroPaddingDefault)
	c.keyPrefix = p.GetString(prop.KeyPrefix, prop.KeyPrefixDefault)
	c.readAllFields = p.GetBool(prop.ReadAllFields, prop.ReadALlFieldsDefault)
	c.writeAllFields = p.GetBool(prop.WriteAllFields, prop.WriteAllFieldsDefault)
	c.dataIntegrity = p.GetBool(prop.DataIntegrity, prop.DataIntegrityDefault)
	fieldLengthDistribution := p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)
	if c.dataIntegrity && fieldLengthDistribution != "constant" {
		util.Fatal("must have constant field size to check data integrity")
	}

	if p.GetString(prop.InsertOrder, prop.InsertOrderDefault) == "hashed" {
		c.orderedInserts = false
	} else {
		c.orderedInserts = true
	}

	c.keySequence = generator.NewCounter(insertStart)
	c.operationChooser = createOperationGenerator(p)
	var keyrangeLowerBound int64 = insertStart
	var keyrangeUpperBound int64 = insertStart + insertCount - 1

	c.transactionInsertKeySequence = generator.NewAcknowledgedCounter(c.recordCount)
	switch requestDistrib {
	case "uniform":
		c.keyChooser = generator.NewUniform(keyrangeLowerBound, keyrangeUpperBound)
	case "sequential":
		c.keyChooser = generator.NewSequential(keyrangeLowerBound, keyrangeUpperBound)
	case "zipfian":
		insertProportion := p.GetFloat64(prop.InsertProportion, prop.InsertProportionDefault)
		opCount := p.GetInt64(prop.OperationCount, 0)
		expectedNewKeys := int64(float64(opCount) * insertProportion * 2.0)
		// The last loaded key is insertStart+insertCount-1, the same
		// bound the uniform and sequential branches use. Without the
		// minus one this range includes a key that was never loaded and
		// never will be, and a read of it comes back empty. With
		// dataintegrity on that is a failed run; with it off, which is
		// how a sweep runs, it is a read that returns nothing and
		// reports as the fastest read in the sample.
		keyrangeUpperBound = insertStart + insertCount - 1 + expectedNewKeys
		c.keyChooser = generator.NewScrambledZipfian(keyrangeLowerBound, keyrangeUpperBound, generator.ZipfianConstant)
	case "latest":
		c.keyChooser = generator.NewSkewedLatest(c.transactionInsertKeySequence)
	case "hotspot":
		hotsetFraction := p.GetFloat64(prop.HotspotDataFraction, prop.HotspotDataFractionDefault)
		hotopnFraction := p.GetFloat64(prop.HotspotOpnFraction, prop.HotspotOpnFractionDefault)
		c.keyChooser = generator.NewHotspot(keyrangeLowerBound, keyrangeUpperBound, hotsetFraction, hotopnFraction)
	case "exponential":
		percentile := p.GetFloat64(prop.ExponentialPercentile, prop.ExponentialPercentileDefault)
		frac := p.GetFloat64(prop.ExponentialFrac, prop.ExponentialFracDefault)
		c.keyChooser = generator.NewExponential(percentile, float64(c.recordCount)*frac)
	default:
		util.Fatalf("unknown request distribution %s", requestDistrib)
	}
	fmt.Println(fmt.Sprintf("Using request distribution '%s' a keyrange of [%d %d]", requestDistrib, keyrangeLowerBound, keyrangeUpperBound))

	c.fieldChooser = generator.NewUniform(0, c.fieldCount-1)
	switch scanLengthDistrib {
	case "uniform":
		c.scanLength = generator.NewUniform(minScanLength, maxScanLength)
	case "zipfian":
		c.scanLength = generator.NewZipfianWithRange(minScanLength, maxScanLength, generator.ZipfianConstant)
	default:
		util.Fatalf("distribution %s not allowed for scan length", scanLengthDistrib)
	}

	c.insertionRetryLimit = p.GetInt64(prop.InsertionRetryLimit, prop.InsertionRetryLimitDefault)
	c.insertionRetryInterval = p.GetInt64(prop.InsertionRetryInterval, prop.InsertionRetryIntervalDefault)

	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	c.valuePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, fieldLength)
		},
	}

	return c, nil
}

func init() {
	ycsb.RegisterWorkloadCreator("core", coreCreator{})
	ycsb.RegisterWorkloadCreator("site.ycsb.workloads.CoreWorkload", coreCreator{})
}

// slowBuildKeyName is what buildKeyName used to be, kept so the tests
// can hold the new one against it and the benchmark can price the
// difference. Nothing on a run calls this.
func (c *core) slowBuildKeyName(keyNum int64) string {
	if !c.orderedInserts {
		keyNum = util.Hash64(keyNum)
	}
	return fmt.Sprintf("%s%0[3]*[2]d", c.keyPrefix, keyNum, c.zeroPadding)
}
