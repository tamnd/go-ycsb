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

package measurement

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

var header = []string{"Operation", "Takes(s)", "Count", "OPS", "Avg(us)", "Min(us)", "Max(us)", "50th(us)", "90th(us)", "95th(us)", "99th(us)", "99.9th(us)", "99.99th(us)"}

// A measurer that can absorb another one of its own kind.
//
// Every worker records into a measurer of its own, so a summary or a
// final output has to put them back together before it reads a count
// or a percentile off them.
type merger interface {
	ycsb.Measurer

	// merge folds other into the receiver. The caller holds whatever
	// lock protects other.
	merge(other ycsb.Measurer)
}

// One worker's measurer and the lock that guards it.
//
// The lock is still needed, because the reporting goroutine reads a
// shard every log interval while its worker is writing to it, and
// neither an hdr histogram nor a map is safe against that. What has
// changed is who contends for it: a worker only ever takes its own,
// so the common case is an uncontended lock rather than a process
// wide one that every operation in the run has to queue behind.
type shard struct {
	sync.Mutex

	measurer merger
}

func (s *shard) measure(op string, start time.Time, lan time.Duration) {
	s.Lock()
	s.measurer.Measure(op, start, lan)
	s.Unlock()
}

type measurement struct {
	p *properties.Properties

	// One per worker, plus a spare on the end for anything that
	// measures without having gone through InitThread.
	shards []*shard
}

// The context key a worker's shard is carried under.
type shardKey struct{}

// InitThread hands a worker its own measurer for the rest of the run.
//
// The shard is put on the context because that is the only thing every
// measuring site already has in hand: the wrapper methods in
// pkg/client and the read-modify-write in pkg/workload all take one.
func InitThread(ctx context.Context, threadID int) context.Context {
	return context.WithValue(ctx, shardKey{}, globalMeasure.shardFor(threadID))
}

func (m *measurement) shardFor(threadID int) *shard {
	if threadID < 0 || threadID >= len(m.shards)-1 {
		// The spare. A thread count that disagrees with the property
		// lands here and shares one lock, which is slower but right.
		return m.shards[len(m.shards)-1]
	}
	return m.shards[threadID]
}

func (m *measurement) shardOf(ctx context.Context) *shard {
	if s, ok := ctx.Value(shardKey{}).(*shard); ok {
		return s
	}
	return m.shards[len(m.shards)-1]
}

func (m *measurement) newMeasurer() merger {
	measurementType := m.p.GetString(prop.MeasurementType, prop.MeasurementTypeDefault)
	switch measurementType {
	case "histogram":
		return InitHistograms(m.p)
	case "raw", "csv":
		return InitCSV()
	default:
		panic("unsupported measurement type: " + measurementType)
	}
}

// merged folds every shard into one measurer, which is what the run
// used to keep behind the global lock.
func (m *measurement) merged() merger {
	out := m.newMeasurer()
	for _, s := range m.shards {
		s.Lock()
		out.merge(s.measurer)
		s.Unlock()
	}
	return out
}

func (m *measurement) output() {
	out := m.merged()
	out.GenerateExtendedOutputs()

	outFile := m.p.GetString(prop.MeasurementRawOutputFile, "")
	var w *bufio.Writer
	if outFile == "" {
		w = bufio.NewWriter(os.Stdout)
	} else {
		f, err := os.Create(outFile)
		if err != nil {
			panic("failed to create output file: " + err.Error())
		}
		defer f.Close()
		w = bufio.NewWriter(f)
	}

	err := out.Output(w)
	if err != nil {
		panic("failed to write output: " + err.Error())
	}

	err = w.Flush()
	if err != nil {
		panic("failed to flush output: " + err.Error())
	}

	// After the table and on stderr, so a script collecting the table on
	// stdout is unaffected and a person reading the run sees it. Once, on
	// the final output rather than on each periodic summary, because a
	// note repeated every ten seconds is one nobody reads.
	if hs, ok := out.(*histograms); ok {
		for _, note := range hs.errorNotes() {
			fmt.Fprintln(os.Stderr, note)
		}
	}
}

func (m *measurement) summary() {
	m.merged().Summary()
}

// InitMeasure initializes the global measurement.
func InitMeasure(p *properties.Properties) {
	globalMeasure = new(measurement)
	globalMeasure.p = p
	threadCount := p.GetInt(prop.ThreadCount, 1)
	if threadCount < 1 {
		threadCount = 1
	}
	globalMeasure.shards = make([]*shard, threadCount+1)
	for i := range globalMeasure.shards {
		globalMeasure.shards[i] = &shard{measurer: globalMeasure.newMeasurer()}
	}
	// What the host's clock can resolve, once, before anything is timed
	// with it. A run whose percentile columns are the timer tick rather
	// than the engine has to say so, or the zeros read as a result.
	reportClock(os.Stdout)
	EnableWarmUp(p.GetInt64(prop.WarmUpTime, 0) > 0)
}

// Output prints the complete measurements.
func Output() {
	globalMeasure.output()
}

// Summary prints the measurement summary.
func Summary() {
	globalMeasure.summary()
}

// EnableWarmUp sets whether to enable warm-up.
func EnableWarmUp(b bool) {
	if b {
		atomic.StoreInt32(&warmUp, 1)
	} else {
		atomic.StoreInt32(&warmUp, 0)
	}
}

// IsWarmUpFinished returns whether warm-up is finished or not.
func IsWarmUpFinished() bool {
	return atomic.LoadInt32(&warmUp) == 0
}

// Measure measures the operation into the shard the context carries.
func Measure(ctx context.Context, op string, start time.Time, lan time.Duration) {
	if IsWarmUpFinished() {
		globalMeasure.shardOf(ctx).measure(op, start, lan)
	}
}

var globalMeasure *measurement
var warmUp int32 // use as bool, 1 means in warmup progress, 0 means warmup finished.
