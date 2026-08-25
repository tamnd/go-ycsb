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

//go:build zu2 && zu2bench

// The floor ladder again, this time at several thread counts, because
// the question that is open is a scaling question and the ladder next
// door cannot answer it. tamnd/zu#613.
//
// Where that issue stands. Workload C is a hundred per cent point reads
// and zu2 gains 1.58x from sixteen threads there while sqlite gains
// 2.40x, so the lead falls from 12.2x at one thread to 8.0x at sixteen.
// Those two rows are the only ones in the sweep under 10x. A native
// read scaling sweep with no Go in the process was the first thing owed
// and it has been run: the engine scales 11.85x, and the disjoint and
// shared key passes are identical, so there is no shared write in the
// engine to find. Whatever costs the other ten x is on this side of the
// boundary.
//
// "On the Go side" is not an answer anybody can act on, for the same
// reason "390 ns of overhead" was not an answer in #645. So this does to
// the scaling question what floor_test.go did to the latency question:
// runs the same read at a series of depths, each one the previous plus
// one thing, and at 1, 2, 4, 8, 16 and 32 threads.
//
// Read it by dividing each row's throughput by its own one thread
// throughput. A step that scales and a step above it that does not name
// the thing between them as the cost:
//
//	Key       building table:key in the session's scratch, no cgo at
//	          all, so this is the control. It has no shared state in it
//	          and it has to scale linearly. If it does not, the machine
//	          or the run is wrong and nothing below it means anything.
//	Crossing  one cgo call with one argument. Key against this is what
//	          the boundary itself costs under contention.
//	Nop       the shape of zu2_read against a C function that does
//	          nothing, with the key and the out slots in Go memory,
//	          which is what the adapter really passes.
//	NopNoGo   the same with every argument in C memory. Against Nop it
//	          is what cgo charges for a call that touches Go's heap,
//	          which is the pointer check, and that is a prime suspect
//	          for a cost that grows with threads.
//	Read      the engine's read, value left in the session's buffer.
//	Copy      plus the copy into Go memory.
//	Decode    plus the row decoder and the map, which is adapter Read.
//	Iface     the same through the ycsb.DB interface, which is how the
//	          client reaches it.
//
// Threads are explicit rather than left to RunParallel, because the
// thread count is the independent variable here and coupling it to
// GOMAXPROCS would make it one setting rather than a sweep.
//
// b.N is per goroutine and not split across them, so -benchtime 200000x
// means each thread does two hundred thousand operations and the 32
// thread point does 6.4 million. That makes the framework's own ns/op
// column mean the average time one thread waited for one operation,
// which is flat down a rung under perfect scaling and climbing when
// something is contended, so the column that is printed by default is
// already the one to read. ops/s and ops/s/thread are reported beside
// it. Splitting b.N instead, which is what this did first, had the 32
// thread points doing a millisecond of work behind a barrier that takes
// tens of microseconds to open, and the Key rung, which has no shared
// state in it whatsoever, came out faster per thread at two threads
// than at one.
//
// GOMAXPROCS is left alone. A run at 32 threads on a box with 8 cores
// measures the scheduler and not the adapter, so run this on the host
// the claim is being made on, with at least as many cores as the
// largest thread count.
//
// Never on the laptop for a published number, same rule as everything
// else here. On a server:
//
//	go test -tags "zu2 zu2bench" -run '^$' -bench Scale -benchtime 200000x ./db/zu2
//
// Each rung says so itself if the run was too short to mean anything.
package zu2

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// The thread counts the sweep uses, plus the powers between them so a
// curve that bends has somewhere to bend.
var scaleThreads = []int{1, 2, 4, 8, 16, 32}

// Same database as the floor ladder, so the two files' one thread
// numbers are comparable and the ladder here can be checked against the
// one there rather than standing on its own.
type scaleFixture struct {
	db   ycsb.DB
	zu2  *zu2DB
	keys []string
	ckey *floorKeys
}

func newScaleFixture(tb testing.TB, threads int) *scaleFixture {
	tb.Helper()

	p := properties.NewProperties()
	p.MustSet(zu2Path, filepath.Join(tb.TempDir(), "scale.zu2"))
	p.MustSet(zu2IndexBuckets, fmt.Sprint(floorRecords/4+1))
	p.MustSet(zu2StorageReport, "false")
	// The engine refuses a session past this and does not queue for one,
	// so it has to be asked for up front. The loader takes one as well.
	p.MustSet(zu2Sessions, fmt.Sprint(threads+4))
	p.MustSet(prop.FieldCount, fmt.Sprint(floorFields))
	p.MustSet(prop.FieldLength, fmt.Sprint(floorLength))
	p.MustSet(prop.ThreadCount, fmt.Sprint(threads))

	db, err := zu2Creator{}.Create(p)
	if err != nil {
		tb.Skipf("zu2 did not open, so there is nothing to measure: %v", err)
	}
	tb.Cleanup(func() { db.Close() })

	f := &scaleFixture{
		db:   db,
		zu2:  db.(*zu2DB),
		keys: make([]string, floorRecords),
	}

	ctx := db.InitThread(context.Background(), 0, threads)
	value := make([]byte, floorLength)
	for i := range value {
		value[i] = 'v'
	}
	values := map[string][]byte{"field0": value}
	for i := 0; i < floorRecords; i++ {
		f.keys[i] = fmt.Sprintf("user%019d", i)
		if err := db.Insert(ctx, floorTable, f.keys[i], values); err != nil {
			tb.Fatalf("insert %d: %v", i, err)
		}
	}

	f.ckey = newFloorKeys(f.keys, floorTable)
	tb.Cleanup(f.ckey.free)
	return f
}

// The key of the i-th operation of thread t.
//
// Every thread walks the whole key space rather than a slice of it, and
// the stride is offset per thread so two threads are rarely on the same
// record at the same moment. That is deliberate and it is the case the
// sweep runs: workload C gives every worker the same generator over the
// same range. The native sweep already showed disjoint and shared
// ranges coming out identical in the engine, so the shared case is the
// one worth carrying here.
func (f *scaleFixture) key(t, i int) string {
	return f.keys[(i*7919+t*104729)%floorRecords]
}

func (f *scaleFixture) index(t, i int) int {
	return (i*7919 + t*104729) % floorRecords
}

// One rung of the ladder. `each` is one operation, given the goroutine's
// own session and context, its thread number and the operation number
// within that thread. It returns an error rather than calling b.Fatal
// because a Fatal from a non test goroutine does not stop the run.
type scaleStep struct {
	name string
	each func(f *scaleFixture, s *session, ctx context.Context, t, i int) error
}

var scaleSteps = []scaleStep{
	{"Key", func(f *scaleFixture, s *session, _ context.Context, t, i int) error {
		if len(s.rowKey(floorTable, f.key(t, i))) == 0 {
			return fmt.Errorf("empty key")
		}
		return nil
	}},
	{"Crossing", func(f *scaleFixture, _ *session, _ context.Context, _, _ int) error {
		floorCrossing(f.zu2)
		return nil
	}},
	{"Nop", func(f *scaleFixture, s *session, _ context.Context, t, i int) error {
		if floorNop(s, s.rowKey(floorTable, f.key(t, i))) == 0 {
			return fmt.Errorf("the nop wrote nothing")
		}
		return nil
	}},
	{"NopNoGo", func(f *scaleFixture, s *session, _ context.Context, t, i int) error {
		if floorNopNoGoPointers(s, f.ckey, f.index(t, i)) == 0 {
			return fmt.Errorf("the nop wrote nothing")
		}
		return nil
	}},
	{"Read", func(f *scaleFixture, s *session, _ context.Context, t, i int) error {
		n, err := floorRead(s, s.rowKey(floorTable, f.key(t, i)))
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("key %q is not there", f.key(t, i))
		}
		return nil
	}},
	{"Copy", func(f *scaleFixture, s *session, _ context.Context, t, i int) error {
		row, err := floorReadCopy(s, s.rowKey(floorTable, f.key(t, i)))
		if err != nil {
			return err
		}
		if len(row) == 0 {
			return fmt.Errorf("key %q is not there", f.key(t, i))
		}
		return nil
	}},
	{"Decode", func(f *scaleFixture, _ *session, ctx context.Context, t, i int) error {
		row, err := f.zu2.Read(ctx, floorTable, f.key(t, i), nil)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("key %q is not there", f.key(t, i))
		}
		return nil
	}},
	{"Iface", func(f *scaleFixture, _ *session, ctx context.Context, t, i int) error {
		row, err := f.db.Read(ctx, floorTable, f.key(t, i), nil)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("key %q is not there", f.key(t, i))
		}
		return nil
	}},
}

func BenchmarkScale(b *testing.B) {
	if runtime.GOMAXPROCS(0) < scaleThreads[len(scaleThreads)-1] {
		b.Logf("GOMAXPROCS is %d and the sweep goes to %d, so the top points measure the scheduler",
			runtime.GOMAXPROCS(0), scaleThreads[len(scaleThreads)-1])
	}
	for _, step := range scaleSteps {
		b.Run(step.name, func(b *testing.B) {
			for _, threads := range scaleThreads {
				b.Run(fmt.Sprintf("t%d", threads), func(b *testing.B) {
					runScale(b, step, threads)
				})
			}
		})
	}
}

// Operations each goroutine runs before the clock starts. Twenty
// thousand is a few milliseconds on the slowest rung and it is enough
// for the Key rung to come out flat on the first count, which is the
// thing it has to buy.
const scaleWarm = 20_000

func runScale(b *testing.B, step scaleStep, threads int) {
	f := newScaleFixture(b, threads)

	// Every goroutine's session is opened before the clock starts. A
	// session open is a lock on the database's session table and it
	// happens once a run, so timing it would charge the 32 thread point
	// for 32 of them and call it contention.
	ctxs := make([]context.Context, threads)
	sess := make([]*session, threads)
	for t := 0; t < threads; t++ {
		ctxs[t] = f.db.InitThread(context.Background(), t, threads)
		sess[t] = sessionOf(ctxs[t])
	}

	// b.N operations per goroutine, not b.N split across them.
	//
	// Splitting was the obvious way and it is wrong here for two
	// reasons. The measurable one: -benchtime 50000x then means 1562
	// operations on each goroutine at the 32 thread point, which at a
	// couple of hundred nanoseconds apiece is under a millisecond of
	// work behind a barrier that takes tens of microseconds to open, so
	// the top of the sweep measures goroutine wakeup. It showed up
	// immediately as a Key row, which has no shared state in it at all,
	// reporting more throughput per thread at two threads than at one.
	//
	// The other is that it makes the framework's own ns/op column say
	// something useful. Elapsed over b.N with b.N per thread is the
	// average time one thread waited for one operation, so perfect
	// scaling holds that column flat down a rung and the thread count
	// where it starts climbing is the answer. Split across threads it
	// would fall by construction and say nothing.
	//
	// The cost is that -benchtime Nx now means N per thread, so the 32
	// thread point does 32N operations and takes proportionally longer.
	// That is the right trade for a sweep whose whole purpose is
	// comparing the rungs against each other.
	errs := make([]error, threads)
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	start := make(chan struct{})
	ready.Add(threads)
	wg.Add(threads)
	for t := 0; t < threads; t++ {
		go func(t, n int) {
			defer wg.Done()
			// A warm pass before the barrier, outside the timer.
			//
			// The fixture inserts two hundred thousand records right
			// before this, so the first operations of the first measured
			// run walk a cache and a heap in the state that loop left
			// them, and they are two to three times slower than the same
			// operations a moment later. On the Key rung, which touches no
			// shared state at all, that alone made the first -count of
			// every thread point come out slow and read as if one thread
			// scaled worse than two. The rung being flat is the control
			// that says the instrument is sound, so the warm pass is not
			// optional here.
			for i := 0; i < scaleWarm && i < n; i++ {
				if err := step.each(f, sess[t], ctxs[t], t, i); err != nil {
					errs[t] = err
					break
				}
			}

			// Started, warm and parked before the clock runs, so neither
			// goroutine creation nor the cold pass is in the measurement.
			// At 32 threads and a short benchtime the first would
			// otherwise be most of it.
			ready.Done()
			<-start
			for i := 0; i < n; i++ {
				if err := step.each(f, sess[t], ctxs[t], t, i); err != nil {
					errs[t] = err
					return
				}
			}
		}(t, b.N)
	}
	ready.Wait()

	b.ResetTimer()
	close(start)
	wg.Wait()
	b.StopTimer()

	for t, err := range errs {
		if err != nil {
			b.Fatalf("thread %d: %v", t, err)
		}
	}

	// A run this short is a measurement of the barrier and not of the
	// work, so it says so rather than printing a number that will be
	// read as one. Ten milliseconds against a barrier that opens in tens
	// of microseconds leaves the error under a per cent.
	//
	// b.N > 1 because testing always runs the body once with b.N of 1
	// before the real iteration, whatever -benchtime says. That probe is
	// always under ten milliseconds and warning about it would put the
	// warning on every rung of every sweep, including the sound ones.
	secs := b.Elapsed().Seconds()
	if b.N > 1 && secs < 0.010 {
		b.Logf("only %.1f ms of work at %d threads, raise -benchtime or this is measuring goroutine wakeup",
			secs*1000, threads)
	}
	if secs > 0 {
		// b.N is per thread, so the total is that times the thread
		// count. ops/s/thread is the figure to read down a rung:
		// perfect scaling holds it flat, and the thread count where it
		// starts falling is what this file exists to find.
		ops := float64(b.N) * float64(threads) / secs
		b.ReportMetric(ops, "ops/s")
		b.ReportMetric(ops/float64(threads), "ops/s/thread")
	}
}
