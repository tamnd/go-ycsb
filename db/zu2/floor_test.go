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

// What a point read costs on this side of the library, taken apart.
//
// tamnd/zu#645. A read through the harness costs about 390 ns before
// the engine is reached, and a ratio taken over a floor that big is not
// the ratio between two engines: it compresses toward 1, and hardest
// when the engine is fastest. Before anybody optimises the floor it has
// to be attributed, because "390 ns of overhead" names no line of code
// and cannot be argued with.
//
// So this file measures the same read at a series of depths, each one
// the previous plus one thing:
//
//	Key               building table:key in the session's scratch
//	Crossing          one cgo call, one argument, no work behind it
//	NopNoGoPointers   the shape of zu2_read against a C function that
//	                  does nothing, with every argument in C memory
//	Nop               the same, with the key and the out slots in Go
//	                  memory, which is what the adapter really passes
//	ReadNoGoPointers  the engine's read, arguments in C memory
//	Read              the engine's read as the adapter makes it
//	Copy              read plus C.GoBytes of the value into Go memory
//	Decode            copy plus the row decoder, which is Read
//	Iface             Decode reached through the ycsb.DB interface
//
// Subtract a row from the one below it and the difference is what that
// step costs. Two of the pairs are there to separate things that are
// usually assumed together: Crossing against Nop is what the arguments
// cost rather than the boundary, and NopNoGoPointers against Nop is
// what cgo charges for arguments that point into the Go heap.
//
// What is left over between `Iface` here and a read through the running
// harness is the client: key generation, the measurement histogram, the
// context and the operation dispatch, none of which this file touches.
// Measure that with `go-ycsb run basic`, which runs the same client
// against a database that does nothing, and the two together should
// come out at the harness's own figure. On an M4 they do, to two per
// cent: 800 ns of adapter plus 303 ns of client against 1127 ns
// measured, of which the engine itself is about 350.
//
// The engine's own cost is measured from C, with no Go in the process
// at all, by crates/zu2-capi/tests/readfloor.c in tamnd/zu.
//
// The numbers are per operation on one thread against a database that
// fits in memory, which is the case the milestone is claimed in.
//
//	go test -tags "zu2 zu2bench" -run '^$' -bench Floor -benchtime 2000000x ./db/zu2
package zu2

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// The shape the sweeps use: a million rows is too slow to build for
// every benchmark, and the point here is the cost of the call and not
// the cost of a cache miss, so this is the smallest database whose
// index behaves like the big one.
const (
	floorRecords = 200_000
	floorFields  = 1
	floorLength  = 1000
	floorTable   = "usertable"
)

// A database with floorRecords rows in it, the session to read them
// through, and the keys to ask for.
type floorFixture struct {
	db   ycsb.DB
	zu2  *zu2DB
	ctx  context.Context
	sess *session
	keys []string
}

func newFloorFixture(tb testing.TB) *floorFixture {
	tb.Helper()

	p := properties.NewProperties()
	p.MustSet(zu2Path, filepath.Join(tb.TempDir(), "floor.zu2"))
	// One bucket per four records is the sweep's setting and keeps the
	// index off its growth path, which is a different measurement.
	p.MustSet(zu2IndexBuckets, fmt.Sprint(floorRecords/4+1))
	p.MustSet(zu2StorageReport, "false")
	p.MustSet(prop.FieldCount, fmt.Sprint(floorFields))
	p.MustSet(prop.FieldLength, fmt.Sprint(floorLength))
	p.MustSet(prop.ThreadCount, "1")

	db, err := zu2Creator{}.Create(p)
	if err != nil {
		tb.Skipf("zu2 did not open, so there is nothing to measure: %v", err)
	}
	tb.Cleanup(func() { db.Close() })

	ctx := db.InitThread(context.Background(), 0, 1)
	f := &floorFixture{
		db:   db,
		zu2:  db.(*zu2DB),
		ctx:  ctx,
		sess: sessionOf(ctx),
		keys: make([]string, floorRecords),
	}

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
	return f
}

// The key of the i-th operation. A stride that is coprime with the
// record count walks every record once before it repeats, which gives
// the uniform read of workload C without a generator in the timed loop:
// a zipfian here would be measuring the generator on the row whose
// whole point is that it measures nothing else.
func (f *floorFixture) key(i int) string {
	return f.keys[(i*7919)%floorRecords]
}

func BenchmarkFloorKey(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rk := f.sess.rowKey(floorTable, f.key(i))
		if len(rk) == 0 {
			b.Fatal("empty key")
		}
	}
}

// One crossing and nothing else. zu2_nodes reads a counter out of the
// database and returns it, so what this times is cgo's own cost: the
// stack switch and whatever the runtime does around a call that might
// block.
func BenchmarkFloorCrossing(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		floorCrossing(f.zu2)
	}
}

// The same call shape with the engine taken out of it: six arguments
// across the boundary and a C function that only writes the three out
// slots. Against BenchmarkFloorCrossing it is what the arguments cost,
// and against BenchmarkFloorRead it is what the engine costs.
func BenchmarkFloorNop(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rk := f.sess.rowKey(floorTable, f.key(i))
		if floorNop(f.sess, rk) == 0 {
			b.Fatal("the nop wrote nothing")
		}
	}
}

// The nop call again with the key and the out slots in C memory.
func BenchmarkFloorNopNoGoPointers(b *testing.B) {
	f := newFloorFixture(b)
	keys := newFloorKeys(f.keys, floorTable)
	defer keys.free()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if floorNopNoGoPointers(f.sess, keys, (i*7919)%floorRecords) == 0 {
			b.Fatal("the nop wrote nothing")
		}
	}
}

// The engine's read, with the value left in the session's buffer.
func BenchmarkFloorRead(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rk := f.sess.rowKey(floorTable, f.key(i))
		n, err := floorRead(f.sess, rk)
		if err != nil {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatalf("key %q is not there", f.key(i))
		}
	}
}

// The same read with the key and the three out slots in C memory, so
// the call carries no Go pointer. Against the row above it, the
// difference is what cgo charges for a call that touches Go's heap.
func BenchmarkFloorReadNoGoPointers(b *testing.B) {
	f := newFloorFixture(b)
	keys := newFloorKeys(f.keys, floorTable)
	defer keys.free()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := floorReadNoGoPointers(f.sess, keys, (i*7919)%floorRecords)
		if err != nil {
			b.Fatal(err)
		}
		if n == 0 {
			b.Fatal("a key that was written is not there")
		}
	}
}

// The same, plus the copy into Go memory the adapter has to make
// because the session's buffer lives only until the next call on it.
func BenchmarkFloorCopy(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rk := f.sess.rowKey(floorTable, f.key(i))
		row, err := floorReadCopy(f.sess, rk)
		if err != nil {
			b.Fatal(err)
		}
		if len(row) == 0 {
			b.Fatalf("key %q is not there", f.key(i))
		}
	}
}

// The adapter's own Read, which is the copy plus the row decoder and
// the map it hands back.
func BenchmarkFloorDecode(b *testing.B) {
	f := newFloorFixture(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row, err := f.zu2.Read(f.ctx, floorTable, f.key(i), nil)
		if err != nil {
			b.Fatal(err)
		}
		if row == nil {
			b.Fatalf("key %q is not there", f.key(i))
		}
	}
}

// The same call reached the way the client reaches it, through the
// interface, so the difference from the row above is the dispatch.
func BenchmarkFloorIface(b *testing.B) {
	f := newFloorFixture(b)
	db := f.db
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row, err := db.Read(f.ctx, floorTable, f.key(i), nil)
		if err != nil {
			b.Fatal(err)
		}
		if row == nil {
			b.Fatalf("key %q is not there", f.key(i))
		}
	}
}
