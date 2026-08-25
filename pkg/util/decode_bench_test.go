package util

import (
	"fmt"
	"strings"
	"testing"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
)

func benchRow(b *testing.B) (*RowCodec, []byte) {
	p := properties.NewProperties()
	p.Set(prop.FieldCount, "10")
	r := NewRowCodec(p)
	values := make(map[string][]byte, 10)
	for i := 0; i < 10; i++ {
		values[fmt.Sprintf("field%d", i)] = []byte(strings.Repeat("a", 100))
	}
	row, err := r.Encode(nil, values)
	if err != nil {
		b.Fatal(err)
	}
	return r, row
}

func BenchmarkDecodeWholeRow(b *testing.B) {
	r, row := benchRow(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Decode(row, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRowMap(b *testing.B) {
	_, row := benchRow(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeRow(row); err != nil {
			b.Fatal(err)
		}
	}
}

// The pooled path, which is what the zu2 and lmdb drivers take: no map
// allocation, the clear and the insertions only. The gap between this
// and BenchmarkEachColumn below is what the map costs after pooling has
// already taken the allocation out, and that is the number that says
// whether handing columns back instead of a map is worth an interface
// change.
func BenchmarkDecodeInto(b *testing.B) {
	r, row := benchRow(b)
	into := make(map[string][]byte, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.DecodeInto(row, nil, into); err != nil {
			b.Fatal(err)
		}
	}
}

// One scan's worth of it. Workload E asks for fifty rows on average and
// the driver decodes every one of them inside the single Scan call the
// harness times, so this is the shape that actually shows up in a
// published latency: fifty rows through fifty pooled maps.
func BenchmarkDecodeIntoScan(b *testing.B) {
	const rows = 50
	r, row := benchRow(b)
	pool := make([]map[string][]byte, rows)
	for i := range pool {
		pool[i] = make(map[string][]byte, 16)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < rows; j++ {
			if _, err := r.DecodeInto(row, nil, pool[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// The same fifty rows on every core at once, which is the state the
// published numbers are taken in. A pooled decode allocates nothing, so
// what changes between here and the serial version is contention for
// cache and for memory bandwidth, and that is not a small correction on
// a workload whose whole cost is touching a row it just read.
func BenchmarkDecodeIntoScanParallel(b *testing.B) {
	const rows = 50
	r, row := benchRow(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		pool := make([]map[string][]byte, rows)
		for i := range pool {
			pool[i] = make(map[string][]byte, 16)
		}
		for pb.Next() {
			for j := 0; j < rows; j++ {
				if _, err := r.DecodeInto(row, nil, pool[j]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// The floor under the one above: the same fifty rows on every core with
// the map taken out. Whatever separates the two is what the harness
// could stop paying if a driver handed columns back instead of a map.
func BenchmarkEachColumnScanParallel(b *testing.B) {
	const rows = 50
	_, row := benchRow(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			for j := 0; j < rows; j++ {
				if err := EachColumn(row, func(int64, []byte) { n++ }); err != nil {
					b.Fatal(err)
				}
			}
		}
		if n == 0 {
			b.Error("the walk found no columns")
		}
	})
}

func BenchmarkEachColumn(b *testing.B) {
	_, row := benchRow(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		if err := EachColumn(row, func(int64, []byte) { n++ }); err != nil {
			b.Fatal(err)
		}
	}
}
