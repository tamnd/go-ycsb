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
