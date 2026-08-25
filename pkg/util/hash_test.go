package util

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"testing"
)

// Hash64 decides which key an operation lands on, so an implementation
// that hashes differently is a different benchmark. This holds the
// inlined FNV-1a against hash/fnv, which is what it replaced.
func TestHash64MatchesFnv(t *testing.T) {
	check := func(n int64) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[0:8], uint64(n))
		h := fnv.New64a()
		h.Write(b[0:8])
		want := int64(h.Sum64())
		if want < 0 {
			want = -want
		}
		if got := Hash64(n); got != want {
			t.Fatalf("n %d: got %d want %d", n, got, want)
		}
	}
	for n := int64(-1000); n <= 1000; n++ {
		check(n)
	}
	for _, n := range []int64{math.MaxInt64, math.MinInt64, 1 << 32, -(1 << 32), 123456789012345} {
		check(n)
	}
	// A wide sweep, so a difference in a high byte cannot hide.
	for i := int64(0); i < 200000; i++ {
		check(i * 7919)
	}
}

func BenchmarkHash64(b *testing.B) {
	sink := int64(0)
	for i := 0; i < b.N; i++ {
		sink += Hash64(int64(i))
	}
	if sink == 0 {
		b.Fatal("sink")
	}
}
