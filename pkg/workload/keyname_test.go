package workload

import (
	"fmt"
	"math"
	"testing"
)

// The key a run sends is part of what a benchmark means, so a faster way
// of building it is only allowed if it builds the same bytes. This holds
// buildKeyName against the fmt.Sprintf it replaced, over the padding
// widths a run might ask for and over the numbers an int64 key can be,
// including the two that overflow a negation. See tamnd/zu#645.
func TestBuildKeyNameMatchesSprintf(t *testing.T) {
	nums := []int64{
		0, 1, 9, 10, 99, 100, 12345, 999999999,
		math.MaxInt64, math.MaxInt64 - 1,
		-1, -9, -10, -12345, math.MinInt64 + 1, math.MinInt64,
	}
	prefixes := []string{"user", "", "a-very-long-prefix-indeed-"}
	for _, prefix := range prefixes {
		for _, padding := range []int64{0, 1, 2, 5, 20, 40} {
			c := &core{keyPrefix: prefix, zeroPadding: padding, orderedInserts: true}
			for _, n := range nums {
				want := fmt.Sprintf("%s%0[3]*[2]d", prefix, n, padding)
				got := c.buildKeyName(n)
				if got != want {
					t.Fatalf("prefix %q padding %d n %d: got %q want %q",
						prefix, padding, n, got, want)
				}
			}
		}
	}
}

// The hashed insert order goes through util.Hash64 first, so the same
// check has to cover the path a default run actually takes.
func TestBuildKeyNameHashedMatchesSprintf(t *testing.T) {
	c := &core{keyPrefix: "user", zeroPadding: 1, orderedInserts: false}
	slow := &core{keyPrefix: "user", zeroPadding: 1, orderedInserts: false}
	for n := int64(0); n < 20000; n++ {
		got := c.buildKeyName(n)
		want := slow.slowBuildKeyName(n)
		if got != want {
			t.Fatalf("n %d: got %q want %q", n, got, want)
		}
	}
}

func BenchmarkBuildKeyName(b *testing.B) {
	c := &core{keyPrefix: "user", zeroPadding: 1, orderedInserts: false}
	sink := 0
	for i := 0; i < b.N; i++ {
		sink += len(c.buildKeyName(int64(i)))
	}
	if sink == 0 {
		b.Fatal("sink")
	}
}

func BenchmarkBuildKeyNameSprintf(b *testing.B) {
	c := &core{keyPrefix: "user", zeroPadding: 1, orderedInserts: false}
	sink := 0
	for i := 0; i < b.N; i++ {
		sink += len(c.slowBuildKeyName(int64(i)))
	}
	if sink == 0 {
		b.Fatal("sink")
	}
}
