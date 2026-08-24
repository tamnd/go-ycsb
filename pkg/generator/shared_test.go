package generator

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// A core holds one key chooser and every worker calls Next on it, so
// every generator that can be a key chooser has to survive that. Before
// the last value write came out of them, the race detector reported
// each of these on the second goroutine.
func TestSharedGeneratorsAreRaceFree(t *testing.T) {
	cases := map[string]func() ycsb.Generator{
		"zipfian":          func() ycsb.Generator { return NewZipfianWithRange(0, 1000000, ZipfianConstant) },
		"scrambledZipfian": func() ycsb.Generator { return NewScrambledZipfian(0, 1000000, ZipfianConstant) },
		"uniform":          func() ycsb.Generator { return NewUniform(0, 1000000) },
		"hotspot":          func() ycsb.Generator { return NewHotspot(0, 1000000, 0.2, 0.8) },
		"exponential":      func() ycsb.Generator { return NewExponential(90, 1000) },
		"skewedLatest":     func() ycsb.Generator { return NewSkewedLatest(NewCounter(1000000)) },
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			g := build()
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					r := rand.New(rand.NewSource(seed))
					for n := 0; n < 5000; n++ {
						g.Next(r)
					}
				}(int64(i + 1))
			}
			wg.Wait()
		})
	}
}

// The operation chooser is shared the same way.
func TestSharedDiscreteIsRaceFree(t *testing.T) {
	d := NewDiscrete()
	d.Add(0.5, 1)
	d.Add(0.3, 2)
	d.Add(0.2, 3)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for n := 0; n < 5000; n++ {
				d.Next(r)
			}
		}(int64(i + 1))
	}
	wg.Wait()
}

// Hoisting math.Pow(0.5, theta) out of the draw must not move a single
// value, so the same seed has to give the same sequence it gave before.
// The reference is the expression the draw used to evaluate inline.
func TestZipfianHoistedConstantMatches(t *testing.T) {
	z := NewZipfianWithRange(0, 1000000, ZipfianConstant)
	if want := math.Pow(0.5, z.theta); z.halfPowTheta != want {
		t.Fatalf("halfPowTheta: got %v want %v", z.halfPowTheta, want)
	}

	// And the draw itself, against a hand rolled copy of the old body.
	r := rand.New(rand.NewSource(42))
	ref := rand.New(rand.NewSource(42))
	for i := 0; i < 100000; i++ {
		got := z.Next(r)

		u := ref.Float64()
		uz := u * z.zetan
		var want int64
		switch {
		case uz < 1.0:
			want = z.base
		case uz < 1.0+math.Pow(0.5, z.theta):
			want = z.base + 1
		default:
			want = z.base + int64(float64(z.items)*math.Pow(z.eta*u-z.eta+1, z.alpha))
		}
		if got != want {
			t.Fatalf("draw %d: got %d want %d", i, got, want)
		}
	}
}
