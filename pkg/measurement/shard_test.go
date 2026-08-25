package measurement

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/magiconair/properties"
)

// The latencies every case below records, chosen so the percentiles
// land on distinct values rather than all on the same bucket.
func samples(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i%997+1) * time.Microsecond
	}
	return out
}

func props(t *testing.T, kv map[string]string) *properties.Properties {
	t.Helper()
	p := properties.NewProperties()
	for k, v := range kv {
		if _, _, err := p.Set(k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	return p
}

// What the run used to do: one measurer, everything recorded into it.
func recordIntoOne(t *testing.T, p *properties.Properties, ops []string, lat []time.Duration) merger {
	t.Helper()
	m := &measurement{p: p}
	one := m.newMeasurer()
	start := time.Unix(1700000000, 0)
	for i, d := range lat {
		one.Measure(ops[i%len(ops)], start.Add(time.Duration(i)*time.Microsecond), d)
	}
	return one
}

// A merged set of shards has to say exactly what one measurer says.
//
// This is the whole claim of the change: the per worker measurers are
// an implementation detail of where the samples are parked, not of
// what the run reports.
func TestMergedShardsMatchOneMeasurer(t *testing.T) {
	const threads = 8
	p := props(t, map[string]string{"threadcount": fmt.Sprint(threads)})
	ops := []string{"READ", "UPDATE", "TOTAL"}
	lat := samples(20000)

	InitMeasure(p)
	EnableWarmUp(false)
	start := time.Unix(1700000000, 0)
	for i, d := range lat {
		ctx := InitThread(context.Background(), i%threads)
		Measure(ctx, ops[i%len(ops)], start.Add(time.Duration(i)*time.Microsecond), d)
	}
	got := globalMeasure.merged().(*histograms)

	want := recordIntoOne(t, p, ops, lat).(*histograms)

	if len(got.histograms) != len(want.histograms) {
		t.Fatalf("operations: got %d want %d", len(got.histograms), len(want.histograms))
	}
	for op, w := range want.histograms {
		g, ok := got.histograms[op]
		if !ok {
			t.Fatalf("%s missing from the merged set", op)
		}
		if g.hist.TotalCount() != w.hist.TotalCount() {
			t.Errorf("%s count: got %d want %d", op, g.hist.TotalCount(), w.hist.TotalCount())
		}
		for _, q := range []float64{50, 90, 95, 99, 99.9, 99.99} {
			if gv, wv := g.hist.ValueAtPercentile(q), w.hist.ValueAtPercentile(q); gv != wv {
				t.Errorf("%s p%v: got %d want %d", op, q, gv, wv)
			}
		}
		if g.hist.Min() != w.hist.Min() || g.hist.Max() != w.hist.Max() {
			t.Errorf("%s range: got [%d,%d] want [%d,%d]",
				op, g.hist.Min(), g.hist.Max(), w.hist.Min(), w.hist.Max())
		}
	}
}

// The raw output has to hold every row, once, from every worker.
func TestMergedCSVKeepsEveryRow(t *testing.T) {
	const threads = 4
	p := props(t, map[string]string{
		"threadcount":     fmt.Sprint(threads),
		"measurementtype": "raw",
	})
	InitMeasure(p)
	EnableWarmUp(false)
	start := time.Unix(1700000000, 0)
	const n = 4000
	for i := 0; i < n; i++ {
		ctx := InitThread(context.Background(), i%threads)
		Measure(ctx, "READ", start.Add(time.Duration(i)*time.Microsecond), time.Duration(i+1)*time.Microsecond)
	}
	var buf bytes.Buffer
	if err := globalMeasure.merged().Output(&buf); err != nil {
		t.Fatalf("output: %v", err)
	}
	// One header line, then one line an operation.
	lines := strings.Count(strings.TrimRight(buf.String(), "\n"), "\n") + 1
	if lines != n+1 {
		t.Fatalf("lines: got %d want %d", lines, n+1)
	}
	// And in start time order, which is what the sort in Output is for.
	var last int64 = -1
	for _, line := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")[1:] {
		var op string
		var us, lan int64
		if _, err := fmt.Sscanf(strings.ReplaceAll(line, ",", " "), "%s %d %d", &op, &us, &lan); err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		if us < last {
			t.Fatalf("out of order: %d after %d", us, last)
		}
		last = us
	}
}

// A summary every log interval reads the shards while the workers are
// writing to them, which is the reason the shards are still locked.
// Run it under the race detector and it either holds or it does not.
func TestSummaryIsSafeWhileWorkersMeasure(t *testing.T) {
	const threads = 8
	p := props(t, map[string]string{"threadcount": fmt.Sprint(threads)})
	InitMeasure(p)
	EnableWarmUp(false)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := InitThread(context.Background(), id)
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				Measure(ctx, "READ", time.Now(), time.Duration(n%100+1)*time.Microsecond)
			}
		}(i)
	}
	for i := 0; i < 20; i++ {
		globalMeasure.merged()
	}
	close(stop)
	wg.Wait()
}

// A thread id the property never accounted for still records, into the
// spare shard on the end, rather than panicking on a slice bound.
func TestUnknownThreadUsesTheSpareShard(t *testing.T) {
	p := props(t, map[string]string{"threadcount": "2"})
	InitMeasure(p)
	EnableWarmUp(false)
	for _, id := range []int{-1, 2, 99} {
		ctx := InitThread(context.Background(), id)
		Measure(ctx, "READ", time.Now(), time.Millisecond)
	}
	// And a context that never went through InitThread at all.
	Measure(context.Background(), "READ", time.Now(), time.Millisecond)

	got := globalMeasure.merged().(*histograms)
	if c := got.histograms["READ"].hist.TotalCount(); c != 4 {
		t.Fatalf("count: got %d want 4", c)
	}
}
