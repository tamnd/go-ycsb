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

package measurement

import (
	"fmt"
	"io"
	"time"
)

// How coarse the clock has to be before the percentile columns stop
// meaning anything.
//
// An engine this harness measures answers a point read in hundreds of
// nanoseconds to tens of microseconds. A clock that steps by more than
// this records almost all of those as zero and the rest as one whole
// step, so the median is zero and the tail is a multiple of the step,
// and neither is a property of the engine.
//
// Ten microseconds, because that is about the slowest point read any
// engine here produces on a warm store, so a clock finer than it can
// still tell two of them apart.
const clockUsable = 10 * time.Microsecond

// clockStep is the smallest non zero difference between two consecutive
// reads of the clock, over probes reads of it, along with how many of
// those reads saw the clock move at all.
//
// Consecutive reads and not a sleep, because what matters is whether
// the clock can separate two events that are a microsecond apart, and
// the only way to find that out is to ask it twice as fast as it can
// answer.
func clockStep(probes int) (step time.Duration, moved int) {
	step = time.Hour
	last := time.Now()
	for i := 0; i < probes; i++ {
		now := time.Now()
		if d := now.Sub(last); d > 0 {
			moved++
			if d < step {
				step = d
			}
		}
		last = now
	}
	if moved == 0 {
		// The clock did not move once in the whole probe. There is no
		// step to report, only a lower bound on it, and the caller only
		// uses this to decide whether it is too coarse, so anything
		// past the threshold does.
		return time.Hour, 0
	}
	return step, moved
}

// reportClock writes what the host's clock can resolve, and says plainly
// when it cannot resolve the operations about to be timed.
//
// This exists because a Windows run of this harness printed a median of
// zero microseconds for every engine and every workload, and that read
// as a measurement rather than as the absence of one. Go reads the time
// on Windows out of the shared user data page, which the system updates
// on the timer tick: measured at 517.7 microseconds on a quiet
// i9-13900K, against 41 nanoseconds on macOS. tamnd/zu#649.
//
// The rate columns are not affected. They are wall clock over a whole
// run of millions of operations, which is many thousands of ticks.
func reportClock(w io.Writer) {
	const probes = 100000
	step, moved := clockStep(probes)
	if step < clockUsable {
		fmt.Fprintf(w, "clock: %v resolution, moved on %d of %d reads\n", step, moved, probes)
		return
	}
	fmt.Fprintf(w,
		"clock: %v resolution, moved on %d of %d reads. That is coarser than the operations "+
			"this run is timing, so the percentile columns below are the clock and not the "+
			"engine and must not be quoted. The Takes, Count and OPS columns are wall clock "+
			"over the whole run and are unaffected. See tamnd/zu#649.\n",
		step, moved, probes)
}
