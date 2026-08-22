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
	"bytes"
	"strings"
	"testing"
	"time"
)

// The probe reports something for the host it is on, and the line it
// writes says which of the two cases the host is in. There is no
// assertion on the number itself: it is 41 ns on macOS and 517.7 us on
// Windows, and both are correct answers about the machine.
func TestReportClockSaysWhichCase(t *testing.T) {
	step, moved := clockStep(100000)
	t.Logf("clock step %v, moved on %d of 100000", step, moved)

	var buf bytes.Buffer
	reportClock(&buf)
	line := buf.String()
	if !strings.HasPrefix(line, "clock: ") {
		t.Fatalf("no clock line, got %q", line)
	}
	warned := strings.Contains(line, "must not be quoted")
	if warned != (step >= clockUsable) {
		t.Fatalf("step %v against the %v threshold, but warned=%v: %q",
			step, clockUsable, warned, line)
	}
}

// A clock that never moves has no step to report, and the probe has to
// come back with something past the threshold rather than with the zero
// value, which would read as the finest clock possible.
func TestClockStepWithNoMovementIsCoarse(t *testing.T) {
	step, moved := clockStep(0)
	if moved != 0 {
		t.Fatalf("zero probes cannot see the clock move, got %d", moved)
	}
	if step < clockUsable {
		t.Fatalf("a clock that never moved reported %v, which is under the %v threshold",
			step, clockUsable)
	}
}

// The threshold has to sit above the operations this harness times or it
// never fires, and under a timer tick or it always does.
func TestClockThresholdIsBetweenTheTwoCases(t *testing.T) {
	if clockUsable <= time.Microsecond {
		t.Fatalf("threshold %v is at or under a microsecond, which is a point read", clockUsable)
	}
	if clockUsable >= 500*time.Microsecond {
		t.Fatalf("threshold %v is at or over a Windows timer tick, so it would never fire",
			clockUsable)
	}
}
