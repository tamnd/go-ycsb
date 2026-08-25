package measurement

import (
	"strings"
	"testing"
	"time"

	"github.com/magiconair/properties"
)

// record puts n samples of one microsecond under op, which is enough for
// errorNotes since it only ever reads the counts.
func record(h *histograms, op string, n int) {
	for i := 0; i < n; i++ {
		h.Measure(op, time.Now(), time.Microsecond)
	}
}

func TestErrorNotesTotalFailure(t *testing.T) {
	h := InitHistograms(properties.NewProperties())
	record(h, "INSERT", 13)
	record(h, "SCAN_ERROR", 187)

	notes := h.errorNotes()
	if len(notes) != 1 {
		t.Fatalf("want one note, got %d: %v", len(notes), notes)
	}
	// The distinction the note exists to draw. No SCAN line was recorded,
	// so the run has no scan result at all and saying so is the point.
	if !strings.Contains(notes[0], "every one of the 187 attempts failed") {
		t.Errorf("note does not say the run had no result: %q", notes[0])
	}
	if !strings.Contains(notes[0], "no SCAN result") {
		t.Errorf("note does not name the operation: %q", notes[0])
	}
}

func TestErrorNotesPartialFailure(t *testing.T) {
	h := InitHistograms(properties.NewProperties())
	record(h, "READ", 990)
	record(h, "READ_ERROR", 10)

	notes := h.errorNotes()
	if len(notes) != 1 {
		t.Fatalf("want one note, got %d: %v", len(notes), notes)
	}
	// A run with a one percent error rate is still a result, so the note
	// is a footnote and has to say what the READ line is over.
	if !strings.Contains(notes[0], "10 of 1000 attempts failed (1.00%)") {
		t.Errorf("rate is wrong: %q", notes[0])
	}
	if !strings.Contains(notes[0], "over the 990 that did not") {
		t.Errorf("note does not say what the line covers: %q", notes[0])
	}
}

func TestErrorNotesSilentWhenNothingFailed(t *testing.T) {
	h := InitHistograms(properties.NewProperties())
	record(h, "READ", 1000)
	record(h, "UPDATE", 500)

	if notes := h.errorNotes(); len(notes) != 0 {
		t.Errorf("a clean run should say nothing, got %v", notes)
	}
}

// Several failing operation types in one run come out in a fixed order,
// so a diff between two runs is a diff and not a reordering.
func TestErrorNotesSorted(t *testing.T) {
	h := InitHistograms(properties.NewProperties())
	record(h, "UPDATE_ERROR", 3)
	record(h, "INSERT_ERROR", 2)
	record(h, "SCAN_ERROR", 1)

	notes := h.errorNotes()
	if len(notes) != 3 {
		t.Fatalf("want three notes, got %d: %v", len(notes), notes)
	}
	want := []string{"# INSERT:", "# SCAN:", "# UPDATE:"}
	for i, w := range want {
		if !strings.HasPrefix(notes[i], w) {
			t.Errorf("note %d is %q, want it to start %q", i, notes[i], w)
		}
	}
}
