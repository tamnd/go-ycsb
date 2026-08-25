package util

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
)

func TestFieldPair(t *testing.T) {
	m := map[string][]byte{
		"f2": []byte("b"),
		"f1": []byte("a"),
	}

	p := NewFieldPairs(m)

	check := FieldPairs{
		{"f1", []byte("a")},
		{"f2", []byte("b")},
	}

	if !reflect.DeepEqual(p, check) {
		t.Errorf("want %v, but got %v", check, p)
	}
}

// The one pass decode has to agree with what the two map version
// returned, both for a row read whole and for a single field, because
// every read and every scan in the benchmark goes through it.
func TestDecodeMatchesTheRowMap(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.FieldCount, "10")
	r := NewRowCodec(p)

	values := make(map[string][]byte, 10)
	for i := 0; i < 10; i++ {
		values[fmt.Sprintf("field%d", i)] = []byte(strings.Repeat(string(rune('a'+i)), 100))
	}
	row, err := r.Encode(nil, values)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got, err := r.Decode(row, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Errorf("a whole row came back as %v", got)
	}

	one, err := r.Decode(row, []string{"field7"})
	if err != nil {
		t.Fatalf("decode one: %v", err)
	}
	want := map[string][]byte{"field7": values["field7"]}
	if !reflect.DeepEqual(one, want) {
		t.Errorf("one field came back as %v", one)
	}

	// An empty row is what a key with no value decodes to, and it is
	// not an error.
	empty, err := r.Decode(nil, nil)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("an empty row came back as %v", empty)
	}
}

// A reused map has to decode to what a fresh one decodes to, and has to
// hold this row and nothing of the row before it.
func TestDecodeIntoMatchesDecode(t *testing.T) {
	p := properties.NewProperties()
	if _, _, err := p.Set("fieldcount", "10"); err != nil {
		t.Fatalf("set: %v", err)
	}
	c := NewRowCodec(p)

	rows := make([][]byte, 3)
	for i := range rows {
		values := map[string][]byte{}
		// The third row is short, so a stale entry from the second
		// would survive into it if the map were not cleared.
		n := 10
		if i == 2 {
			n = 3
		}
		for f := 0; f < n; f++ {
			values[fmt.Sprintf("field%d", f)] = []byte(fmt.Sprintf("row%d-value%d", i, f))
		}
		row, err := c.Encode(nil, values)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		rows[i] = row
	}

	reused := make(map[string][]byte, 16)
	for i, row := range rows {
		want, err := c.Decode(row, nil)
		if err != nil {
			t.Fatalf("decode row %d: %v", i, err)
		}
		got, err := c.DecodeInto(row, nil, reused)
		if err != nil {
			t.Fatalf("decode into row %d: %v", i, err)
		}
		if len(got) != len(want) {
			t.Fatalf("row %d: got %d fields want %d", i, len(got), len(want))
		}
		for k, v := range want {
			if !bytes.Equal(got[k], v) {
				t.Errorf("row %d field %s: got %q want %q", i, k, got[k], v)
			}
		}
	}
}

// The map free decode, against the map one. DecodeEach is only worth
// having if it answers exactly what DecodeInto answers, so the check is
// that they agree, over field sets that are shuffled, short, and asking
// for something that is not there.
func TestDecodeEachAgreesWithDecodeInto(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.FieldCount, "10")
	r := NewRowCodec(p)

	values := make(map[string][]byte, 10)
	for i := 0; i < 10; i++ {
		values[fmt.Sprintf("field%d", i)] = []byte(fmt.Sprintf("value-%d", i))
	}
	row, err := r.Encode(nil, values)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	cases := [][]string{
		nil,
		{"field0"},
		{"field9", "field0"},
		{"field3", "field1", "field7", "field0"},
		// Every field, in an order that is not column order. The fast
		// path a decode is tempted to take is "all the fields means the
		// columns land where they already are", and this is the case
		// that catches it.
		{"field9", "field8", "field7", "field6", "field5", "field4", "field3", "field2", "field1", "field0"},
		// A name no column carries. It takes a slot and the slot stays
		// nil, rather than shifting everything after it.
		{"field2", "nosuchfield", "field5"},
	}

	for _, fields := range cases {
		want, err := r.DecodeInto(row, fields, nil)
		if err != nil {
			t.Fatalf("%v: DecodeInto: %v", fields, err)
		}

		eff := fields
		if len(eff) == 0 {
			eff = r.Fields()
		}
		pos := r.Positions(fields, nil)
		got, err := r.DecodeEach(row, pos, make([][]byte, len(eff)))
		if err != nil {
			t.Fatalf("%v: DecodeEach: %v", fields, err)
		}

		if len(got) != len(eff) {
			t.Fatalf("%v: got %d slots, want %d", fields, len(got), len(eff))
		}
		found := 0
		for i, name := range eff {
			w, ok := want[name]
			if !ok {
				if got[i] != nil {
					t.Errorf("%v: slot %d (%s) holds %q, but the map path has no such field",
						fields, i, name, got[i])
				}
				continue
			}
			found++
			if !bytes.Equal(got[i], w) {
				t.Errorf("%v: slot %d (%s) is %q, want %q", fields, i, name, got[i], w)
			}
		}
		if found != len(want) {
			t.Errorf("%v: the map path found %d fields and the slots account for %d",
				fields, len(want), found)
		}
	}
}

// The slots are reused across rows, so a field missing from the second
// row has to come back nil rather than as whatever the first row left
// there.
func TestDecodeEachClearsBetweenRows(t *testing.T) {
	p := properties.NewProperties()
	p.Set(prop.FieldCount, "4")
	r := NewRowCodec(p)

	full, err := r.Encode(nil, map[string][]byte{
		"field0": []byte("a"), "field1": []byte("b"),
		"field2": []byte("c"), "field3": []byte("d"),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	partial, err := r.Encode(nil, map[string][]byte{"field0": []byte("z")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	pos := r.Positions(nil, nil)
	into := make([][]byte, 4)
	if _, err := r.DecodeEach(full, pos, into); err != nil {
		t.Fatalf("first row: %v", err)
	}
	got, err := r.DecodeEach(partial, pos, into)
	if err != nil {
		t.Fatalf("second row: %v", err)
	}
	if !bytes.Equal(got[0], []byte("z")) {
		t.Errorf("field0 is %q, want %q", got[0], "z")
	}
	for i := 1; i < 4; i++ {
		if got[i] != nil {
			t.Errorf("field%d is %q, and the second row has no such field", i, got[i])
		}
	}
}
