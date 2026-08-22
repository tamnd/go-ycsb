package util

import (
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
