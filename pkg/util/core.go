// Copyright 2018 PingCAP, Inc.
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

package util

import (
	"fmt"
	"sort"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
)

// createFieldIndices is a helper function to create a field -> index mapping
// for the core workload
func createFieldIndices(p *properties.Properties) map[string]int64 {
	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	m := make(map[string]int64, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		field := fmt.Sprintf("field%d", i)
		m[field] = i
	}
	return m
}

// allFields is a helper function to create all fields
func allFields(p *properties.Properties) []string {
	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fields := make([]string, 0, fieldCount)
	for i := int64(0); i < fieldCount; i++ {
		field := fmt.Sprintf("field%d", i)
		fields = append(fields, field)
	}
	return fields
}

// RowCodec is a helper struct to encode and decode TiDB format row
type RowCodec struct {
	fieldIndices map[string]int64
	fields       []string
	// The field names by column id, which is the reverse of
	// fieldIndices and is what lets a decode name a column without a
	// lookup. The ids are dense from zero because createFieldIndices
	// hands them out that way.
	fieldNames []string
}

// NewRowCodec creates the RowCodec
func NewRowCodec(p *properties.Properties) *RowCodec {
	indices := createFieldIndices(p)
	names := make([]string, len(indices))
	for field, i := range indices {
		names[i] = field
	}
	return &RowCodec{
		fieldIndices: indices,
		fields:       allFields(p),
		fieldNames:   names,
	}
}

// Decode decodes the row and returns a field-value map
//
// One walk of the row rather than a walk into a map[int64][]byte and a
// copy of that map into this one. The intermediate map was two thirds
// of the allocations a decoded row cost, and a scan decodes fifty rows,
// which made it the largest cost in workload E for every engine that
// keeps rows in this encoding.
func (r *RowCodec) Decode(row []byte, fields []string) (map[string][]byte, error) {
	return r.DecodeInto(row, fields, nil)
}

// DecodeInto is Decode into a map the caller keeps and hands back.
//
// A decoded row costs a map allocation and one insertion a field, and
// on a read heavy workload the allocation is the larger half of that: a
// thirty two thread profile of zu2 through this harness put makemap at
// 16.4 percent of the run and the insertions at 5.8. A driver that
// keeps one map a session and passes it here pays the insertions only.
//
// `into` is cleared first, so what comes back holds this row and no
// part of the row before it. A driver that reuses a map this way is
// promising its caller that the map is good until the next call on the
// same connection, which is the same promise it already makes about a
// value that points into a reused buffer.
//
// A nil `into` allocates, which is what Decode does.
func (r *RowCodec) DecodeInto(row []byte, fields []string, into map[string][]byte) (map[string][]byte, error) {
	if len(fields) == 0 {
		fields = r.fields
	}

	res := into
	if res == nil {
		res = make(map[string][]byte, len(fields))
	} else {
		clear(res)
	}
	all := len(fields) == len(r.fields)
	err := EachColumn(row, func(id int64, value []byte) {
		if id < 0 || int(id) >= len(r.fieldNames) {
			return
		}
		field := r.fieldNames[id]
		if !all {
			wanted := false
			for _, f := range fields {
				if f == field {
					wanted = true
					break
				}
			}
			if !wanted {
				return
			}
		}
		res[field] = value
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Fields is the full field list in column order, which is what an empty
// fields slice means everywhere else here. A driver that has to name its
// columns rather than ask for all of them needs the list itself.
func (r *RowCodec) Fields() []string {
	return r.fields
}

// Positions maps a column id to where that column belongs in a fields
// slice, or to -1 when the fields slice does not ask for it.
//
// Built once for a connection and handed to DecodeEach on every row,
// which is what keeps DecodeEach free of both a map and any string work.
// Doing the same job inside the decode would mean either comparing field
// names a column a row, or assuming a full fields slice arrives in column
// order, and the second is true of this workload today and is not
// something a decode should quietly depend on.
//
// An empty fields slice means every field, the same as elsewhere here.
// `into` is grown or reused.
func (r *RowCodec) Positions(fields []string, into []int) []int {
	if len(fields) == 0 {
		fields = r.fields
	}
	if cap(into) < len(r.fieldNames) {
		into = make([]int, len(r.fieldNames))
	}
	into = into[:len(r.fieldNames)]
	for i := range into {
		into[i] = -1
	}
	for i, f := range fields {
		if id, ok := r.fieldIndices[f]; ok {
			into[id] = i
		}
	}
	return into
}

// DecodeEach is Decode with no map in it: the row is walked and each
// wanted column is dropped into the slot Positions gave it.
//
// `into` is the caller's, is sized to the fields slice the positions were
// built from, and comes back holding one entry a field with nil where the
// row carried no such column. The values point into `row` rather than
// into a copy of it, the same as DecodeInto.
//
// A pooled DecodeInto is 166 ns a row where this is 77, ten fields of a
// hundred bytes on an i9-13900K, and a scan decodes fifty rows inside the
// call the harness times. See tamnd/zu#750.
func (r *RowCodec) DecodeEach(row []byte, positions []int, into [][]byte) ([][]byte, error) {
	clear(into)
	err := EachColumn(row, func(id int64, value []byte) {
		if id < 0 || int(id) >= len(positions) {
			return
		}
		if p := positions[id]; p >= 0 && p < len(into) {
			into[p] = value
		}
	})
	if err != nil {
		return nil, err
	}
	return into, nil
}

// Encode encodes the values
func (r *RowCodec) Encode(buf []byte, values map[string][]byte) ([]byte, error) {
	cols := make([][]byte, 0, len(values))
	colIDs := make([]int64, 0, len(values))

	for k, v := range values {
		i := r.fieldIndices[k]
		cols = append(cols, v)
		colIDs = append(colIDs, i)
	}

	rowData, err := EncodeRow(cols, colIDs, buf)
	return rowData, err
}

// FieldPair is a pair to hold field + value.
type FieldPair struct {
	Field string
	Value []byte
}

// FieldPairs implements sort interface for []FieldPair
type FieldPairs []FieldPair

// Len implements sort interface Len
func (s FieldPairs) Len() int {
	return len(s)
}

// Len implements sort interface Swap
func (s FieldPairs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Len implements sort interface Less
func (s FieldPairs) Less(i, j int) bool {
	return s[i].Field < s[j].Field
}

// NewFieldPairs sorts the map by fields and return a sorted slice of FieldPair.
func NewFieldPairs(values map[string][]byte) FieldPairs {
	pairs := make(FieldPairs, 0, len(values))
	for field, value := range values {
		pairs = append(pairs, FieldPair{
			Field: field,
			Value: value,
		})
	}

	sort.Sort(pairs)
	return pairs
}
