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

//go:build zu2 && zu2bench

// The cgo half of floor_test.go.
//
// go test refuses cgo in a _test.go file, so the three calls the
// decomposition needs below the adapter live here instead. Both files
// are behind the zu2bench tag, so an ordinary zu2 build of the harness
// does not carry them.

package zu2

/*
#include <stdlib.h>
#include "zu2.h"

// The shape of zu2_read with none of its work: six arguments, three of
// them written through. Timed beside the real call, it separates what
// cgo charges for a call of this shape from what the engine does inside
// it.
static zu2_status floor_nop(zu2_session *s, const uint8_t *key, size_t key_len,
                            const uint8_t **value, size_t *value_len, int *found) {
  (void)s;
  *value = key;
  *value_len = key_len;
  *found = 1;
  return ZU2_OK;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// floorCrossing makes one cgo call that does nothing but read a counter
// out of the database, which is the cost of the crossing on its own.
func floorCrossing(db *zu2DB) uint32 {
	return uint32(C.zu2_nodes(db.db))
}

// floorRead is the engine's read with the value left in the session's
// buffer: no copy into Go memory and no decoding. It returns the length
// so the caller can see the row was really there.
func floorRead(s *session, rk []byte) (int, error) {
	var val *C.uint8_t
	var valLen C.size_t
	var found C.int
	if st := C.zu2_read(s.s, ptr(rk), C.size_t(len(rk)), &val, &valLen, &found); st != C.ZU2_OK {
		return 0, fmt.Errorf("zu2: read failed with status %d", int(st))
	}
	if found == 0 {
		return 0, nil
	}
	return int(valLen), nil
}

// floorNop makes the same shaped call as floorRead against a C function
// that does nothing, so what it costs is the crossing and the six
// arguments and no engine at all.
func floorNop(s *session, rk []byte) int {
	var val *C.uint8_t
	var valLen C.size_t
	var found C.int
	C.floor_nop(s.s, ptr(rk), C.size_t(len(rk)), &val, &valLen, &found)
	return int(valLen)
}

// floorKeys is the same key stream and the same three out slots, in C
// memory rather than Go's.
//
// It exists to tell two costs apart. A cgo call that takes Go pointers
// makes the runtime check them, and the three out-parameters escape to
// the heap because their addresses reach C, which is two allocations an
// operation. A call with nothing of Go's in it pays neither. The keys
// are one block with a fixed stride so a read costs an index and no
// copy, which is what the Go side's rowKey scratch amounts to as well.
type floorKeys struct {
	base   unsafe.Pointer
	stride int
	lens   []int
	val    **C.uint8_t
	vlen   *C.size_t
	found  *C.int
}

const floorKeyStride = 64

func newFloorKeys(keys []string, table string) *floorKeys {
	f := &floorKeys{
		stride: floorKeyStride,
		base:   C.malloc(C.size_t(len(keys) * floorKeyStride)),
		lens:   make([]int, len(keys)),
	}
	block := unsafe.Slice((*byte)(f.base), len(keys)*floorKeyStride)
	for i, k := range keys {
		row := block[i*floorKeyStride : (i+1)*floorKeyStride]
		n := copy(row, table)
		row[n] = ':'
		n++
		n += copy(row[n:], k)
		f.lens[i] = n
	}
	// One allocation for the three out slots, laid out by hand rather
	// than as a struct, because what is being measured is the call and
	// not the shape of somebody's scratch.
	f.val = (**C.uint8_t)(C.malloc(C.size_t(unsafe.Sizeof(f.val))))
	f.vlen = (*C.size_t)(C.malloc(C.size_t(unsafe.Sizeof(*f.vlen))))
	f.found = (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(*f.found))))
	return f
}

func (f *floorKeys) free() {
	C.free(f.base)
	C.free(unsafe.Pointer(f.val))
	C.free(unsafe.Pointer(f.vlen))
	C.free(unsafe.Pointer(f.found))
}

// floorNopNoGoPointers is the nop call with nothing of Go's in it, so
// against floorNop it is what cgo charges for pointer arguments that
// point into the Go heap.
func floorNopNoGoPointers(s *session, f *floorKeys, i int) int {
	key := (*C.uint8_t)(unsafe.Add(f.base, i*f.stride))
	C.floor_nop(s.s, key, C.size_t(f.lens[i]), f.val, f.vlen, f.found)
	return int(*f.vlen)
}

// floorReadNoGoPointers reads the i-th key with no Go pointer anywhere
// in the call.
func floorReadNoGoPointers(s *session, f *floorKeys, i int) (int, error) {
	key := (*C.uint8_t)(unsafe.Add(f.base, i*f.stride))
	if st := C.zu2_read(s.s, key, C.size_t(f.lens[i]), f.val, f.vlen, f.found); st != C.ZU2_OK {
		return 0, fmt.Errorf("zu2: read failed with status %d", int(st))
	}
	if *f.found == 0 {
		return 0, nil
	}
	return int(*f.vlen), nil
}

// floorReadCopy is floorRead plus the copy the adapter has to make,
// because the session's buffer is valid only until the next call on it.
func floorReadCopy(s *session, rk []byte) ([]byte, error) {
	var val *C.uint8_t
	var valLen C.size_t
	var found C.int
	if st := C.zu2_read(s.s, ptr(rk), C.size_t(len(rk)), &val, &valLen, &found); st != C.ZU2_OK {
		return nil, fmt.Errorf("zu2: read failed with status %d", int(st))
	}
	if found == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(val), C.int(valLen)), nil
}
