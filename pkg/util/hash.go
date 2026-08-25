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
	"encoding/binary"
	"hash/fnv"
)

// FNV-1a's 64 bit offset basis and prime, from the reference the
// hash/fnv package implements.
const (
	fnv64aOffset = 14695981039346656037
	fnv64aPrime  = 1099511628211
)

// Hash64 returns a fnv Hash of the integer.
//
// The eight bytes are hashed here rather than through hash/fnv, which
// allocates a hasher on the heap for every call. This is the default
// insert order, so it is called for every key of every operation of
// every run, and it was on the profile of a driver that does nothing at
// all (tamnd/zu#645). The bytes are the same bytes, big endian, and the
// digest is the same digest, which TestHash64MatchesFnv holds.
func Hash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(n))
	hash := uint64(fnv64aOffset)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= fnv64aPrime
	}
	result := int64(hash)
	if result < 0 {
		return -result
	}
	return result
}

// BytesHash64 returns the fnv hash of a bytes
func BytesHash64(b []byte) int64 {
	hash := fnv.New64a()
	hash.Write(b)
	return int64(hash.Sum64())
}

// StringHash64 returns the fnv hash of a string
func StringHash64(s string) int64 {
	hash := fnv.New64a()
	hash.Write(Slice(s))
	return int64(hash.Sum64())
}
