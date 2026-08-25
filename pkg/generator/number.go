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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package generator

// Number is a common generator.
//
// LastValue is not maintained, and the reason is that the generators
// embedding this one are shared by every worker in a run. A core holds
// one key chooser, one field chooser and one operation chooser, and all
// of the threads call Next on them. Writing one int64 from thirty two
// threads on every operation is a data race, which the race detector
// reports, and it is also the same cache line bouncing between every
// core in the machine for a value nothing reads: within this client
// only Counter, AcknowledgedCounter and Sequential have their Last
// asked for, and each of those keeps its own.
//
// Last still answers, because ycsb.Generator requires it and the
// interface says an unadvanced generator should return something
// reasonable. Zero is that.
type Number struct {
	LastValue int64
}

// SetLastValue sets the last value generated.
//
// A generator that one worker owns may call this. A generator that the
// workers share must not, see the type comment.
func (n *Number) SetLastValue(value int64) {
	n.LastValue = value
}

// Last implements the Generator Last interface.
func (n *Number) Last() int64 {
	return n.LastValue
}
