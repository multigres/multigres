// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryregistry

// floatRing is a fixed-capacity ring buffer of float64 samples. Writes
// overwrite the oldest entry once the buffer is full. Not goroutine-safe;
// callers (QueryStats) hold the trend mutex.
type floatRing struct {
	buf  []float64
	pos  int // next write index
	size int // number of valid entries (capped at len(buf))
}

func newFloatRing(capacity int) floatRing {
	if capacity <= 0 {
		return floatRing{}
	}
	return floatRing{buf: make([]float64, capacity)}
}

// push appends v as the newest sample.
func (r *floatRing) push(v float64) {
	if len(r.buf) == 0 {
		return
	}
	r.buf[r.pos] = v
	r.pos = (r.pos + 1) % len(r.buf)
	if r.size < len(r.buf) {
		r.size++
	}
}

// snapshot returns a fresh slice of the current samples in oldest-to-newest
// order. Returns nil if the buffer is empty.
func (r *floatRing) snapshot() []float64 {
	if r.size == 0 {
		return nil
	}
	out := make([]float64, r.size)
	if r.size < len(r.buf) {
		// Buffer hasn't wrapped yet — values live at indices [0, size).
		copy(out, r.buf[:r.size])
		return out
	}
	// Buffer has wrapped — oldest is at r.pos.
	copy(out, r.buf[r.pos:])
	copy(out[len(r.buf)-r.pos:], r.buf[:r.pos])
	return out
}

// trendBuffers groups the rolling sample buffers we track per fingerprint.
// All buffers share the same capacity (set at construction time).
type trendBuffers struct {
	callRate    floatRing // calls/s
	totalTime   floatRing // ms of duration time per second of wall clock
	p50Ms       floatRing // p50 latency in ms
	p99Ms       floatRing // p99 latency in ms
	rowsRate    floatRing // rows returned/s
	initialized bool
}

func newTrendBuffers(capacity int) trendBuffers {
	return trendBuffers{
		callRate:    newFloatRing(capacity),
		totalTime:   newFloatRing(capacity),
		p50Ms:       newFloatRing(capacity),
		p99Ms:       newFloatRing(capacity),
		rowsRate:    newFloatRing(capacity),
		initialized: true,
	}
}
