// Copyright 2025 Supabase, Inc.
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

// Package bufpool provides buffer pooling utilities for efficient memory management.
// This implementation is inspired by Vitess's bucketpool package.
package bufpool

import (
	"math/bits"
	"sync"
)

// sizedPool is a pool for buffers of a specific size.
type sizedPool struct {
	size int
	pool sync.Pool
}

// newSizedPool creates a new sized pool.
func newSizedPool(size int) *sizedPool {
	return &sizedPool{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
	}
}

// Pool is a collection of pools for buffers of different sizes.
// It provides efficient allocation and recycling of byte buffers.
//
// The pool maintains multiple buckets, each for buffers of a specific size.
// Bucket sizes increase exponentially (powers of 2), starting from minSize up to maxSize.
type Pool struct {
	minSize int
	maxSize int
	pools   []*sizedPool
}

// New creates a new buffer pool with buckets from minSize to maxSize.
// Bucket sizes increase by powers of 2: [minSize, minSize*2, minSize*4, ..., maxSize].
func New(minSize, maxSize int) *Pool {
	if maxSize < minSize {
		panic("maxSize must be >= minSize")
	}

	const multiplier = 2
	var pools []*sizedPool

	// Create pools for each power of 2 from minSize to maxSize.
	curSize := minSize
	for curSize < maxSize {
		pools = append(pools, newSizedPool(curSize))
		curSize *= multiplier
	}
	// Add final pool for maxSize.
	pools = append(pools, newSizedPool(maxSize))

	return &Pool{
		minSize: minSize,
		maxSize: maxSize,
		pools:   pools,
	}
}

// findPool finds the appropriate pool for the given size.
// Returns nil if size exceeds maxSize.
func (p *Pool) findPool(size int) *sizedPool {
	if size > p.maxSize {
		return nil
	}

	// Calculate the bucket index based on size.
	// We need to find the smallest bucket that can hold 'size' bytes.
	div, rem := bits.Div64(0, uint64(size), uint64(p.minSize))
	idx := bits.Len64(div)

	// If size is an exact power-of-2 multiple of minSize, adjust index.
	if rem == 0 && div != 0 && (div&(div-1)) == 0 {
		idx = idx - 1
	}

	// Ensure index is within bounds.
	if idx >= len(p.pools) {
		idx = len(p.pools) - 1
	}

	return p.pools[idx]
}

// Get returns a pointer to a byte slice with at least 'size' bytes.
// The slice's length is set to 'size', and capacity may be larger.
//
// If no pool bucket can accommodate the size, a new slice is allocated on the heap.
// The caller is responsible for returning the buffer using Put() when done.
func (p *Pool) Get(size int) *[]byte {
	sp := p.findPool(size)
	if sp == nil {
		// Size exceeds maxSize, allocate directly.
		buf := make([]byte, size)
		return &buf
	}

	buf := sp.pool.Get().(*[]byte)
	*buf = (*buf)[:size]
	return buf
}

// Put returns a buffer to the pool for reuse.
// The buffer's capacity determines which pool bucket it goes to.
//
// If the buffer's capacity doesn't match any pool bucket, it's discarded.
// After calling Put(), the caller should not use the buffer anymore.
func (p *Pool) Put(buf *[]byte) {
	sp := p.findPool(cap(*buf))
	if sp == nil {
		// Buffer too large or too small, discard it.
		return
	}

	// Reset length to capacity before returning to pool.
	*buf = (*buf)[:cap(*buf)]
	sp.pool.Put(buf)
}
