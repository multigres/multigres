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

package bufpool

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPoolGetPut(t *testing.T) {
	pool := New(1024, 16384)

	// Test getting and putting a buffer.
	// Requesting 2048 should get exactly 2048 from the 2048 bucket.
	buf := pool.Get(2048)
	require.NotNil(t, buf, "Get returned nil")
	assert.Equal(t, 2048, len(*buf), "buffer length should be set to requested size")
	assert.Equal(t, 2048, cap(*buf), "buffer capacity should match bucket size (2048)")

	// Put it back.
	pool.Put(buf)

	// Get another buffer of the same size, should reuse the same bucket.
	buf2 := pool.Get(2048)
	require.NotNil(t, buf2, "Get returned nil on second allocation")
	assert.Equal(t, 2048, len(*buf2), "buffer length on second allocation")
	assert.Equal(t, 2048, cap(*buf2), "buffer capacity should match bucket size")
}

func TestPoolSizeRoundingUp(t *testing.T) {
	// Pool with buckets at: 1024, 2048, 4096, 8192, 16384
	pool := New(1024, 16384)

	// Request 1500 bytes. Should get buffer from 2048 bucket (next power-of-2 multiple).
	buf := pool.Get(1500)
	require.NotNil(t, buf, "Get returned nil")
	assert.Equal(t, 1500, len(*buf), "buffer length should be set to requested size")
	assert.Equal(t, 2048, cap(*buf), "buffer capacity should be from 2048 bucket")

	pool.Put(buf)

	// Request 1000 bytes. Should get buffer from 1024 bucket.
	buf2 := pool.Get(1000)
	require.NotNil(t, buf2, "Get returned nil")
	assert.Equal(t, 1000, len(*buf2), "buffer length should be set to requested size")
	assert.Equal(t, 1024, cap(*buf2), "buffer capacity should be from 1024 bucket")

	pool.Put(buf2)

	// Request 3000 bytes. Should get buffer from 4096 bucket.
	buf3 := pool.Get(3000)
	require.NotNil(t, buf3, "Get returned nil")
	assert.Equal(t, 3000, len(*buf3), "buffer length should be set to requested size")
	assert.Equal(t, 4096, cap(*buf3), "buffer capacity should be from 4096 bucket")

	pool.Put(buf3)
}

func TestPoolBucketBoundaries(t *testing.T) {
	// Pool with buckets at: 1024, 2048, 4096, 8192, 16384
	pool := New(1024, 16384)

	testCases := []struct {
		name           string
		requestSize    int
		expectedBucket int
	}{
		{"exact bucket 1024", 1024, 1024},
		{"just over 1024", 1025, 2048},
		{"exact bucket 2048", 2048, 2048},
		{"just over 2048", 2049, 4096},
		{"exact bucket 4096", 4096, 4096},
		{"just over 4096", 4097, 8192},
		{"exact bucket 8192", 8192, 8192},
		{"just over 8192", 8193, 16384},
		{"exact bucket 16384", 16384, 16384},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := pool.Get(tc.requestSize)
			require.NotNil(t, buf, "Get returned nil")
			assert.Equal(t, tc.requestSize, len(*buf), "buffer length")
			assert.Equal(t, tc.expectedBucket, cap(*buf), "buffer should come from correct bucket")
			pool.Put(buf)
		})
	}
}

func TestPoolConcurrency(t *testing.T) {
	pool := New(1024, 16384)

	const numGoroutines = 100
	const numAllocations = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := range numGoroutines {
		go func(goroutineID int) {
			defer wg.Done()
			for j := range numAllocations {
				// Get buffers of various sizes.
				size := 1024 + (j%10)*512
				buf := pool.Get(size)
				if !assert.NotNil(t, buf, "Get returned nil in goroutine %d iteration %d", goroutineID, j) {
					continue
				}
				assert.Equal(t, size, len(*buf), "buffer length in goroutine %d iteration %d", goroutineID, j)
				// Verify capacity is from a valid bucket.
				assert.GreaterOrEqual(t, cap(*buf), size, "capacity should be at least requested size")
				// Return immediately.
				pool.Put(buf)
			}
		}(i)
	}

	wg.Wait()
}

func TestPoolReuse(t *testing.T) {
	pool := New(1024, 16384)

	// Allocate, free, and reallocate to verify reuse.
	buf1 := pool.Get(2048)
	require.NotNil(t, buf1, "Get returned nil")
	ptr1 := &(*buf1)[0] // Pointer to first byte.

	pool.Put(buf1)

	buf2 := pool.Get(2048)
	require.NotNil(t, buf2, "Get returned nil")
	ptr2 := &(*buf2)[0]

	// Check if the same underlying buffer was reused.
	// Note: This is not guaranteed by sync.Pool, but highly likely for immediate reuse.
	if ptr1 != ptr2 {
		t.Log("Buffer was not reused (this is not necessarily an error, sync.Pool behavior)")
	} else {
		t.Log("Buffer was successfully reused from pool")
	}

	pool.Put(buf2)
}

func TestPoolPanic(t *testing.T) {
	assert.Panics(t, func() {
		// This should panic because maxSize < minSize.
		_ = New(16384, 1024)
	}, "expected panic for maxSize < minSize")
}

func TestPoolPutOversizedBuffer(t *testing.T) {
	pool := New(1024, 16384)

	// Create a buffer larger than maxSize.
	oversized := make([]byte, 32768)
	buf := &oversized

	// Putting oversized buffer should not panic, just discard it.
	assert.NotPanics(t, func() {
		pool.Put(buf)
	}, "putting oversized buffer should not panic")
}

func TestPoolPutUndersizedBuffer(t *testing.T) {
	pool := New(1024, 16384)

	// Create a buffer smaller than minSize.
	undersized := make([]byte, 512)
	buf := &undersized

	// Putting undersized buffer with capacity < minSize should not panic.
	assert.NotPanics(t, func() {
		pool.Put(buf)
	}, "putting undersized buffer should not panic")
}

func BenchmarkPoolGet(b *testing.B) {
	pool := New(1024, 16384)

	b.ResetTimer()
	for range b.N {
		buf := pool.Get(4096)
		pool.Put(buf)
	}
}

func BenchmarkPoolGetParallel(b *testing.B) {
	pool := New(1024, 16384)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(4096)
			pool.Put(buf)
		}
	})
}

func BenchmarkDirectAllocation(b *testing.B) {
	b.ResetTimer()
	for range b.N {
		buf := make([]byte, 4096)
		_ = buf
	}
}
