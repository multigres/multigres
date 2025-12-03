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

package connpool

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/atomic2"
)

// ============================================================================
// Stack Interface and Test Item
// ============================================================================

// BenchStack is the interface for stack implementations to benchmark
type BenchStack interface {
	Push(item *benchItem)
	Pop() (*benchItem, bool)
}

// benchItem is a simple item for benchmarking stacks
type benchItem struct {
	next  atomic.Pointer[benchItem]
	value int
}

// ============================================================================
// Lock-Free Stack Implementation (using atomic2.PointerAndUint64)
// ============================================================================

type lockFreeStack struct {
	top atomic2.PointerAndUint64[benchItem]
}

func newLockFreeStack() *lockFreeStack {
	return &lockFreeStack{}
}

func (s *lockFreeStack) Push(item *benchItem) {
	for {
		oldHead, popCount := s.top.Load()
		item.next.Store(oldHead)
		if s.top.CompareAndSwap(oldHead, popCount, item, popCount) {
			return
		}
		runtime.Gosched()
	}
}

func (s *lockFreeStack) Pop() (*benchItem, bool) {
	for {
		oldHead, popCount := s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		newHead := oldHead.next.Load()
		if s.top.CompareAndSwap(oldHead, popCount, newHead, popCount+1) {
			oldHead.next.Store(nil)
			return oldHead, true
		}
		runtime.Gosched()
	}
}

// ============================================================================
// Mutex-Based Stack Implementation
// ============================================================================

type mutexStack struct {
	mu   sync.Mutex
	top  *benchItem
	size int
}

func newMutexStack() *mutexStack {
	return &mutexStack{}
}

func (s *mutexStack) Push(item *benchItem) {
	s.mu.Lock()
	item.next.Store(s.top)
	s.top = item
	s.size++
	s.mu.Unlock()
}

func (s *mutexStack) Pop() (*benchItem, bool) {
	s.mu.Lock()
	if s.top == nil {
		s.mu.Unlock()
		return nil, false
	}
	item := s.top
	s.top = item.next.Load()
	s.size--
	s.mu.Unlock()
	item.next.Store(nil)
	return item, true
}

// ============================================================================
// RWMutex-Based Stack Implementation
// ============================================================================

type rwMutexStack struct {
	mu   sync.RWMutex
	top  *benchItem
	size int
}

func newRWMutexStack() *rwMutexStack {
	return &rwMutexStack{}
}

func (s *rwMutexStack) Push(item *benchItem) {
	s.mu.Lock()
	item.next.Store(s.top)
	s.top = item
	s.size++
	s.mu.Unlock()
}

func (s *rwMutexStack) Pop() (*benchItem, bool) {
	s.mu.Lock()
	if s.top == nil {
		s.mu.Unlock()
		return nil, false
	}
	item := s.top
	s.top = item.next.Load()
	s.size--
	s.mu.Unlock()
	item.next.Store(nil)
	return item, true
}

// ============================================================================
// Benchmark Helpers
// ============================================================================

// preFillStack fills the stack with n items for Pop benchmarks
func preFillStack(s BenchStack, n int) []*benchItem {
	items := make([]*benchItem, n)
	for i := 0; i < n; i++ {
		items[i] = &benchItem{value: i}
		s.Push(items[i])
	}
	return items
}

// ============================================================================
// Single-Core Benchmarks (GOMAXPROCS=1)
// ============================================================================

func BenchmarkStackSingleCore(b *testing.B) {
	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
		{"RWMutex", func() BenchStack { return newRWMutexStack() }},
	}

	// Save current GOMAXPROCS and set to 1
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)

	for _, impl := range implementations {
		b.Run(impl.name+"/Push", func(b *testing.B) {
			stack := impl.factory()
			items := make([]*benchItem, b.N)
			for i := range items {
				items[i] = &benchItem{value: i}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stack.Push(items[i])
			}
		})

		b.Run(impl.name+"/Pop", func(b *testing.B) {
			stack := impl.factory()
			preFillStack(stack, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stack.Pop()
			}
		})

		b.Run(impl.name+"/PushPop", func(b *testing.B) {
			stack := impl.factory()
			item := &benchItem{value: 1}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stack.Push(item)
				stack.Pop()
			}
		})
	}
}

// ============================================================================
// Multi-Core Benchmarks (default GOMAXPROCS)
// ============================================================================

func BenchmarkStackMultiCore(b *testing.B) {
	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
		{"RWMutex", func() BenchStack { return newRWMutexStack() }},
	}

	goroutineCounts := []int{2, 4, 8, 16, 32}

	for _, impl := range implementations {
		for _, numGoroutines := range goroutineCounts {
			// Parallel Push benchmark
			b.Run(fmt.Sprintf("%s/Push/G%d", impl.name, numGoroutines), func(b *testing.B) {
				stack := impl.factory()
				itemPool := sync.Pool{
					New: func() any { return &benchItem{} },
				}

				b.SetParallelism(numGoroutines)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						item := itemPool.Get().(*benchItem)
						stack.Push(item)
					}
				})
			})

			// Parallel Pop benchmark (with continuous refill)
			b.Run(fmt.Sprintf("%s/Pop/G%d", impl.name, numGoroutines), func(b *testing.B) {
				stack := impl.factory()

				// Pre-fill and keep refilling in background
				stop := make(chan struct{})
				var pushCount atomic.Int64

				// Start producer goroutines to keep the stack filled
				for i := 0; i < numGoroutines/2+1; i++ {
					go func() {
						for {
							select {
							case <-stop:
								return
							default:
								stack.Push(&benchItem{})
								pushCount.Add(1)
							}
						}
					}()
				}

				// Wait for stack to fill up a bit
				for pushCount.Load() < 1000 {
					runtime.Gosched()
				}

				b.SetParallelism(numGoroutines)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for {
							if _, ok := stack.Pop(); ok {
								break
							}
							runtime.Gosched()
						}
					}
				})
				b.StopTimer()
				close(stop)
			})

			// Parallel mixed Push/Pop benchmark (50/50)
			b.Run(fmt.Sprintf("%s/Mixed/G%d", impl.name, numGoroutines), func(b *testing.B) {
				stack := impl.factory()
				// Pre-fill to avoid empty stack
				preFillStack(stack, 1000)

				var ops atomic.Int64

				b.SetParallelism(numGoroutines)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					localOps := int64(0)
					for pb.Next() {
						if localOps%2 == 0 {
							stack.Push(&benchItem{value: int(localOps)})
						} else {
							stack.Pop()
						}
						localOps++
					}
					ops.Add(localOps)
				})
			})
		}
	}
}

// ============================================================================
// Contention Benchmarks - High contention scenarios
// ============================================================================

func BenchmarkStackContention(b *testing.B) {
	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
	}

	// High contention: many goroutines, small work between operations
	for _, impl := range implementations {
		b.Run(impl.name+"/HighContention", func(b *testing.B) {
			stack := impl.factory()
			preFillStack(stack, 1000)

			numGoroutines := runtime.NumCPU() * 4

			b.SetParallelism(numGoroutines)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%2 == 0 {
						stack.Push(&benchItem{value: i})
					} else {
						stack.Pop()
					}
					i++
				}
			})
		})
	}
}

// ============================================================================
// Throughput Test - Measure ops/second over a fixed duration
// ============================================================================

func TestStackThroughput(t *testing.T) {
	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
	}

	goroutineCounts := []int{1, 2, 4, 8, 16}
	opsPerGoroutine := 100000

	t.Log("=== Stack Throughput Comparison ===")
	t.Log("")

	for _, impl := range implementations {
		t.Logf("--- %s Stack ---", impl.name)

		for _, numG := range goroutineCounts {
			stack := impl.factory()
			preFillStack(stack, 10000) // Pre-fill to avoid empty pops

			var totalOps atomic.Int64
			var wg sync.WaitGroup

			start := runtime.NumGoroutine()
			startTime := testing.AllocsPerRun(1, func() {})
			_ = startTime

			wg.Add(numG)
			actualStart := make(chan struct{})

			for i := 0; i < numG; i++ {
				go func(id int) {
					defer wg.Done()
					<-actualStart

					localOps := 0
					for j := 0; j < opsPerGoroutine; j++ {
						if j%2 == 0 {
							stack.Push(&benchItem{value: j})
						} else {
							stack.Pop()
						}
						localOps++
					}
					totalOps.Add(int64(localOps))
				}(i)
			}

			// Start all goroutines at once
			startMeasure := nanotime()
			close(actualStart)
			wg.Wait()
			elapsed := nanotime() - startMeasure

			opsPerSec := float64(totalOps.Load()) / (float64(elapsed) / 1e9)
			t.Logf("  Goroutines: %2d | Ops: %8d | Time: %8.2fms | Throughput: %12.0f ops/sec",
				numG, totalOps.Load(), float64(elapsed)/1e6, opsPerSec)

			_ = start
		}
		t.Log("")
	}
}

// nanotime returns the current time in nanoseconds
func nanotime() int64 {
	return time.Now().UnixNano()
}

// ============================================================================
// Fixed Throughput Latency Test - Measure latency at target ops/sec
// ============================================================================

// TestStackLatencyAtThroughput measures operation latency when targeting a fixed throughput.
// This uses a batched approach: do ops as fast as possible, then sleep to hit the target rate.
//
// The test answers: "If we need to sustain X ops/sec, what latency can we expect per operation?"
func TestStackLatencyAtThroughput(t *testing.T) {
	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
	}

	// Target: 1M ops/sec total across all goroutines
	targetOpsPerSec := 1_000_000
	testDuration := 500 * time.Millisecond
	goroutineCounts := []int{4, 8, 16, 32}
	// Sample every N ops to reduce overhead (store ~10k samples per goroutine)
	sampleRate := 100

	t.Logf("=== Stack Latency at %d ops/sec (sampling 1/%d) ===", targetOpsPerSec, sampleRate)
	t.Log("")

	for _, impl := range implementations {
		t.Logf("--- %s Stack ---", impl.name)

		for _, numG := range goroutineCounts {
			stack := impl.factory()
			preFillStack(stack, 10000)

			// Each goroutine's share of throughput
			opsPerGoroutinePerSec := targetOpsPerSec / numG
			// Batch size: do this many ops, then check timing
			batchSize := opsPerGoroutinePerSec / 100 // 100 batches per second
			if batchSize < 10 {
				batchSize = 10
			}
			// Expected time per batch in nanoseconds
			expectedBatchTimeNs := int64(batchSize) * int64(1e9) / int64(opsPerGoroutinePerSec)

			var wg sync.WaitGroup
			allLatencies := make([][]int64, numG)
			actualOps := make([]int64, numG)

			wg.Add(numG)
			startBarrier := make(chan struct{})

			for g := 0; g < numG; g++ {
				allLatencies[g] = make([]int64, 0, 10000)

				go func(gID int) {
					defer wg.Done()
					<-startBarrier

					latencies := allLatencies[gID]
					testEnd := time.Now().Add(testDuration)
					ops := int64(0)

					for time.Now().Before(testEnd) {
						batchStart := nanotime()

						// Do a batch of operations
						for i := 0; i < batchSize && time.Now().Before(testEnd); i++ {
							// Sample latency periodically
							if int(ops)%sampleRate == 0 {
								opStart := nanotime()
								if ops%2 == 0 {
									stack.Push(&benchItem{value: int(ops)})
								} else {
									stack.Pop()
								}
								opEnd := nanotime()
								latencies = append(latencies, opEnd-opStart)
							} else {
								if ops%2 == 0 {
									stack.Push(&benchItem{value: int(ops)})
								} else {
									stack.Pop()
								}
							}
							ops++
						}

						// Throttle: if we finished the batch too fast, sleep
						batchElapsed := nanotime() - batchStart
						if batchElapsed < expectedBatchTimeNs {
							sleepTime := time.Duration(expectedBatchTimeNs - batchElapsed)
							time.Sleep(sleepTime)
						}
					}

					allLatencies[gID] = latencies
					actualOps[gID] = ops
				}(g)
			}

			// Start all goroutines simultaneously
			startTime := time.Now()
			close(startBarrier)
			wg.Wait()
			elapsed := time.Since(startTime)

			// Merge latencies
			var totalOps int64
			var totalLatencies []int64
			for g := 0; g < numG; g++ {
				totalOps += actualOps[g]
				totalLatencies = append(totalLatencies, allLatencies[g]...)
			}

			if len(totalLatencies) == 0 {
				t.Logf("  G=%2d | No samples collected", numG)
				continue
			}

			// Sort for percentile calculation (use slices.Sort from Go 1.21+)
			sortInt64Slice(totalLatencies)

			// Calculate percentiles
			p50 := percentileInt64(totalLatencies, 50)
			p90 := percentileInt64(totalLatencies, 90)
			p99 := percentileInt64(totalLatencies, 99)
			p999 := percentileInt64(totalLatencies, 99.9)
			maxLat := totalLatencies[len(totalLatencies)-1]

			actualThroughput := float64(totalOps) / elapsed.Seconds()
			targetAchieved := (actualThroughput / float64(targetOpsPerSec)) * 100

			// Convert to microseconds for readability
			t.Logf("  G=%2d | Actual: %7.0f kops/s (%5.1f%%) | p50: %5.1fµs | p90: %5.1fµs | p99: %6.1fµs | p99.9: %7.1fµs | max: %7.1fµs",
				numG, actualThroughput/1000, targetAchieved,
				float64(p50)/1000, float64(p90)/1000, float64(p99)/1000, float64(p999)/1000, float64(maxLat)/1000)
		}
		t.Log("")
	}
}

// sortInt64Slice sorts a slice of int64 in ascending order using insertion sort (fast for small slices)
// and quicksort for larger ones
func sortInt64Slice(a []int64) {
	n := len(a)
	if n < 2 {
		return
	}
	if n < 50 {
		// Insertion sort for small slices
		for i := 1; i < n; i++ {
			key := a[i]
			j := i - 1
			for j >= 0 && a[j] > key {
				a[j+1] = a[j]
				j--
			}
			a[j+1] = key
		}
		return
	}
	quickSortInt64(a, 0, n-1)
}

func quickSortInt64(a []int64, lo, hi int) {
	if hi-lo < 20 {
		// Insertion sort for small partitions
		for i := lo + 1; i <= hi; i++ {
			key := a[i]
			j := i - 1
			for j >= lo && a[j] > key {
				a[j+1] = a[j]
				j--
			}
			a[j+1] = key
		}
		return
	}
	p := partitionInt64(a, lo, hi)
	quickSortInt64(a, lo, p-1)
	quickSortInt64(a, p+1, hi)
}

func partitionInt64(a []int64, lo, hi int) int {
	// Median of three pivot selection
	mid := lo + (hi-lo)/2
	if a[mid] < a[lo] {
		a[lo], a[mid] = a[mid], a[lo]
	}
	if a[hi] < a[lo] {
		a[lo], a[hi] = a[hi], a[lo]
	}
	if a[mid] < a[hi] {
		a[mid], a[hi] = a[hi], a[mid]
	}
	pivot := a[hi]

	i := lo - 1
	for j := lo; j < hi; j++ {
		if a[j] <= pivot {
			i++
			a[i], a[j] = a[j], a[i]
		}
	}
	a[i+1], a[hi] = a[hi], a[i+1]
	return i + 1
}

// percentileInt64 returns the p-th percentile value from a sorted slice
func percentileInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p / 100.0)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// ============================================================================
// Scalability Test - How performance scales with goroutines
// ============================================================================

func TestStackScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scalability test in short mode")
	}

	implementations := []struct {
		name    string
		factory func() BenchStack
	}{
		{"LockFree", func() BenchStack { return newLockFreeStack() }},
		{"Mutex", func() BenchStack { return newMutexStack() }},
	}

	t.Log("=== Stack Scalability Test ===")
	t.Logf("CPU cores available: %d", runtime.NumCPU())
	t.Log("")

	// Test from 1 to 2x CPU cores
	maxGoroutines := runtime.NumCPU() * 2
	opsPerGoroutine := 50000

	for _, impl := range implementations {
		t.Logf("--- %s Stack ---", impl.name)

		var baselineThroughput float64

		for numG := 1; numG <= maxGoroutines; numG++ {
			stack := impl.factory()
			preFillStack(stack, 10000)

			var totalOps atomic.Int64
			var wg sync.WaitGroup

			wg.Add(numG)
			actualStart := make(chan struct{})

			for i := 0; i < numG; i++ {
				go func() {
					defer wg.Done()
					<-actualStart

					for j := 0; j < opsPerGoroutine; j++ {
						if j%2 == 0 {
							stack.Push(&benchItem{value: j})
						} else {
							stack.Pop()
						}
					}
					totalOps.Add(int64(opsPerGoroutine))
				}()
			}

			startMeasure := nanotime()
			close(actualStart)
			wg.Wait()
			elapsed := nanotime() - startMeasure

			throughput := float64(totalOps.Load()) / (float64(elapsed) / 1e9)

			if numG == 1 {
				baselineThroughput = throughput
			}

			scalingEfficiency := (throughput / baselineThroughput) / float64(numG) * 100

			t.Logf("  G=%2d | Throughput: %12.0f ops/sec | Scaling: %6.1f%%",
				numG, throughput, scalingEfficiency)
		}
		t.Log("")
	}
}
