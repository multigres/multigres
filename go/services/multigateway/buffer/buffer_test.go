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

package buffer

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/tools/viperutil"
)

var (
	shard1Key = commontypes.ShardKey{TableGroup: "tg1", Shard: "shard1"}
	shard2Key = commontypes.ShardKey{TableGroup: "tg1", Shard: "shard2"}
)

func testConfig(t *testing.T, opts ...func(*Config)) *Config {
	t.Helper()
	reg := viperutil.NewRegistry()
	cfg := NewConfig(reg)

	// Set defaults suitable for testing.
	cfg.Enabled.Set(true)
	cfg.Window.Set(5 * time.Second)
	cfg.Size.Set(10)
	cfg.MaxFailoverDuration.Set(10 * time.Second)
	cfg.MinTimeBetweenFailovers.Set(0) // Disable timing guard in tests by default.
	cfg.DrainConcurrency.Set(1)

	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// waitForCondition polls cond at 1ms intervals with a 5s timeout.
func waitForCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// waitForQueueLen polls until the global queue has exactly want entries.
func waitForQueueLen(t *testing.T, b *Buffer, want int) {
	t.Helper()
	waitForCondition(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.queue) == want
	})
}

func TestBufferBasicBufferingAndDrain(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	var (
		gotRetryDone bool
		waitErr      error
		wg           sync.WaitGroup
	)

	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			gotRetryDone = true
			retryDone() // Must call inside goroutine so drain can complete.
		}
	})

	// Wait for the entry to be enqueued.
	waitForQueueLen(t, buf, 1)

	// Simulate new PRIMARY discovered — stop buffering.
	buf.StopBuffering(shard1Key)

	wg.Wait()
	require.NoError(t, waitErr)
	require.True(t, gotRetryDone)

	// Queue should be empty after drain.
	buf.mu.Lock()
	assert.Empty(t, buf.queue)
	buf.mu.Unlock()
}

func TestBufferDisabled(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.Enabled.Set(false)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	retryDone, err := buf.WaitForFailoverEnd(context.Background(), shard1Key)
	assert.NoError(t, err)
	assert.Nil(t, retryDone)
}

func TestBufferGlobalEviction(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.Size.Set(2)
		c.Window.Set(30 * time.Second) // Long window so timeout doesn't interfere.
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	errs := make([]error, 3)
	var wg sync.WaitGroup

	// Enqueue 3 requests sequentially — buffer size is 2, so the first
	// should be evicted when the third arrives.
	// Goroutines call retryDone immediately so the drain can proceed.
	var wg0 sync.WaitGroup
	wg0.Add(1)
	wg.Go(func() {
		defer wg0.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		errs[0] = err
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)

	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		errs[1] = err
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 2)

	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		errs[2] = err
		if retryDone != nil {
			retryDone()
		}
	})
	// Goroutine 0's eviction confirms goroutine 2 has enqueued.
	wg0.Wait()

	buf.mu.Lock()
	assert.Len(t, buf.queue, 2)
	buf.mu.Unlock()

	// Stop buffering to drain remaining.
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// First request should have MTB01 (buffer full).
	assert.True(t, mterrors.IsErrorCode(errs[0], mterrors.MTB01.ID))

	// Others should succeed.
	for i := 1; i < 3; i++ {
		assert.NoError(t, errs[i])
	}
}

func TestBufferWindowTimeout(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.Window.Set(100 * time.Millisecond)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	retryDone, err := buf.WaitForFailoverEnd(context.Background(), shard1Key)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTB02.ID))
	assert.Nil(t, retryDone)
}

func TestBufferMaxFailoverDuration(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.Window.Set(5 * time.Second)
		c.MaxFailoverDuration.Set(100 * time.Millisecond)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	var waitErr error

	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	})

	wg.Wait()

	// Max failover duration fires stopBuffering which drains entries.
	// The entry should be drained (not evicted) because stopBuffering
	// transitions to DRAINING and closes done channels.
	assert.NoError(t, waitErr)
}

// TestBufferStaleMaxDurationTimer verifies that a maxDurationTimer from a
// previous failover does not kill a subsequent failover's buffering.
// Regression test for the stale timer race.
func TestBufferStaleMaxDurationTimer(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.MaxFailoverDuration.Set(80 * time.Millisecond)
		c.MinTimeBetweenFailovers.Set(0)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	// Failover 1: start buffering, let maxDurationTimer fire to end it.
	var wg sync.WaitGroup
	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, err)
		if retryDone != nil {
			retryDone()
		}
	})

	// Wait for max duration to fire and drain.
	wg.Wait()
	sb := buf.buffers[shard1Key]
	sb.drainWg.Wait()

	// Failover 2: start buffering again immediately.
	var retryDone2 RetryDoneFunc
	var err2 error
	wg.Go(func() {
		retryDone2, err2 = buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone2 != nil {
			retryDone2()
		}
	})

	waitForQueueLen(t, buf, 1)

	// Verify shard is still BUFFERING — a stale timer from failover 1
	// should NOT have stopped it.
	sb.mu.Lock()
	assert.Equal(t, stateBuffering, sb.state,
		"shard should still be buffering; stale timer must not kill it")
	sb.mu.Unlock()

	// End failover 2 normally.
	buf.StopBuffering(shard1Key)
	wg.Wait()

	assert.NoError(t, err2)
}

func TestBufferContextCancellation(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var retryDone RetryDoneFunc
	var err error

	wg.Go(func() {
		retryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
	})

	waitForQueueLen(t, buf, 1)
	cancel()
	wg.Wait()

	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, retryDone)

	// Queue should be empty (entry removed on cancel).
	buf.mu.Lock()
	assert.Empty(t, buf.queue)
	buf.mu.Unlock()
}

func TestBufferTimingGuard(t *testing.T) {
	var mu sync.Mutex
	fakeTime := time.Now()
	fakeClock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeTime
	}
	advanceClock := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		fakeTime = fakeTime.Add(d)
	}

	cfg := testConfig(t, func(c *Config) {
		c.MinTimeBetweenFailovers.Set(1 * time.Hour)
	})
	buf := New(context.Background(), cfg, testLogger(), WithNowFunc(fakeClock))
	defer buf.Shutdown()

	ctx := context.Background()

	// First failover: should buffer.
	var wg sync.WaitGroup
	wg.Go(func() {
		retryDone, waitErr := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, waitErr)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	sb := buf.buffers[shard1Key]
	sb.drainWg.Wait()

	// Second failover: should be skipped (too soon).
	retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
	assert.NoError(t, err)
	assert.Nil(t, retryDone, "second failover should be skipped due to timing guard")

	// Advance clock past MinTimeBetweenFailovers.
	advanceClock(1*time.Hour + 1*time.Second)

	// Third failover: should buffer again (enough time elapsed).
	wg.Go(func() {
		retryDone, waitErr := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, waitErr)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()
}

func TestBufferMultipleShards(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	var err1, err2 error

	// Buffer requests for two different shards.
	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		err1 = err
		if retryDone != nil {
			retryDone()
		}
	})
	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard2Key)
		err2 = err
		if retryDone != nil {
			retryDone()
		}
	})

	waitForQueueLen(t, buf, 2)

	// Stop buffering for shard1 only.
	buf.StopBuffering(shard1Key)
	sb := buf.buffers[shard1Key]
	sb.drainWg.Wait()

	// shard2 should still be buffered.
	buf.mu.Lock()
	assert.Len(t, buf.queue, 1)
	assert.Equal(t, shard2Key, buf.queue[0].shardKey)
	buf.mu.Unlock()

	// Stop buffering for shard2.
	buf.StopBuffering(shard2Key)
	wg.Wait()

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestBufferShutdown(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())

	ctx := context.Background()
	var wg sync.WaitGroup
	var retryDone RetryDoneFunc
	var err error

	wg.Go(func() {
		retryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
	})

	waitForQueueLen(t, buf, 1)
	buf.Shutdown()
	wg.Wait()

	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTB03.ID))
	assert.Nil(t, retryDone)
}

func TestBufferDrainConcurrency(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.DrainConcurrency.Set(3)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	const numRequests = 5
	var wg sync.WaitGroup
	errs := make([]error, numRequests)

	for i := range numRequests {
		wg.Go(func() {
			retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
			errs[i] = err
			if retryDone != nil {
				retryDone()
			}
		})
		waitForQueueLen(t, buf, i+1)
	}

	buf.StopBuffering(shard1Key)
	wg.Wait()

	for i := range numRequests {
		assert.NoError(t, errs[i], "request %d", i)
	}
}

// TestBufferDrainConcurrencyActuallyParallel verifies that DrainConcurrency > 1
// actually drains entries in parallel, not sequentially.
func TestBufferDrainConcurrencyActuallyParallel(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.DrainConcurrency.Set(3)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	const numRequests = 3

	// Each request will hold its retryDone until we signal it.
	// If drain is truly parallel, all 3 should be draining simultaneously.
	retryGate := make(chan struct{}) // Closed to let all retries complete.
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	var wg sync.WaitGroup
	for i := range numRequests {
		wg.Go(func() {
			retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
			if err != nil {
				return
			}
			// Track concurrency: increment on entry, wait for gate, decrement on exit.
			cur := concurrent.Add(1)
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			<-retryGate
			concurrent.Add(-1)
			retryDone()
		})
		waitForQueueLen(t, buf, i+1)
	}

	// Trigger drain — all 3 entries should start draining concurrently.
	buf.StopBuffering(shard1Key)

	// Wait for all drain goroutines to reach the gate.
	waitForCondition(t, func() bool {
		return concurrent.Load() == int32(numRequests)
	})

	// All 3 should be waiting at the gate concurrently.
	assert.Equal(t, int32(numRequests), concurrent.Load(),
		"all requests should be draining concurrently")

	// Release all retries.
	close(retryGate)
	wg.Wait()

	assert.Equal(t, int32(numRequests), maxConcurrent.Load(),
		"max concurrent drain should equal DrainConcurrency")
}

func TestBufferDrainingSkipsNewRequests(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.DrainConcurrency.Set(1)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	// Start a request that blocks the drain.
	var wg sync.WaitGroup
	var firstRetryDone RetryDoneFunc
	wg.Go(func() {
		var err error
		firstRetryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
		require.NoError(t, err)
		// Deliberately don't call retryDone yet to keep drain busy.
	})

	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// While draining, a new request should not be enqueued but should get
	// a RetryDoneFunc for immediate retry (new PRIMARY is available).
	retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
	assert.NoError(t, err)
	assert.NotNil(t, retryDone, "should get immediate retry while draining")
	retryDone()

	// Unblock the drain.
	if firstRetryDone != nil {
		firstRetryDone()
	}
}

// TestBufferContextCancelDuringDrain verifies that if a client's context is
// canceled after drain has extracted the entry from the queue but before/during
// drainEntry closes the done channel, the drain does not hang forever.
// This is a regression test for the drain hang bug.
func TestBufferContextCancelDuringDrain(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		c.DrainConcurrency.Set(1)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var waitErr error

	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	})

	waitForQueueLen(t, buf, 1)

	// Cancel the client context right before triggering drain.
	// This creates a race between ctx.Done() and e.done in waitOnEntry's select.
	cancel()

	// Trigger drain — must not hang even if waitOnEntry already exited via ctx.Done().
	buf.StopBuffering(shard1Key)

	// Wait for the shard buffer drain to complete. If the bug is present,
	// this will hang because drainEntry blocks on <-e.bufferCtx.Done() forever.
	sb := buf.buffers[shard1Key]
	sb.drainWg.Wait()

	wg.Wait()

	// The request either got canceled or drained — both are acceptable.
	if waitErr != nil {
		assert.ErrorIs(t, waitErr, context.Canceled)
	}
}

func TestBufferProactiveBuffering(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	// WaitIfAlreadyBuffering on an idle shard should return (nil, nil).
	retryDone, err := buf.WaitIfAlreadyBuffering(ctx, shard1Key)
	assert.NoError(t, err)
	assert.Nil(t, retryDone, "idle shard should not proactively buffer")

	// WaitIfAlreadyBuffering on an unknown shard should return (nil, nil).
	retryDone, err = buf.WaitIfAlreadyBuffering(ctx, commontypes.ShardKey{TableGroup: "unknown", Shard: "unknown"})
	assert.NoError(t, err)
	assert.Nil(t, retryDone, "unknown shard should not proactively buffer")

	// Trigger IDLE→BUFFERING via a reactive request.
	var wg sync.WaitGroup
	wg.Go(func() {
		rd, waitErr := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, waitErr)
		if rd != nil {
			rd()
		}
	})
	waitForQueueLen(t, buf, 1)

	// Now WaitIfAlreadyBuffering should join the existing buffer.
	var proactiveRetryDone RetryDoneFunc
	var proactiveErr error
	wg.Go(func() {
		proactiveRetryDone, proactiveErr = buf.WaitIfAlreadyBuffering(ctx, shard1Key)
		if proactiveRetryDone != nil {
			proactiveRetryDone()
		}
	})
	waitForQueueLen(t, buf, 2)

	// Stop buffering — both entries should drain.
	buf.StopBuffering(shard1Key)
	wg.Wait()

	assert.NoError(t, proactiveErr)
}

func TestBufferProactiveBufferingDraining(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	// Start buffering and trigger drain (but hold retryDone to stay in DRAINING).
	var wg sync.WaitGroup
	var firstRetryDone RetryDoneFunc
	wg.Go(func() {
		var err error
		firstRetryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
		require.NoError(t, err)
	})

	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// Shard is DRAINING. WaitIfAlreadyBuffering should return immediate retry.
	retryDone, err := buf.WaitIfAlreadyBuffering(ctx, shard1Key)
	assert.NoError(t, err)
	assert.NotNil(t, retryDone, "should get immediate retry while draining")
	retryDone()

	// Unblock the drain.
	if firstRetryDone != nil {
		firstRetryDone()
	}
}

func TestBufferShutdownAfterEnqueue(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())

	ctx := context.Background()
	var wg sync.WaitGroup

	// Enqueue several entries.
	for range 5 {
		wg.Go(func() {
			_, _ = buf.WaitForFailoverEnd(ctx, shard1Key)
		})
	}

	waitForQueueLen(t, buf, 5)

	// Shutdown should evict all entries cleanly.
	buf.Shutdown()
	wg.Wait()

	buf.mu.Lock()
	assert.Empty(t, buf.queue)
	buf.mu.Unlock()
}
