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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			gotRetryDone = true
			retryDone() // Must call inside goroutine so drain can complete.
		}
	}()

	// Give it time to enqueue.
	time.Sleep(50 * time.Millisecond)

	// Verify the entry is in the queue.
	buf.mu.Lock()
	assert.Len(t, buf.queue, 1)
	buf.mu.Unlock()

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

	// Enqueue 3 requests — buffer size is 2, so the first should be evicted.
	// Goroutines call retryDone immediately so the drain can proceed.
	for i := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
			errs[idx] = err
			if retryDone != nil {
				retryDone()
			}
		}(i)
		time.Sleep(20 * time.Millisecond) // Ensure ordering.
	}

	// Give time for all enqueues.
	time.Sleep(50 * time.Millisecond)

	// The first request should have been evicted.
	// The queue should have 2 entries.
	buf.mu.Lock()
	assert.Len(t, buf.queue, 2)
	buf.mu.Unlock()

	// Stop buffering to drain remaining.
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// First request should have MTB01 (buffer full).
	assert.True(t, mterrors.IsError(errs[0], mterrors.MTB01.ID))

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
	assert.True(t, mterrors.IsError(err, mterrors.MTB02.ID))
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	}()

	wg.Wait()

	// Max failover duration fires stopBuffering which drains entries.
	// The entry should be drained (not evicted) because stopBuffering
	// transitions to DRAINING and closes done channels.
	assert.NoError(t, waitErr)
}

func TestBufferContextCancellation(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var retryDone RetryDoneFunc
	var err error

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
	}()

	// Give it time to enqueue.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context.
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
	cfg := testConfig(t, func(c *Config) {
		c.MinTimeBetweenFailovers.Set(1 * time.Hour)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()

	// First failover: should buffer.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, waitErr := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, waitErr)
		if retryDone != nil {
			retryDone()
		}
	}()
	time.Sleep(50 * time.Millisecond)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// Wait for drain to complete.
	time.Sleep(50 * time.Millisecond)

	// Second failover: should be skipped (too soon).
	retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
	assert.NoError(t, err)
	assert.Nil(t, retryDone, "second failover should be skipped due to timing guard")
}

func TestBufferMultipleShards(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	var err1, err2 error

	// Buffer requests for two different shards.
	wg.Add(2)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		err1 = err
		if retryDone != nil {
			retryDone()
		}
	}()
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard2Key)
		err2 = err
		if retryDone != nil {
			retryDone()
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Queue should have 2 entries from different shards.
	buf.mu.Lock()
	assert.Len(t, buf.queue, 2)
	buf.mu.Unlock()

	// Stop buffering for shard1 only.
	buf.StopBuffering(shard1Key)
	time.Sleep(50 * time.Millisecond)

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
	}()

	time.Sleep(50 * time.Millisecond)

	buf.Shutdown()
	wg.Wait()

	assert.True(t, mterrors.IsError(err, mterrors.MTB03.ID))
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
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
			errs[idx] = err
			if retryDone != nil {
				retryDone()
			}
		}(i)
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	for i := range numRequests {
		assert.NoError(t, errs[i], "request %d", i)
	}
}

func TestBufferListenerOnPoolerChanged(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	listener := NewBufferListener(buf)

	ctx := context.Background()
	var wg sync.WaitGroup
	var waitErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	}()
	time.Sleep(50 * time.Millisecond)

	// Simulate PRIMARY pooler appearing.
	listener.OnPoolerChanged(&clustermetadatapb.MultiPooler{
		TableGroup: "tg1",
		Shard:      "shard1",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
	})

	wg.Wait()
	assert.NoError(t, waitErr)
}

func TestBufferListenerReplicaIgnored(t *testing.T) {
	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	listener := NewBufferListener(buf)

	ctx := context.Background()
	var wg sync.WaitGroup
	var waitErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	}()
	time.Sleep(50 * time.Millisecond)

	// REPLICA change should NOT stop buffering.
	listener.OnPoolerChanged(&clustermetadatapb.MultiPooler{
		TableGroup: "tg1",
		Shard:      "shard1",
		Type:       clustermetadatapb.PoolerType_REPLICA,
	})

	// Verify still buffering.
	time.Sleep(50 * time.Millisecond)
	buf.mu.Lock()
	assert.Len(t, buf.queue, 1)
	buf.mu.Unlock()

	// Now send PRIMARY to actually drain.
	listener.OnPoolerChanged(&clustermetadatapb.MultiPooler{
		TableGroup: "tg1",
		Shard:      "shard1",
		Type:       clustermetadatapb.PoolerType_PRIMARY,
	})

	wg.Wait()
	assert.NoError(t, waitErr)
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
	wg.Add(1)
	var firstRetryDone RetryDoneFunc
	go func() {
		defer wg.Done()
		var err error
		firstRetryDone, err = buf.WaitForFailoverEnd(ctx, shard1Key)
		require.NoError(t, err)
		// Deliberately don't call retryDone yet to keep drain busy.
	}()

	time.Sleep(50 * time.Millisecond)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	// While draining, a new request should skip buffering (state is DRAINING).
	retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
	assert.NoError(t, err)
	assert.Nil(t, retryDone, "should skip buffering while draining")

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		waitErr = err
		if retryDone != nil {
			retryDone()
		}
	}()

	// Give it time to enqueue.
	time.Sleep(50 * time.Millisecond)

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

	time.Sleep(50 * time.Millisecond)

	// Shutdown should evict all entries cleanly.
	buf.Shutdown()
	wg.Wait()

	buf.mu.Lock()
	assert.Empty(t, buf.queue)
	buf.mu.Unlock()
}
