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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// setupBufferTelemetry installs an in-memory metric reader so tests can
// assert on the buffer's emitted metrics. It must be called BEFORE
// buffer.New() because the stats instruments capture the global meter
// provider at construction time.
func setupBufferTelemetry(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-buffer"))
	t.Cleanup(func() {
		_ = setup.Telemetry.ShutdownTelemetry(context.Background())
	})
	return setup.MetricReader
}

// readSumInt64 reads the single data point of an Int64 Sum (UpDownCounter)
// metric. Returns 0 if the instrument has not yet been observed.
func readSumInt64(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "%s should be Sum[int64], got %T", name, m.Data)
			if len(sum.DataPoints) == 0 {
				return 0
			}
			return sum.DataPoints[0].Value
		}
	}
	return 0
}

// readHistogramFloat64 reads the single data point of a Float64 histogram.
// Returns nil if the instrument has not yet been observed.
func readHistogramFloat64(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.HistogramDataPoint[float64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s should be Histogram[float64], got %T", name, m.Data)
			if len(h.DataPoints) == 0 {
				return nil
			}
			return &h.DataPoints[0]
		}
	}
	return nil
}

// TestQueueDepthMetricTracksDrain verifies the queue.depth gauge follows
// the buffer occupancy through a happy-path enqueue → drain cycle.
func TestQueueDepthMetricTracksDrain(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
			assert.NoError(t, err)
			if retryDone != nil {
				retryDone()
			}
		})
	}
	waitForQueueLen(t, buf, 3)

	assert.Equal(t, int64(3), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should equal the number of buffered requests")

	buf.StopBuffering(shard1Key)
	wg.Wait()
	sb := buf.buffers[commontypes.FormatShardKey(shard1Key)]
	sb.drainWg.Wait()

	assert.Equal(t, int64(0), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should return to 0 after drain completes")
}

// TestQueueDepthMetricOnEviction verifies that when an enqueue forces
// eviction of the oldest entry (slot transfer), the gauge stays at the cap
// rather than briefly dipping.
func TestQueueDepthMetricOnEviction(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t, func(c *Config) {
		c.Size.Set(2)
		c.Window.Set(30 * time.Second)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	// Sequentially enqueue 3; the first is evicted when the third arrives.
	var firstDone sync.WaitGroup
	firstDone.Add(1)
	wg.Go(func() {
		defer firstDone.Done()
		retryDone, _ := buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)
	wg.Go(func() {
		retryDone, _ := buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 2)
	wg.Go(func() {
		retryDone, _ := buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone != nil {
			retryDone()
		}
	})
	firstDone.Wait() // The eviction has happened by the time the first goroutine returns.

	assert.Equal(t, int64(2), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should stay at the cap after a slot transfer")

	buf.StopBuffering(shard1Key)
	wg.Wait()
	sb := buf.buffers[commontypes.FormatShardKey(shard1Key)]
	sb.drainWg.Wait()

	assert.Equal(t, int64(0), readSumInt64(t, reader, "multigateway.buffer.queue.depth"))
}

// TestQueueDepthMetricOnWindowTimeout verifies the gauge decrements when an
// entry is evicted by the window-timeout watcher.
func TestQueueDepthMetricOnWindowTimeout(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t, func(c *Config) {
		c.Window.Set(50 * time.Millisecond)
	})
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	retryDone, err := buf.WaitForFailoverEnd(context.Background(), shard1Key)
	assert.Error(t, err, "window should expire before any drain trigger")
	assert.Nil(t, retryDone)

	assert.Equal(t, int64(0), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should drop to 0 after window-timeout eviction")
}

// TestQueueDepthMetricOnContextCancel verifies the gauge decrements when a
// waiting request's context is canceled.
func TestQueueDepthMetricOnContextCancel(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = buf.WaitForFailoverEnd(ctx, shard1Key)
	})
	waitForQueueLen(t, buf, 1)

	cancel()
	wg.Wait()

	assert.Equal(t, int64(0), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should drop to 0 after a context-canceled removal")
}

// TestQueueDepthMetricOnShutdown verifies the gauge drops to 0 when
// Shutdown evicts queued entries.
func TestQueueDepthMetricOnShutdown(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())

	ctx := context.Background()
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			_, _ = buf.WaitForFailoverEnd(ctx, shard1Key)
		})
	}
	waitForQueueLen(t, buf, 3)
	assert.Equal(t, int64(3), readSumInt64(t, reader, "multigateway.buffer.queue.depth"))

	buf.Shutdown()
	wg.Wait()

	assert.Equal(t, int64(0), readSumInt64(t, reader, "multigateway.buffer.queue.depth"),
		"queue.depth should be 0 after Shutdown evicts all entries")
}

// TestFailoverDurationMetricRecords verifies the failover.duration histogram
// records one sample per failover, and the recorded value matches the clock
// delta between BUFFERING start and the drain trigger.
func TestFailoverDurationMetricRecords(t *testing.T) {
	reader := setupBufferTelemetry(t)

	var mu sync.Mutex
	fakeTime := time.Now()
	fakeClock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return fakeTime
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		fakeTime = fakeTime.Add(d)
	}

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger(), WithNowFunc(fakeClock))
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Go(func() {
		retryDone, err := buf.WaitForFailoverEnd(ctx, shard1Key)
		assert.NoError(t, err)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)

	advance(3 * time.Second)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.failover.duration")
	require.NotNil(t, hist, "failover.duration should have at least one data point")
	assert.Equal(t, uint64(1), hist.Count, "expected exactly one failover sample")
	assert.InDelta(t, 3.0, hist.Sum, 0.001, "recorded duration should match the simulated clock advance")
}

// TestFailoverDurationMetricBuckets verifies the bucket boundaries are the
// ones we configured, rather than the OTel default (which is tuned for
// milliseconds and would collapse seconds-scale data into 1-2 buckets).
func TestFailoverDurationMetricBuckets(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Go(func() {
		retryDone, _ := buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.failover.duration")
	require.NotNil(t, hist)
	assert.Equal(t,
		[]float64{0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120},
		hist.Bounds,
		"failover.duration must use seconds-scale buckets, not the OTel ms default")
}

// TestWaitDurationMetricBuckets verifies the wait.duration buckets too —
// this metric had been silently using the OTel ms-default buckets while
// recording in seconds, making its quantiles useless.
func TestWaitDurationMetricBuckets(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Go(func() {
		retryDone, _ := buf.WaitForFailoverEnd(ctx, shard1Key)
		if retryDone != nil {
			retryDone()
		}
	})
	waitForQueueLen(t, buf, 1)
	buf.StopBuffering(shard1Key)
	wg.Wait()

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.wait.duration")
	require.NotNil(t, hist)
	assert.Equal(t,
		[]float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30},
		hist.Bounds,
		"wait.duration must use seconds-scale buckets, not the OTel ms default")
}

// TestFailoverDurationBucketDistribution feeds the histogram explicit
// samples and verifies each sample lands in the bucket the operator would
// expect from the configured boundaries. Samples are recorded directly via
// the package-private recorder to keep the test independent of buffer
// state-machine timing.
func TestFailoverDurationBucketDistribution(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	// Bounds: 0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120 → 11 buckets.
	// Per OTel spec, sample <= Bound[i] lands in Bucket[i].
	samples := []float64{
		0.05, // (-inf, 0.1]
		0.3,  // (0.1, 0.5]
		1.5,  // (1, 2]
		7,    // (5, 10]
		25,   // (20, 30]
		150,  // (120, +inf)
	}
	var wantSum float64
	for _, s := range samples {
		buf.stats.recordFailoverDuration(ctx, "tg1/shard1", s)
		wantSum += s
	}

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.failover.duration")
	require.NotNil(t, hist)
	require.Equal(t, []float64{0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120}, hist.Bounds)

	assert.Equal(t,
		[]uint64{1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1},
		hist.BucketCounts,
		"samples should land in expected buckets")
	assert.Equal(t, uint64(len(samples)), hist.Count)
	assert.InDelta(t, wantSum, hist.Sum, 0.001)
}

// TestFailoverDurationBoundaryInclusivity verifies the OTel
// ExplicitBucketHistogram inclusivity rule: a sample equal to Bound[i]
// belongs to Bucket[i], not Bucket[i+1].
func TestFailoverDurationBoundaryInclusivity(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	// Record exact boundary values: 0.1, 0.5, 1, 2.
	// Expected: bucket 0, 1, 2, 3 each get one sample.
	for _, s := range []float64{0.1, 0.5, 1, 2} {
		buf.stats.recordFailoverDuration(ctx, "tg1/shard1", s)
	}

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.failover.duration")
	require.NotNil(t, hist)
	assert.Equal(t,
		[]uint64{1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0},
		hist.BucketCounts,
		"a sample equal to Bound[i] must land in Bucket[i]")
}

// TestWaitDurationBucketDistribution feeds the wait-duration histogram
// explicit samples and verifies bucket placement.
func TestWaitDurationBucketDistribution(t *testing.T) {
	reader := setupBufferTelemetry(t)

	cfg := testConfig(t)
	buf := New(context.Background(), cfg, testLogger())
	defer buf.Shutdown()

	ctx := context.Background()
	// Bounds: 0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30 → 10 buckets.
	samples := []float64{
		0.0005, // (-inf, 0.001]
		0.05,   // (0.01, 0.1]
		0.7,    // (0.5, 1]
		4,      // (2, 5]
		50,     // (30, +inf)
	}
	var wantSum float64
	for _, s := range samples {
		buf.stats.recordWaitDuration(ctx, s)
		wantSum += s
	}

	hist := readHistogramFloat64(t, reader, "multigateway.buffer.wait.duration")
	require.NotNil(t, hist)
	require.Equal(t, []float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30}, hist.Bounds)

	assert.Equal(t,
		[]uint64{1, 0, 1, 0, 1, 0, 1, 0, 0, 1},
		hist.BucketCounts)
	assert.Equal(t, uint64(len(samples)), hist.Count)
	assert.InDelta(t, wantSum, hist.Sum, 0.001)
}
