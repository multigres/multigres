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

// Package queryregistry tracks per-query-shape statistics using a W-TinyLFU
// cache (via theine) keyed by query fingerprint. The TinyLFU doorkeeper
// ensures one-off queries don't evict popular ones, and its frequency
// comparison naturally promotes newly-popular queries.
//
// The registry is used to:
//   - Bound Prometheus label cardinality by deciding which fingerprints get
//     their own label value vs. fall into the "__other__" bucket.
//   - Back a /debug/queries endpoint that exposes aggregated per-query stats
//     (a pg_stat_statements equivalent).
package queryregistry

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/cache/theine"
)

// OtherLabel is the fingerprint label value used for queries not in the
// tracked top set. Keeping this bucket named lets us still emit metrics for
// the long tail without paying cardinality cost per fingerprint.
const OtherLabel = "__other__"

// UtilityLabel is used for statements we don't fingerprint individually
// (BEGIN/COMMIT/SET/DDL etc.) — their space is tiny and bounded.
const UtilityLabel = "__utility__"

// QueryStats holds aggregated statistics for a single query fingerprint.
// Counter fields use atomics so the hot Record path stays lockless. Trend
// ring buffers and the per-sample bookkeeping fields are protected by
// trendMu — only the sampler goroutine and Snapshot readers touch them.
type QueryStats struct {
	fingerprint   string
	normalizedSQL string

	calls           atomic.Uint64
	errors          atomic.Uint64
	totalDurationNs atomic.Int64
	totalRows       atomic.Uint64
	minDurationNs   atomic.Int64
	maxDurationNs   atomic.Int64
	lastSeenUnixNs  atomic.Int64

	// Per-fingerprint duration histogram. Bucket layout matches
	// durationBucketsNs; index numHistBuckets-1 is the +Inf overflow.
	durationBuckets [numHistBuckets]atomic.Uint64

	// Trend bookkeeping — guarded by trendMu. Last-* fields hold the
	// counter / histogram values at the previous sample so the sampler can
	// compute per-interval deltas. Without this delta-of-histograms the
	// trend percentile would be cumulative-since-admission and would
	// flat-line under sustained load.
	trendMu                   sync.Mutex
	trends                    trendBuffers
	lastSampleCalls           uint64
	lastSampleTotalDurationNs int64
	lastSampleTotalRows       uint64
	lastSampleBuckets         [numHistBuckets]uint64
}

// CachedSize implements theine's cacheval interface so theine can bound
// the registry by bytes, not entries. The fixed overhead covers the struct
// + atomics + histogram buckets; the variable part covers the per-instance
// strings and the trend ring buffers (5 rings × cap × 8 bytes/float64),
// charged eagerly at admit time so theine's admission policy isn't fooled
// into over-admitting once the sampler warms up.
func (s *QueryStats) CachedSize(_ bool) int64 {
	base := int64(384) + int64(len(s.fingerprint)) + int64(len(s.normalizedSQL))
	if s.trends.initialized {
		base += int64(5) * int64(len(s.trends.callRate.buf)) * 8
	}
	return base
}

// Snapshot is a point-in-time copy of a QueryStats, safe to hand out to
// HTTP handlers or serialize to JSON. Trend slices are oldest-to-newest
// rolling samples captured by the registry's sampler goroutine — empty
// when sampling is disabled or no samples have been taken yet.
type Snapshot struct {
	Fingerprint     string        `json:"fingerprint"`
	NormalizedSQL   string        `json:"normalized_sql"`
	Calls           uint64        `json:"calls"`
	Errors          uint64        `json:"errors"`
	TotalDuration   time.Duration `json:"total_duration_ns"`
	AverageDuration time.Duration `json:"average_duration_ns"`
	MinDuration     time.Duration `json:"min_duration_ns"`
	MaxDuration     time.Duration `json:"max_duration_ns"`
	P50Duration     time.Duration `json:"p50_duration_ns"`
	P99Duration     time.Duration `json:"p99_duration_ns"`
	TotalRows       uint64        `json:"total_rows"`
	LastSeen        time.Time     `json:"last_seen"`

	// SampleIntervalSeconds is the cadence at which the trend slices
	// below were captured. 0 means trends are disabled.
	SampleIntervalSeconds float64 `json:"sample_interval_seconds"`

	// CallRateTrend is calls/s, oldest sample first.
	CallRateTrend []float64 `json:"call_rate_trend,omitempty"`
	// TotalTimeMsTrend is duration-ms-per-second of wall clock, oldest first.
	TotalTimeMsTrend []float64 `json:"total_time_ms_trend,omitempty"`
	// P50MsTrend is p50 latency in ms over time, oldest first.
	P50MsTrend []float64 `json:"p50_ms_trend,omitempty"`
	// P99MsTrend is p99 latency in ms over time, oldest first.
	P99MsTrend []float64 `json:"p99_ms_trend,omitempty"`
	// RowsRateTrend is rows/s, oldest first.
	RowsRateTrend []float64 `json:"rows_rate_trend,omitempty"`
}

// Registry tracks per-fingerprint query statistics.
// The zero value is not usable — construct via New.
type Registry struct {
	store      *theine.Store[theine.StringKey, *QueryStats]
	maxSQLLen  int
	newEntryMu sync.Mutex

	// Sampler config; trendCapacity == 0 disables trend collection.
	sampleInterval time.Duration
	trendCapacity  int

	// Sampler goroutine lifecycle.
	samplerCancel context.CancelFunc
	samplerDone   chan struct{}
}

// Config configures a Registry.
type Config struct {
	// MaxMemoryBytes caps the memory used by the tracked fingerprint set.
	// A value <= 0 disables the registry (all calls become no-ops).
	MaxMemoryBytes int
	// MaxSQLLength caps the stored representative normalized SQL for each
	// fingerprint. Queries longer than this are truncated in the stored
	// copy; the fingerprint itself is still computed over the full text.
	MaxSQLLength int
	// SampleInterval is the cadence at which the trend sampler captures
	// per-fingerprint deltas (calls/s, p50/p99 ms, etc.) into a rolling
	// ring. 0 disables sampling.
	SampleInterval time.Duration
	// TrendWindowSamples is the number of samples retained per metric per
	// fingerprint. The visible time window is SampleInterval * this value.
	// 0 disables sampling regardless of SampleInterval.
	TrendWindowSamples int
}

// DefaultConfig returns reasonable defaults for the registry.
func DefaultConfig() Config {
	return Config{
		MaxMemoryBytes:     2 * 1024 * 1024, // 2 MB
		MaxSQLLength:       4096,
		SampleInterval:     10 * time.Second,
		TrendWindowSamples: 60, // 60 × 10s = 10-minute trend window
	}
}

// New constructs a Registry with the given config.
// If cfg.MaxMemoryBytes <= 0 the registry is disabled and all methods become no-ops.
func New(cfg Config) *Registry {
	return newRegistry(cfg, true)
}

// NewForTest constructs a Registry without the TinyLFU doorkeeper so tests
// can assert deterministic admission behavior.
func NewForTest(cfg Config) *Registry {
	return newRegistry(cfg, false)
}

func newRegistry(cfg Config, doorkeeper bool) *Registry {
	r := &Registry{maxSQLLen: cfg.MaxSQLLength}
	if cfg.MaxMemoryBytes <= 0 {
		return r
	}
	r.store = theine.NewStore[theine.StringKey, *QueryStats](int64(cfg.MaxMemoryBytes), doorkeeper)
	if cfg.SampleInterval > 0 && cfg.TrendWindowSamples > 0 {
		r.sampleInterval = cfg.SampleInterval
		r.trendCapacity = cfg.TrendWindowSamples
		//nolint:gocritic // Long-lived sampler tied to the registry's lifetime, cancelled in Close().
		ctx, cancel := context.WithCancel(context.Background())
		r.samplerCancel = cancel
		r.samplerDone = make(chan struct{})
		go r.runSampler(ctx)
	}
	return r
}

// Record updates stats for the given fingerprint. If the fingerprint is not
// yet tracked, the TinyLFU admission policy decides whether to admit it —
// one-off queries won't enter the tracked set on their first hit.
func (r *Registry) Record(fingerprint, normalizedSQL string, duration time.Duration, rows int, hadError bool) {
	if r == nil || r.store == nil || fingerprint == "" {
		return
	}

	stats, ok := r.store.Get(theine.StringKey(fingerprint), 0)
	if !ok {
		// Try to admit — may fail if TinyLFU rejects a cold entry.
		stats = r.admit(fingerprint, normalizedSQL)
		if stats == nil {
			return
		}
	}

	durNs := duration.Nanoseconds()
	stats.calls.Add(1)
	stats.totalDurationNs.Add(durNs)
	stats.durationBuckets[bucketIndex(durNs)].Add(1)
	if rows > 0 {
		stats.totalRows.Add(uint64(rows))
	}
	if hadError {
		stats.errors.Add(1)
	}
	stats.lastSeenUnixNs.Store(time.Now().UnixNano())

	// Update min/max with CAS loops.
	for {
		cur := stats.minDurationNs.Load()
		if cur != 0 && durNs >= cur {
			break
		}
		if stats.minDurationNs.CompareAndSwap(cur, durNs) {
			break
		}
	}
	for {
		cur := stats.maxDurationNs.Load()
		if durNs <= cur {
			break
		}
		if stats.maxDurationNs.CompareAndSwap(cur, durNs) {
			break
		}
	}
}

// admit attempts to insert a new QueryStats entry for fingerprint.
// Returns the stats (either newly admitted or racing winner's), or nil if
// TinyLFU rejected the admission.
func (r *Registry) admit(fingerprint, normalizedSQL string) *QueryStats {
	r.newEntryMu.Lock()
	defer r.newEntryMu.Unlock()

	// Re-check after acquiring lock: another goroutine may have admitted it.
	if existing, ok := r.store.Get(theine.StringKey(fingerprint), 0); ok {
		return existing
	}

	truncated := normalizedSQL
	if r.maxSQLLen > 0 && len(truncated) > r.maxSQLLen {
		truncated = truncated[:r.maxSQLLen]
	}
	stats := &QueryStats{
		fingerprint:   fingerprint,
		normalizedSQL: truncated,
	}
	if r.trendCapacity > 0 {
		// Allocate up-front so CachedSize reflects the real footprint and
		// theine's admission policy can use accurate sizes.
		stats.trends = newTrendBuffers(r.trendCapacity)
	}
	if !r.store.Set(theine.StringKey(fingerprint), stats, 0, 0) {
		return nil
	}
	// Reload through Get so we return the authoritative stored pointer —
	// theine may have rejected via doorkeeper and returned false without
	// actually admitting.
	stored, ok := r.store.Get(theine.StringKey(fingerprint), 0)
	if !ok {
		return nil
	}
	return stored
}

// Labelize returns the fingerprint if it is currently tracked in the
// registry, otherwise returns OtherLabel. Use this as the value for
// a Prometheus/OTel label to bound cardinality.
func (r *Registry) Labelize(fingerprint string) string {
	if r == nil || r.store == nil || fingerprint == "" {
		return OtherLabel
	}
	if _, ok := r.store.Get(theine.StringKey(fingerprint), 0); ok {
		return fingerprint
	}
	return OtherLabel
}

// SortKey identifies how Top should sort results.
type SortKey string

const (
	SortByCalls       SortKey = "calls"
	SortByTotalTime   SortKey = "total_time"
	SortByAverageTime SortKey = "avg_time"
	SortByErrors      SortKey = "errors"
	SortByLastSeen    SortKey = "last_seen"
)

// Top returns up to `limit` snapshots of the currently-tracked query
// statistics, sorted by the given key in descending order. Passing
// limit <= 0 returns all tracked entries.
func (r *Registry) Top(limit int, sortBy SortKey) []Snapshot {
	if r == nil || r.store == nil {
		return nil
	}

	intervalSec := r.sampleInterval.Seconds()
	var snapshots []Snapshot
	r.store.Range(0, func(_ theine.StringKey, v *QueryStats) bool {
		snapshots = append(snapshots, v.snapshot(intervalSec))
		return true
	})

	sortSnapshots(snapshots, sortBy)
	if limit > 0 && len(snapshots) > limit {
		snapshots = snapshots[:limit]
	}
	return snapshots
}

// Len returns the number of fingerprints currently tracked in the registry.
func (r *Registry) Len() int {
	if r == nil || r.store == nil {
		return 0
	}
	return r.store.Len()
}

// Close stops the background maintenance goroutine.
// Safe to call on a nil or disabled registry.
func (r *Registry) Close() {
	if r == nil || r.store == nil {
		return
	}
	if r.samplerCancel != nil {
		r.samplerCancel()
		<-r.samplerDone
		r.samplerCancel = nil
	}
	r.store.Close()
}

// runSampler periodically captures a per-fingerprint sample (rates +
// histogram percentiles) into each entry's trend rings until ctx is done.
func (r *Registry) runSampler(ctx context.Context) {
	defer close(r.samplerDone)
	t := time.NewTicker(r.sampleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sampleAll()
		}
	}
}

// sampleAll walks the registry and records one trend sample per entry.
func (r *Registry) sampleAll() {
	if r == nil || r.store == nil {
		return
	}
	intervalSec := r.sampleInterval.Seconds()
	if intervalSec <= 0 {
		return
	}
	r.store.Range(0, func(_ theine.StringKey, v *QueryStats) bool {
		v.takeSample(intervalSec)
		return true
	})
}

// takeSample reads the current counters/histogram, computes deltas vs. the
// previous sample, and pushes one new value into each trend ring. The rings
// are pre-allocated by admit() when the registry has sampling enabled, so a
// nil-or-uninitialised state means the entry pre-dates the sampler config
// and trend pushes are skipped.
func (s *QueryStats) takeSample(intervalSec float64) {
	s.trendMu.Lock()
	defer s.trendMu.Unlock()

	if !s.trends.initialized {
		return
	}

	callsNow := s.calls.Load()
	totalDurNow := s.totalDurationNs.Load()
	rowsNow := s.totalRows.Load()

	callsDelta := callsNow - s.lastSampleCalls
	durDeltaNs := max(totalDurNow-s.lastSampleTotalDurationNs, 0)
	rowsDelta := rowsNow - s.lastSampleTotalRows

	s.lastSampleCalls = callsNow
	s.lastSampleTotalDurationNs = totalDurNow
	s.lastSampleTotalRows = rowsNow

	s.trends.callRate.push(float64(callsDelta) / intervalSec)
	s.trends.totalTime.push(float64(durDeltaNs) / 1e6 / intervalSec)
	s.trends.rowsRate.push(float64(rowsDelta) / intervalSec)

	// Percentile sparkline tracks per-interval distribution: snapshot the
	// histogram, push the percentile of (current - previous), then save
	// the snapshot for the next sample. Without this delta, the sparkline
	// would show cumulative-since-admission percentiles and flatten out.
	var counts, delta [numHistBuckets]uint64
	for i := range s.durationBuckets {
		counts[i] = s.durationBuckets[i].Load()
		delta[i] = counts[i] - s.lastSampleBuckets[i]
	}
	s.lastSampleBuckets = counts
	s.trends.p50Ms.push(float64(percentileNs(delta, 0.50)) / 1e6)
	s.trends.p99Ms.push(float64(percentileNs(delta, 0.99)) / 1e6)
}

func (s *QueryStats) snapshot(sampleIntervalSec float64) Snapshot {
	calls := s.calls.Load()
	total := time.Duration(s.totalDurationNs.Load())
	var avg time.Duration
	if calls > 0 {
		avg = total / time.Duration(calls)
	}

	var counts [numHistBuckets]uint64
	for i := range s.durationBuckets {
		counts[i] = s.durationBuckets[i].Load()
	}
	p50 := time.Duration(percentileNs(counts, 0.50))
	p99 := time.Duration(percentileNs(counts, 0.99))

	snap := Snapshot{
		Fingerprint:           s.fingerprint,
		NormalizedSQL:         s.normalizedSQL,
		Calls:                 calls,
		Errors:                s.errors.Load(),
		TotalDuration:         total,
		AverageDuration:       avg,
		MinDuration:           time.Duration(s.minDurationNs.Load()),
		MaxDuration:           time.Duration(s.maxDurationNs.Load()),
		P50Duration:           p50,
		P99Duration:           p99,
		TotalRows:             s.totalRows.Load(),
		LastSeen:              time.Unix(0, s.lastSeenUnixNs.Load()),
		SampleIntervalSeconds: sampleIntervalSec,
	}

	s.trendMu.Lock()
	if s.trends.initialized {
		snap.CallRateTrend = s.trends.callRate.snapshot()
		snap.TotalTimeMsTrend = s.trends.totalTime.snapshot()
		snap.P50MsTrend = s.trends.p50Ms.snapshot()
		snap.P99MsTrend = s.trends.p99Ms.snapshot()
		snap.RowsRateTrend = s.trends.rowsRate.snapshot()
	}
	s.trendMu.Unlock()

	return snap
}

func sortSnapshots(snapshots []Snapshot, sortBy SortKey) {
	var less func(i, j int) bool
	switch sortBy {
	case SortByTotalTime:
		less = func(i, j int) bool { return snapshots[i].TotalDuration > snapshots[j].TotalDuration }
	case SortByAverageTime:
		less = func(i, j int) bool { return snapshots[i].AverageDuration > snapshots[j].AverageDuration }
	case SortByErrors:
		less = func(i, j int) bool { return snapshots[i].Errors > snapshots[j].Errors }
	case SortByLastSeen:
		less = func(i, j int) bool { return snapshots[i].LastSeen.After(snapshots[j].LastSeen) }
	default:
		less = func(i, j int) bool { return snapshots[i].Calls > snapshots[j].Calls }
	}
	sort.Slice(snapshots, less)
}
