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
// All counters are updated with atomic ops so reads via Snapshot don't
// need to hold any lock.
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
}

// CachedSize implements theine's cacheval interface so theine can bound
// the registry by bytes, not entries. The constant overhead approximates
// the struct + pointer/atomic headers; the variable part covers the strings.
func (s *QueryStats) CachedSize(_ bool) int64 {
	return 256 + int64(len(s.fingerprint)) + int64(len(s.normalizedSQL))
}

// Snapshot is a point-in-time copy of a QueryStats, safe to hand out to
// HTTP handlers or serialize to JSON.
type Snapshot struct {
	Fingerprint     string        `json:"fingerprint"`
	NormalizedSQL   string        `json:"normalized_sql"`
	Calls           uint64        `json:"calls"`
	Errors          uint64        `json:"errors"`
	TotalDuration   time.Duration `json:"total_duration_ns"`
	AverageDuration time.Duration `json:"average_duration_ns"`
	MinDuration     time.Duration `json:"min_duration_ns"`
	MaxDuration     time.Duration `json:"max_duration_ns"`
	TotalRows       uint64        `json:"total_rows"`
	LastSeen        time.Time     `json:"last_seen"`
}

// Registry tracks per-fingerprint query statistics.
// The zero value is not usable — construct via New.
type Registry struct {
	store      *theine.Store[theine.StringKey, *QueryStats]
	maxSQLLen  int
	newEntryMu sync.Mutex
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
}

// DefaultConfig returns reasonable defaults for the registry.
func DefaultConfig() Config {
	return Config{
		MaxMemoryBytes: 2 * 1024 * 1024, // 2 MB
		MaxSQLLength:   4096,
	}
}

// New constructs a Registry with the given config.
// If cfg.MaxMemoryBytes <= 0 the registry is disabled and all methods become no-ops.
func New(cfg Config) *Registry {
	r := &Registry{maxSQLLen: cfg.MaxSQLLength}
	if cfg.MaxMemoryBytes > 0 {
		r.store = theine.NewStore[theine.StringKey, *QueryStats](int64(cfg.MaxMemoryBytes), true)
	}
	return r
}

// NewForTest constructs a Registry without the TinyLFU doorkeeper so tests
// can assert deterministic admission behavior.
func NewForTest(cfg Config) *Registry {
	r := &Registry{maxSQLLen: cfg.MaxSQLLength}
	if cfg.MaxMemoryBytes > 0 {
		r.store = theine.NewStore[theine.StringKey, *QueryStats](int64(cfg.MaxMemoryBytes), false)
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

	var snapshots []Snapshot
	r.store.Range(0, func(_ theine.StringKey, v *QueryStats) bool {
		snapshots = append(snapshots, v.snapshot())
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
	r.store.Close()
}

func (s *QueryStats) snapshot() Snapshot {
	calls := s.calls.Load()
	total := time.Duration(s.totalDurationNs.Load())
	var avg time.Duration
	if calls > 0 {
		avg = total / time.Duration(calls)
	}
	return Snapshot{
		Fingerprint:     s.fingerprint,
		NormalizedSQL:   s.normalizedSQL,
		Calls:           calls,
		Errors:          s.errors.Load(),
		TotalDuration:   total,
		AverageDuration: avg,
		MinDuration:     time.Duration(s.minDurationNs.Load()),
		MaxDuration:     time.Duration(s.maxDurationNs.Load()),
		TotalRows:       s.totalRows.Load(),
		LastSeen:        time.Unix(0, s.lastSeenUnixNs.Load()),
	}
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
