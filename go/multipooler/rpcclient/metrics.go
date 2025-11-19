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

package rpcclient

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// DialPath represents the possible paths for dialing a connection.
type DialPath string

const (
	DialPathCacheFast DialPath = "cache_fast" // Connection retrieved from cache without blocking
	DialPathSemaFast  DialPath = "sema_fast"  // Semaphore acquired without blocking to dial new connection
	DialPathSemaPoll  DialPath = "sema_poll"  // Polling for evictable connections while waiting for capacity
)

// Metrics holds all OpenTelemetry metrics for the rpcclient connection cache.
type Metrics struct {
	meter        metric.Meter
	connReuse    metric.Int64Counter
	connNew      metric.Int64Counter
	dialTimeouts metric.Int64Counter
	cacheSize    CacheSize
	dialDuration DialDuration
}

// CacheSize wraps an Int64ObservableGauge for observing cache size.
// Use the Inst() method to get the underlying gauge for callback registration.
type CacheSize struct {
	metric.Int64ObservableGauge
}

// Inst returns the underlying metric instrument for callback registration.
func (m CacheSize) Inst() metric.Int64ObservableGauge {
	return m.Int64ObservableGauge
}

// DialDuration wraps a Float64Histogram for recording dial operation durations.
// It abstracts away attribute key names so callers don't need to know them.
type DialDuration struct {
	metric.Float64Histogram
}

// Record records a dial operation duration with proper OTel attributes.
//
// Parameters:
//   - ctx: Context for the metric recording
//   - val: How long the dial took (in seconds)
//   - path: The dial path taken (cache_fast, sema_fast, or sema_poll)
//   - attrs: Optional additional attributes to include in the metric
func (m DialDuration) Record(
	ctx context.Context,
	val float64,
	path DialPath,
	attrs ...attribute.KeyValue,
) {
	m.Float64Histogram.Record(ctx, val,
		metric.WithAttributes(
			append(
				attrs,
				attribute.String("path", string(path)),
			)...,
		))
}

// NewMetrics initializes OpenTelemetry metrics for the rpcclient connection cache.
// For the cacheSize observable gauge, use RegisterCacheSizeCallback() to register a callback.
func NewMetrics() *Metrics {
	m := &Metrics{
		meter: otel.Meter("github.com/multigres/multigres/go/multipooler/rpcclient"),
	}

	var err error

	// Counter for connection reuse
	m.connReuse, err = m.meter.Int64Counter(
		"rpcclient.connection.reuses",
		metric.WithDescription("Number of connection reuse events"),
		metric.WithUnit("{reuse}"),
	)
	if err != nil {
		m.connReuse = noop.Int64Counter{}
	}

	// Counter for new connections
	m.connNew, err = m.meter.Int64Counter(
		"rpcclient.connection.creates",
		metric.WithDescription("Number of connection creation events"),
		metric.WithUnit("{create}"),
	)
	if err != nil {
		m.connNew = noop.Int64Counter{}
	}

	// Counter for dial timeouts
	m.dialTimeouts, err = m.meter.Int64Counter(
		"rpcclient.connection.dial.timeouts",
		metric.WithDescription("Number of connection dial timeout events"),
		metric.WithUnit("{timeout}"),
	)
	if err != nil {
		m.dialTimeouts = noop.Int64Counter{}
	}

	// Observable gauge for cache size
	cacheSizeGauge, err := m.meter.Int64ObservableGauge(
		"rpcclient.connection.cache.size",
		metric.WithDescription("Current number of cached connections"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		m.cacheSize = CacheSize{noop.Int64ObservableGauge{}}
	} else {
		m.cacheSize = CacheSize{cacheSizeGauge}
	}

	// Histogram for dial duration
	dialDurationHistogram, err := m.meter.Float64Histogram(
		"rpcclient.connection.dial.duration",
		metric.WithDescription("Duration of connection dial operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		m.dialDuration = DialDuration{noop.Float64Histogram{}}
	} else {
		m.dialDuration = DialDuration{dialDurationHistogram}
	}

	return m
}

// RegisterCacheSizeCallback registers a callback for the cache size observable gauge.
// The cacheGetter function is called periodically to observe the current cache size.
// Returns an error if callback registration fails.
func (m *Metrics) RegisterCacheSizeCallback(cacheGetter func() int) error {
	if cacheGetter == nil {
		return nil
	}
	_, err := m.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(m.cacheSize.Inst(), int64(cacheGetter()))
			return nil
		},
		m.cacheSize.Inst(),
	)
	return err
}

// AddConnReuse increments the connection reuse counter.
func (m *Metrics) AddConnReuse(ctx context.Context) {
	m.connReuse.Add(ctx, 1)
}

// AddConnNew increments the new connection counter.
func (m *Metrics) AddConnNew(ctx context.Context) {
	m.connNew.Add(ctx, 1)
}

// AddDialTimeout increments the dial timeout counter.
func (m *Metrics) AddDialTimeout(ctx context.Context) {
	m.dialTimeouts.Add(ctx, 1)
}

// RecordDialDuration records a dial operation duration with the specified path.
func (m *Metrics) RecordDialDuration(ctx context.Context, duration time.Duration, path DialPath) {
	m.dialDuration.Record(ctx, duration.Seconds(), path)
}
