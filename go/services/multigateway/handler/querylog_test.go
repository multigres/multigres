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

package handler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// noopEmits returns a QueryLogEmits backed by a no-op counter for tests that
// don't care about the metric value.
func noopEmits() QueryLogEmits { return QueryLogEmits{noop.Int64Counter{}} }

// countingHandler records every Handle call and the level it was called at.
// It honors a minimum level via Enabled so we can simulate operator-set
// handler filtering.
type countingHandler struct {
	minLevel slog.Level
	calls    int
	levels   []slog.Level
	records  []slog.Record
}

func (h *countingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.calls++
	h.levels = append(h.levels, r.Level)
	h.records = append(h.records, r)
	return nil
}

func (h *countingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(_ string) slog.Handler      { return h }

func TestEmitQueryLog_NormalQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 50 * time.Millisecond,
		ParseDuration: 5 * time.Millisecond,
		ExecDuration:  45 * time.Millisecond,
		RowCount:      10,
	}

	var cursor atomic.Uint64
	emitQueryLog(context.Background(), logger, entry, time.Second, 1, &cursor, noopEmits())

	output := buf.String()
	require.Contains(t, output, "level=DEBUG")
	require.Contains(t, output, "query completed")
	require.Contains(t, output, "db.namespace=testdb")
	require.Contains(t, output, "db.operation.name=SELECT")
	require.Contains(t, output, "db.query.protocol=simple")
	require.Contains(t, output, "rows_returned=10")
	require.NotContains(t, output, "sqlstate")
}

func TestEmitQueryLog_ErrorQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 50 * time.Millisecond,
		Error:         errors.New("something failed"),
		SQLSTATE:      "XX000",
		ErrorSource:   "client",
	}

	var cursor atomic.Uint64
	emitQueryLog(context.Background(), logger, entry, time.Second, 0, &cursor, noopEmits())

	output := buf.String()
	require.Contains(t, output, "level=WARN")
	require.Contains(t, output, "sqlstate=XX000")
	require.Contains(t, output, "error.source=client")
}

func TestEmitQueryLog_SlowQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	entry := queryLogEntry{
		User:          "testuser",
		Database:      "testdb",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 2 * time.Second,
		RowCount:      1000,
	}

	var cursor atomic.Uint64
	emitQueryLog(context.Background(), logger, entry, time.Second, 0, &cursor, noopEmits())

	output := buf.String()
	require.Contains(t, output, "level=WARN")
	require.Contains(t, output, "slow_query=true")
}

// Errors always log at WARN even when the handler would drop the DEBUG-level
// normal path and sampling is disabled.
func TestEmitQueryLog_ErrorAlwaysWarn(t *testing.T) {
	h := &countingHandler{minLevel: slog.LevelInfo}
	logger := slog.New(h)

	entry := queryLogEntry{
		User:          "u",
		Database:      "d",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 10 * time.Millisecond,
		Error:         errors.New("boom"),
		SQLSTATE:      "XX000",
		ErrorSource:   "client",
	}

	var cursor atomic.Uint64
	emitQueryLog(context.Background(), logger, entry, time.Second, 0, &cursor, noopEmits())

	require.Equal(t, 1, h.calls)
	require.Equal(t, slog.LevelWarn, h.levels[0])

	var sawError bool
	h.records[0].Attrs(func(a slog.Attr) bool {
		if a.Key == "error" && a.Value.String() == "boom" {
			sawError = true
		}
		return true
	})
	require.True(t, sawError, "expected error attr to be present")
}

// Slow queries always log at WARN even when the handler would drop DEBUG
// and sampling is disabled.
func TestEmitQueryLog_SlowAlwaysWarn(t *testing.T) {
	h := &countingHandler{minLevel: slog.LevelInfo}
	logger := slog.New(h)

	entry := queryLogEntry{
		User:          "u",
		Database:      "d",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 2 * time.Second,
		RowCount:      1,
	}

	var cursor atomic.Uint64
	emitQueryLog(context.Background(), logger, entry, time.Second, 0, &cursor, noopEmits())

	require.Equal(t, 1, h.calls)
	require.Equal(t, slog.LevelWarn, h.levels[0])

	var slow bool
	h.records[0].Attrs(func(a slog.Attr) bool {
		if a.Key == "slow_query" && a.Value.Bool() {
			slow = true
		}
		return true
	})
	require.True(t, slow, "expected slow_query=true attr")
}

// With the default config (sampling disabled) and an INFO-level handler, the
// DEBUG-level normal path must short-circuit before Handle is ever called.
func TestEmitQueryLog_NormalPathSilentByDefault(t *testing.T) {
	h := &countingHandler{minLevel: slog.LevelInfo}
	logger := slog.New(h)

	entry := queryLogEntry{
		User:          "u",
		Database:      "d",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 10 * time.Millisecond,
		RowCount:      1,
	}

	var cursor atomic.Uint64
	for range 50 {
		emitQueryLog(context.Background(), logger, entry, time.Second, 0, &cursor, noopEmits())
	}

	require.Equal(t, 0, h.calls, "Handle must not be invoked on the disabled normal path")
}

// Sampling: with rate=4 and a DEBUG-level handler, exactly 12/4=3 of 12
// normal-path calls emit, all at DEBUG.
func TestEmitQueryLog_Sampling(t *testing.T) {
	h := &countingHandler{minLevel: slog.LevelDebug}
	logger := slog.New(h)

	entry := queryLogEntry{
		User:          "u",
		Database:      "d",
		OperationName: "SELECT",
		Protocol:      "simple",
		TotalDuration: 10 * time.Millisecond,
		RowCount:      1,
	}

	var cursor atomic.Uint64
	for range 12 {
		emitQueryLog(context.Background(), logger, entry, time.Second, 4, &cursor, noopEmits())
	}

	require.Equal(t, 3, h.calls)
	for _, lvl := range h.levels {
		require.Equal(t, slog.LevelDebug, lvl)
	}
}

// The QueryLogEmits counter is bumped per emitted record and labels each
// data point with its level. Sampled-out normal queries do not increment.
func TestEmitQueryLog_EmitsMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	c, err := mp.Meter("test").Int64Counter("mg.gateway.query.log.emits")
	require.NoError(t, err)
	emits := QueryLogEmits{c}

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	normal := queryLogEntry{
		User: "u", Database: "d", OperationName: "SELECT", Protocol: "simple",
		TotalDuration: 10 * time.Millisecond,
	}
	errored := queryLogEntry{
		User: "u", Database: "d", OperationName: "SELECT", Protocol: "simple",
		TotalDuration: 10 * time.Millisecond, Error: errors.New("boom"),
		SQLSTATE: "XX000", ErrorSource: "client",
	}

	var cursor atomic.Uint64
	// 12 normal queries with rate=4 → 3 debug emits.
	for range 12 {
		emitQueryLog(context.Background(), logger, normal, time.Second, 4, &cursor, emits)
	}
	// 2 errors → 2 warn emits (sampling does not apply).
	emitQueryLog(context.Background(), logger, errored, time.Second, 4, &cursor, emits)
	emitQueryLog(context.Background(), logger, errored, time.Second, 4, &cursor, emits)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "mg.gateway.query.log.emits" {
				continue
			}
			sum := m.Data.(metricdata.Sum[int64])
			for _, dp := range sum.DataPoints {
				lvl, _ := dp.Attributes.Value("level")
				counts[lvl.AsString()] = dp.Value
			}
		}
	}

	require.Equal(t, int64(3), counts["debug"], "expected 3 debug emits (12 normal queries / sample rate 4)")
	require.Equal(t, int64(2), counts["warn"], "expected 2 warn emits (errors bypass sampling)")
}
