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

// Package replication holds the multipooler-internal per-stream metrics
// recorder for the protocol-blind replication tunnel. The reusable tunnel core
// lives in go/common/replication; this package supplies the OpenTelemetry
// instruments and a Stream recorder that satisfies its TunnelMetrics interface.
package replication

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/replication"

// Attribute keys and values for replication-tunnel metrics.
const (
	attrDirection = "direction"
	attrUser      = "user"
	attrReason    = "reason"

	directionUpstream   = "upstream"   // client -> backend
	directionDownstream = "downstream" // backend -> client
)

// Termination reasons recorded on mg.pooler.replication.terminations.
const (
	TerminationClientDisconnect = "client_disconnect"
	TerminationPostgresError    = "postgres_error"
	TerminationBackendError     = "backend_error"
	TerminationLeaderChange     = "leader_change"
	TerminationGRPCError        = "grpc_error"
)

// Metrics holds the process-global OpenTelemetry instruments for replication
// tunnels. Create one per process with NewMetrics; derive a per-stream recorder
// with NewStream. A nil *Metrics yields nil per-stream recorders, which are
// valid no-ops.
type Metrics struct {
	bytesCounter   metric.Int64Counter
	chunksCounter  metric.Int64Counter
	forwardLatency metric.Float64Histogram
	active         metric.Int64UpDownCounter
	duration       metric.Float64Histogram
	setupLatency   metric.Float64Histogram
	terminations   metric.Int64Counter
}

// NewMetrics initialises the replication-tunnel instruments. Instruments that
// fail to initialise fall back to noop and are reported in the returned
// (joined) error; the returned *Metrics is always usable.
//
// It takes no arguments by repo convention (metrics stay isolated from service
// code); per-stream attributes such as user are supplied via NewStream.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter(meterName)
	m := &Metrics{}
	var errs []error

	var err error
	if m.bytesCounter, err = meter.Int64Counter(
		"mg.pooler.replication.bytes",
		metric.WithDescription("Bytes tunneled per direction over replication streams"),
		metric.WithUnit("By"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.bytes: %w", err))
		m.bytesCounter = noop.Int64Counter{}
	}

	if m.chunksCounter, err = meter.Int64Counter(
		"mg.pooler.replication.chunks",
		metric.WithDescription("Chunks tunneled per direction (pairs with bytes for mean chunk size)"),
		metric.WithUnit("{chunk}"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.chunks: %w", err))
		m.chunksCounter = noop.Int64Counter{}
	}

	if m.forwardLatency, err = meter.Float64Histogram(
		"mg.pooler.replication.forward.latency",
		metric.WithDescription("Time from reading a chunk off one socket to completing its write on the other"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.forward.latency: %w", err))
		m.forwardLatency = noop.Float64Histogram{}
	}

	if m.active, err = meter.Int64UpDownCounter(
		"mg.pooler.replication.active",
		metric.WithDescription("Active replication tunnels"),
		metric.WithUnit("{stream}"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.active: %w", err))
		m.active = noop.Int64UpDownCounter{}
	}

	if m.duration, err = meter.Float64Histogram(
		"mg.pooler.replication.duration",
		metric.WithDescription("Wall-clock lifetime of a replication tunnel, recorded at teardown"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 1, 10, 60, 300, 1800, 3600, 21600, 86400),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.duration: %w", err))
		m.duration = noop.Float64Histogram{}
	}

	if m.setupLatency, err = meter.Float64Histogram(
		"mg.pooler.replication.setup.latency",
		metric.WithDescription("Time from StreamReplication open to backend connection ready"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.setup.latency: %w", err))
		m.setupLatency = noop.Float64Histogram{}
	}

	if m.terminations, err = meter.Int64Counter(
		"mg.pooler.replication.terminations",
		metric.WithDescription("Replication tunnel terminations by reason"),
		metric.WithUnit("{termination}"),
	); err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.replication.terminations: %w", err))
		m.terminations = noop.Int64Counter{}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// Stream is a per-tunnel metrics recorder. The direction and user attributes
// are fixed for a tunnel's lifetime, so their MeasurementOptions are built once
// here and reused via slice spread on every Record — this keeps the per-chunk
// hot path allocation-free (per-call metric.WithAttributes is the classic OTel
// hot-path allocation footgun, and a fresh variadic argument escapes through
// the global delegating meter).
//
// A nil *Stream is a valid no-op receiver.
type Stream struct {
	m   *Metrics
	ctx context.Context

	// Pre-built option slices, spread on every Add/Record. *AddOpts feed
	// counters/up-down counters; *RecOpts feed histograms. down/up carry
	// direction + user (bytes, chunks); *LatOpts carry direction only
	// (forward.latency); user* carry user only (active, duration, setup).
	downBytesOpts []metric.AddOption
	upBytesOpts   []metric.AddOption
	downLatOpts   []metric.RecordOption
	upLatOpts     []metric.RecordOption
	userAddOpts   []metric.AddOption
	userRecOpts   []metric.RecordOption
}

// NewStream derives a per-tunnel recorder for the given user. Returns nil (a
// valid no-op) when m is nil.
func (m *Metrics) NewStream(user string) *Stream {
	if m == nil {
		return nil
	}
	downBytes := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(attrDirection, directionDownstream),
		attribute.String(attrUser, user),
	))
	upBytes := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(attrDirection, directionUpstream),
		attribute.String(attrUser, user),
	))
	downDir := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(attrDirection, directionDownstream),
	))
	upDir := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(attrDirection, directionUpstream),
	))
	userSet := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(attrUser, user),
	))
	return &Stream{
		m: m,
		// Metrics recording is context-free and must continue during stream
		// teardown, after the request context has been cancelled.
		ctx:           context.Background(), //nolint:gocritic // see comment above
		downBytesOpts: []metric.AddOption{downBytes},
		upBytesOpts:   []metric.AddOption{upBytes},
		downLatOpts:   []metric.RecordOption{downDir},
		upLatOpts:     []metric.RecordOption{upDir},
		userAddOpts:   []metric.AddOption{userSet},
		userRecOpts:   []metric.RecordOption{userSet},
	}
}

// RecordDownstream records n bytes (one chunk) flowing backend -> client.
// Hot path: must stay allocation-free. Satisfies replication.TunnelMetrics.
func (s *Stream) RecordDownstream(n int) {
	if s == nil {
		return
	}
	s.m.bytesCounter.Add(s.ctx, int64(n), s.downBytesOpts...)
	s.m.chunksCounter.Add(s.ctx, 1, s.downBytesOpts...)
}

// RecordUpstream records n bytes (one chunk) flowing client -> backend.
// Hot path: must stay allocation-free. Satisfies replication.TunnelMetrics.
func (s *Stream) RecordUpstream(n int) {
	if s == nil {
		return
	}
	s.m.bytesCounter.Add(s.ctx, int64(n), s.upBytesOpts...)
	s.m.chunksCounter.Add(s.ctx, 1, s.upBytesOpts...)
}

// recordDownstreamLatency records, in milliseconds, the time to forward one
// backend -> client chunk. Hot path: must stay allocation-free.
func (s *Stream) recordDownstreamLatency(ms float64) {
	if s == nil {
		return
	}
	s.m.forwardLatency.Record(s.ctx, ms, s.downLatOpts...)
}

// recordUpstreamLatency records, in milliseconds, the time to forward one
// client -> backend chunk. Hot path: must stay allocation-free.
func (s *Stream) recordUpstreamLatency(ms float64) {
	if s == nil {
		return
	}
	s.m.forwardLatency.Record(s.ctx, ms, s.upLatOpts...)
}

// IncActive increments the active-tunnel gauge. Call once when a tunnel goes live.
func (s *Stream) IncActive() {
	if s == nil {
		return
	}
	s.m.active.Add(s.ctx, 1, s.userAddOpts...)
}

// DecActive decrements the active-tunnel gauge. Call once at teardown.
func (s *Stream) DecActive() {
	if s == nil {
		return
	}
	s.m.active.Add(s.ctx, -1, s.userAddOpts...)
}

// RecordDuration records the wall-clock lifetime of a tunnel at teardown.
func (s *Stream) RecordDuration(seconds float64) {
	if s == nil {
		return
	}
	s.m.duration.Record(s.ctx, seconds, s.userRecOpts...)
}

// RecordSetupLatency records the time from RPC open to backend connection ready.
func (s *Stream) RecordSetupLatency(seconds float64) {
	if s == nil {
		return
	}
	s.m.setupLatency.Record(s.ctx, seconds, s.userRecOpts...)
}

// RecordTermination records a tunnel termination with its reason. Called once
// per tunnel at teardown, so per-call WithAttributes is acceptable here.
func (s *Stream) RecordTermination(reason string) {
	if s == nil {
		return
	}
	s.m.terminations.Add(s.ctx, 1, metric.WithAttributes(attribute.String(attrReason, reason)))
}
