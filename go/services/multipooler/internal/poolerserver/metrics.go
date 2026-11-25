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

package poolerserver

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// drainStats holds OpenTelemetry metrics for graceful-drain observability.
// Operators use these to size --connpool-drain-grace-period and to
// decompose the gateway-perceived failover duration into pooler-time vs.
// new-primary-election time.
type drainStats struct {
	meter metric.Meter

	duration    metric.Float64Histogram
	outcome     metric.Int64Counter
	forceClosed metric.Int64Counter
}

func newDrainStats() *drainStats {
	s := &drainStats{
		meter: otel.Meter("github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"),
	}

	var err error

	s.duration, err = s.meter.Float64Histogram(
		"mg.pooler.drain.duration",
		metric.WithDescription("Wall-clock duration of a graceful drain: a not-serving transition, or an admission fence closing the gate"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 2, 5, 10, 30, 60),
	)
	if err != nil {
		s.duration = noop.Float64Histogram{}
	}

	s.outcome, err = s.meter.Int64Counter(
		"mg.pooler.drain.outcome",
		metric.WithDescription("Drain events by outcome (graceful vs. force_close)"),
		metric.WithUnit("{drain}"),
	)
	if err != nil {
		s.outcome = noop.Int64Counter{}
	}

	s.forceClosed, err = s.meter.Int64Counter(
		"mg.pooler.drain.force_closed",
		metric.WithDescription("Reserved connections force-closed because drain exceeded the grace period"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		s.forceClosed = noop.Int64Counter{}
	}

	return s
}

const (
	drainOutcomeGraceful   = "graceful"
	drainOutcomeForceClose = "force_close"
	// drainOutcomeCancelled: a fence drain was cancelled before it ended, by
	// an unfence or a change of the pooler's role (see fence_gate.go).
	drainOutcomeCancelled = "cancelled"
)

// Drain reasons, the reason attribute of every drain metric: the not-serving
// transition of a failover or shutdown, or an admission fence closing the
// gate (see fence_gate.go).
const (
	drainReasonFailover = "failover"
	drainReasonFence    = "fence"
)

// recordDrain records a completed drain event with its wall-clock duration,
// outcome and reason.
func (s *drainStats) recordDrain(ctx context.Context, seconds float64, outcome, reason string, role servingstate.RoutingRole) {
	attrs := drainAttributes(reason, role)
	s.duration.Record(ctx, seconds, metric.WithAttributes(attrs...))
	s.outcome.Add(ctx, 1, metric.WithAttributes(append(attrs, attribute.String("outcome", outcome))...))
}

// recordForceClosed adds to the count of connections force-closed across
// all drain events.
func (s *drainStats) recordForceClosed(ctx context.Context, n int, reason string, role servingstate.RoutingRole) {
	if n <= 0 {
		return
	}
	s.forceClosed.Add(ctx, int64(n), metric.WithAttributes(drainAttributes(reason, role)...))
}

func drainAttributes(reason string, role servingstate.RoutingRole) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("reason", reason),
		attribute.String("routing_role", role.String()),
	}
}
