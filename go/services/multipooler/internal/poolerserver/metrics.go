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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
		metric.WithDescription("Wall-clock duration of graceful drain on NOT_SERVING transition"),
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
)

// recordDrain records a completed drain event with its wall-clock duration
// and outcome.
func (s *drainStats) recordDrain(ctx context.Context, seconds float64, outcome string, poolerType clustermetadatapb.PoolerType) {
	attrs := drainAttributes(poolerType)
	s.duration.Record(ctx, seconds, metric.WithAttributes(attrs...))
	s.outcome.Add(ctx, 1, metric.WithAttributes(append(attrs, attribute.String("outcome", outcome))...))
}

// recordForceClosed adds to the count of connections force-closed across
// all drain events.
func (s *drainStats) recordForceClosed(ctx context.Context, n int, poolerType clustermetadatapb.PoolerType) {
	if n <= 0 {
		return
	}
	s.forceClosed.Add(ctx, int64(n), metric.WithAttributes(drainAttributes(poolerType)...))
}

func drainAttributes(poolerType clustermetadatapb.PoolerType) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("pooler_type", poolerTypeLabel(poolerType)),
	}
}

func poolerTypeLabel(poolerType clustermetadatapb.PoolerType) string {
	switch poolerType {
	case clustermetadatapb.PoolerType_PRIMARY:
		return "primary"
	case clustermetadatapb.PoolerType_REPLICA:
		return "replica"
	default:
		return "unknown"
	}
}
