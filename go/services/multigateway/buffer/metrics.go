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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// stats holds OpenTelemetry metrics for failover buffering.
type stats struct {
	meter metric.Meter

	requestsBuffered metric.Int64Counter
	requestsDrained  metric.Int64Counter
	requestsEvicted  metric.Int64Counter
	requestsSkipped  metric.Int64Counter
	failoverCount    metric.Int64Counter
	waitDuration     metric.Float64Histogram
}

func newStats() *stats {
	s := &stats{
		meter: otel.Meter("github.com/multigres/multigres/go/services/multigateway/buffer"),
	}

	var err error

	s.requestsBuffered, err = s.meter.Int64Counter(
		"multigateway.buffer.requests.buffered",
		metric.WithDescription("Number of requests buffered during failover"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		s.requestsBuffered = noop.Int64Counter{}
	}

	s.requestsDrained, err = s.meter.Int64Counter(
		"multigateway.buffer.requests.drained",
		metric.WithDescription("Number of buffered requests successfully drained (retried)"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		s.requestsDrained = noop.Int64Counter{}
	}

	s.requestsEvicted, err = s.meter.Int64Counter(
		"multigateway.buffer.requests.evicted",
		metric.WithDescription("Number of buffered requests evicted (window timeout, buffer full, max duration)"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		s.requestsEvicted = noop.Int64Counter{}
	}

	s.requestsSkipped, err = s.meter.Int64Counter(
		"multigateway.buffer.requests.skipped",
		metric.WithDescription("Number of requests that skipped buffering (timing guards, disabled, etc.)"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		s.requestsSkipped = noop.Int64Counter{}
	}

	s.failoverCount, err = s.meter.Int64Counter(
		"multigateway.buffer.failovers",
		metric.WithDescription("Number of failovers detected (transitions to BUFFERING)"),
		metric.WithUnit("{failover}"),
	)
	if err != nil {
		s.failoverCount = noop.Int64Counter{}
	}

	s.waitDuration, err = s.meter.Float64Histogram(
		"multigateway.buffer.wait.duration",
		metric.WithDescription("Time requests spent waiting in the buffer"),
		metric.WithUnit("s"),
	)
	if err != nil {
		s.waitDuration = noop.Float64Histogram{}
	}

	return s
}

func (s *stats) recordBuffered(ctx context.Context, shardKey string) {
	s.requestsBuffered.Add(ctx, 1, metric.WithAttributes(attribute.String("shard_key", shardKey)))
}

func (s *stats) recordDrained(ctx context.Context, shardKey string) {
	s.requestsDrained.Add(ctx, 1, metric.WithAttributes(attribute.String("shard_key", shardKey)))
}

func (s *stats) recordEvicted(ctx context.Context, shardKey string, reason string) {
	s.requestsEvicted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("shard_key", shardKey),
		attribute.String("reason", reason),
	))
}

func (s *stats) recordSkipped(ctx context.Context, reason string) {
	s.requestsSkipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (s *stats) recordFailover(ctx context.Context, shardKey string) {
	s.failoverCount.Add(ctx, 1, metric.WithAttributes(attribute.String("shard_key", shardKey)))
}

func (s *stats) recordWaitDuration(ctx context.Context, seconds float64) {
	s.waitDuration.Record(ctx, seconds)
}
