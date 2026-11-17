// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardingcatalogcache

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Values of the failure reason attribute.
const (
	// failureFetch is a fetch RPC that failed.
	failureFetch = "fetch"
	// failureInvalid is a snapshot that failed validation.
	failureInvalid = "invalid"
)

type metrics struct {
	routingVersion metric.Int64Gauge
	fetchFailures  metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/shardingcatalogcache")
	m := &metrics{}
	var errs []error
	var err error

	m.routingVersion, err = meter.Int64Gauge(
		"mg.gateway.sharding_catalog.routing_version",
		metric.WithDescription("routing_version of the sharding catalog snapshot the gateway has installed, per database"),
		metric.WithUnit("{version}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.sharding_catalog.routing_version gauge: %w", err))
		m.routingVersion = noop.Int64Gauge{}
	}

	m.fetchFailures, err = meter.Int64Counter(
		"mg.gateway.sharding_catalog.fetch.failures",
		metric.WithDescription("Sharding catalog snapshot fetches that left the cached snapshot in place, by reason (fetch, invalid)"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.sharding_catalog.fetch.failures counter: %w", err))
		m.fetchFailures = noop.Int64Counter{}
	}
	return m, errors.Join(errs...)
}

func (m *metrics) installed(ctx context.Context, database string, routingVersion int64) {
	if m == nil {
		return
	}
	m.routingVersion.Record(ctx, routingVersion, metric.WithAttributes(attribute.String("db.namespace", database)))
}

func (m *metrics) fetchFailed(ctx context.Context, database, reason string) {
	if m == nil {
		return
	}
	m.fetchFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("db.namespace", database),
		attribute.String("reason", reason),
	))
}
