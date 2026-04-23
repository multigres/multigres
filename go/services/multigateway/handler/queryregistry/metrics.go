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

package queryregistry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/common/cache/theine"
)

// meterName is the OTel meter scope for query-registry metrics.
const meterName = "github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"

// RegisterMetrics wires an "info" gauge that emits one series per tracked
// fingerprint with both the fingerprint hash and the (truncated) normalized
// SQL as labels. Joining this against the per-fingerprint counters via
// Prometheus's `* on(query_fingerprint) group_left(query_normalized_sql)`
// lets dashboards label series by SQL text instead of opaque hashes.
//
// Safe to call on a nil or disabled registry (returns nil without registering).
func (r *Registry) RegisterMetrics() error {
	if r == nil || r.store == nil {
		return nil
	}

	meter := otel.Meter(meterName)

	info, err := meter.Int64ObservableGauge(
		"mg.gateway.query.info",
		metric.WithDescription("One series per tracked query fingerprint, exposing fingerprint→normalized SQL mapping for join queries"),
	)
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			r.store.Range(0, func(_ theine.StringKey, v *QueryStats) bool {
				// normalizedSQL is already capped at r.maxSQLLen when the
				// registry admits the entry (see Record); reuse that cap as
				// the single source of truth for the exposed label too.
				o.ObserveInt64(info, 1, metric.WithAttributes(
					attribute.String("query.fingerprint", v.fingerprint),
					attribute.String("query.normalized_sql", v.normalizedSQL),
				))
				return true
			})
			// Synthetic entries for the aggregated buckets so table joins against
			// mg_gateway_query_info resolve for every label value emitted on the
			// duration/errors/rows metrics — otherwise __utility__ and __other__
			// rows would have an empty SQL column in the dashboard.
			o.ObserveInt64(info, 1, metric.WithAttributes(
				attribute.String("query.fingerprint", UtilityLabel),
				attribute.String("query.normalized_sql", "(utility statements: BEGIN, COMMIT, SET, DDL, …)"),
			))
			o.ObserveInt64(info, 1, metric.WithAttributes(
				attribute.String("query.fingerprint", OtherLabel),
				attribute.String("query.normalized_sql", "(long-tail query shapes not in tracked top set)"),
			))
			return nil
		},
		info,
	)
	return err
}
