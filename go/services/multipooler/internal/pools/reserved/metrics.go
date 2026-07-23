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

package reserved

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Transaction outcome attribute values.
const (
	txnOutcomeCommit   = "commit"
	txnOutcomeRollback = "rollback"
	txnOutcomeAbort    = "abort"
)

// txnMetrics holds OTel metrics for reserved-connection transaction outcomes.
// commit/rollback are recorded by Conn.Commit/Rollback (so intermediate
// transactions on a long-lived reservation are counted); abort is recorded at
// release for any connection still in-transaction (a clean commit/rollback
// removes the transaction reason, so anything still flagged was not concluded).
type txnMetrics struct {
	duration metric.Float64Histogram
	outcomes metric.Int64Counter
}

func newTxnMetrics() *txnMetrics {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved")
	m := &txnMetrics{}

	var err error
	m.duration, err = meter.Float64Histogram(
		"mg.pooler.txn.duration",
		metric.WithDescription("Reserved-connection transaction lifetime by outcome"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60),
	)
	if err != nil {
		m.duration = noop.Float64Histogram{}
	}

	m.outcomes, err = meter.Int64Counter(
		"mg.pooler.txn.outcomes",
		metric.WithDescription("Reserved-connection transaction outcomes (commit/rollback/abort)"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		m.outcomes = noop.Int64Counter{}
	}

	return m
}

// record counts one transaction outcome and, when d > 0, records its lifetime.
// Nil-safe so connections created without a pool (e.g. in tests) are no-ops.
func (m *txnMetrics) record(ctx context.Context, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	m.outcomes.Add(ctx, 1, attrs)
	if d > 0 {
		m.duration.Record(ctx, d.Seconds(), attrs)
	}
}
