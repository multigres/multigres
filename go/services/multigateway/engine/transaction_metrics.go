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

package engine

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Transaction outcome constants for metric attribution.
const (
	TxnOutcomeCommit   = "commit"
	TxnOutcomeRollback = "rollback"
)

// TransactionMetrics holds OTel metrics for transaction lifecycle tracking.
// Initialized once in the executor and injected into TransactionPrimitive
// at creation time, following the same pattern as HandlerMetrics.
type TransactionMetrics struct {
	duration TxnDuration
	count    TxnCount
}

// TxnDuration wraps a Float64Histogram for recording transaction durations.
type TxnDuration struct {
	metric.Float64Histogram
}

// Record records a transaction duration with the database and outcome attributes.
func (m TxnDuration) Record(ctx context.Context, durationSec float64, dbNamespace, outcome string) {
	m.Float64Histogram.Record(ctx, durationSec,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("outcome", outcome),
		))
}

// TxnCount wraps an Int64Counter for counting completed transactions.
type TxnCount struct {
	metric.Int64Counter
}

// Add increments the transaction counter with the database and outcome attributes.
func (m TxnCount) Add(ctx context.Context, dbNamespace, outcome string) {
	m.Int64Counter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("db.namespace", dbNamespace),
			attribute.String("outcome", outcome),
		))
}

// RecordCompletion records both duration and count for a completed transaction.
func (m *TransactionMetrics) RecordCompletion(ctx context.Context, durationSec float64, dbNamespace, outcome string) {
	if m == nil {
		return
	}
	m.duration.Record(ctx, durationSec, dbNamespace, outcome)
	m.count.Add(ctx, dbNamespace, outcome)
}

// NewTransactionMetrics initialises OTel metrics for transaction tracking.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewTransactionMetrics() (*TransactionMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/engine")
	m := &TransactionMetrics{}
	var errs []error

	dur, err := meter.Float64Histogram(
		"mg.gateway.transaction.duration",
		metric.WithDescription("Duration of transactions from BEGIN to COMMIT/ROLLBACK"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 600),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.transaction.duration histogram: %w", err))
		m.duration = TxnDuration{noop.Float64Histogram{}}
	} else {
		m.duration = TxnDuration{dur}
	}

	cnt, err := meter.Int64Counter(
		"mg.gateway.transaction.count",
		metric.WithDescription("Total number of completed transactions"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.transaction.count counter: %w", err))
		m.count = TxnCount{noop.Int64Counter{}}
	} else {
		m.count = TxnCount{cnt}
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
