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

package heartbeat

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"

// Attribute values for the mg.pooler.heartbeat.* metrics.
const (
	attrOperation = "operation"
	attrReason    = "reason"

	operationWrite = "write"
	operationRead  = "read"

	// reasonSchema: the heartbeat table does not match the queries (see IsSchemaError).
	// It needs an operator or a pooler upgrade, not a retry.
	reasonSchema = "schema"
	// reasonOther: anything else, e.g. a timeout, a lost connection or a missing row.
	reasonOther = "other"
)

// registerMetrics reports the Writer's and Reader's counters as OTel metrics:
//
//   - mg.pooler.heartbeat.operations: successful heartbeat writes and reads.
//   - mg.pooler.heartbeat.errors: failed heartbeat writes and reads, split into schema
//     errors (an incompatible heartbeat table) and everything else.
//
// They are observable counters read from the atomics the Writer and Reader already keep,
// so the heartbeat hot path does no metric work. A heartbeat error was previously only a
// log line and an in-process count nothing read. The returned function unregisters the
// callback. Best-effort: an instrument that fails to initialise is skipped and the joined
// error is returned for logging.
func registerMetrics(hw *Writer, hr *Reader) (unregister func(), _ error) {
	meter := otel.Meter(meterName)

	operations, opsErr := meter.Int64ObservableCounter(
		"mg.pooler.heartbeat.operations",
		metric.WithDescription("Successful heartbeat table writes (leader) and reads (standby)"),
		metric.WithUnit("{operation}"),
	)
	errorsCounter, errsErr := meter.Int64ObservableCounter(
		"mg.pooler.heartbeat.errors",
		metric.WithDescription("Failed heartbeat table writes (leader) and reads (standby). "+
			"reason=schema means the heartbeat table does not match what this release needs."),
		metric.WithUnit("{error}"),
	)
	var instruments []metric.Observable
	var errs []error
	if opsErr == nil {
		instruments = append(instruments, operations)
	} else {
		errs = append(errs, fmt.Errorf("mg.pooler.heartbeat.operations: %w", opsErr))
	}
	if errsErr == nil {
		instruments = append(instruments, errorsCounter)
	} else {
		errs = append(errs, fmt.Errorf("mg.pooler.heartbeat.errors: %w", errsErr))
	}
	if len(instruments) == 0 {
		return func() {}, errors.Join(errs...)
	}

	op := func(name string) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String(attrOperation, name))
	}
	failed := func(name, reason string) metric.MeasurementOption {
		return metric.WithAttributes(attribute.String(attrOperation, name), attribute.String(attrReason, reason))
	}
	reg, regErr := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if opsErr == nil {
			o.ObserveInt64(operations, hw.Writes(), op(operationWrite))
			o.ObserveInt64(operations, hr.Reads(), op(operationRead))
		}
		if errsErr == nil {
			// The schema count is loaded before the total: both only grow and the total is
			// incremented first, so total-schema is never negative.
			ws := hw.WriteSchemaErrors()
			o.ObserveInt64(errorsCounter, ws, failed(operationWrite, reasonSchema))
			o.ObserveInt64(errorsCounter, hw.WriteErrors()-ws, failed(operationWrite, reasonOther))
			rs := hr.ReadSchemaErrors()
			o.ObserveInt64(errorsCounter, rs, failed(operationRead, reasonSchema))
			o.ObserveInt64(errorsCounter, hr.ReadErrors()-rs, failed(operationRead, reasonOther))
		}
		return nil
	}, instruments...)
	if regErr != nil {
		return func() {}, errors.Join(append(errs, fmt.Errorf("heartbeat metrics callback: %w", regErr))...)
	}
	return func() { _ = reg.Unregister() }, errors.Join(errs...)
}
