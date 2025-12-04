// Copyright 2025 Supabase, Inc.
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

package topoclient

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// LockOperation represents the type of lock operation being performed.
type LockOperation string

const (
	LockOpLock            LockOperation = "lock"
	LockOpLockWithTTL     LockOperation = "lock_with_ttl"
	LockOpLockName        LockOperation = "lock_name"
	LockOpLockNameWithTTL LockOperation = "lock_name_with_ttl"
	LockOpTryLock         LockOperation = "try_lock"
	LockOpTryLockName     LockOperation = "try_lock_name"
	LockOpUnlock          LockOperation = "unlock"
)

// LockResult represents the outcome of a lock operation.
type LockResult string

const (
	LockResultSuccess LockResult = "success"
	LockResultError   LockResult = "error"
	LockResultTimeout LockResult = "timeout"
)

// Metrics holds all OpenTelemetry metrics for the topo package.
type Metrics struct {
	meter        metric.Meter
	lockDuration metric.Float64Histogram
}

// metrics is the singleton instance of Metrics for the topo package.
var metrics *Metrics

func init() {
	metrics = newMetrics()
}

// newMetrics initializes OpenTelemetry metrics for the topo package.
func newMetrics() *Metrics {
	m := &Metrics{
		meter: otel.Meter("github.com/multigres/multigres/go/common/topoclient"),
	}

	var err error

	// Histogram for lock operation duration (count can be derived from this)
	m.lockDuration, err = m.meter.Float64Histogram(
		"topoclient.lock.duration",
		metric.WithDescription("Duration of topo lock operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		m.lockDuration = noop.Float64Histogram{}
	}

	return m
}

// RecordLockOperation records a lock operation with its result and duration.
func RecordLockOperation(ctx context.Context, op LockOperation, resourceType, resourceName string, result LockResult, duration time.Duration) {
	attrs := []attribute.KeyValue{
		attribute.String("operation", string(op)),
		attribute.String("resource_type", resourceType),
		attribute.String("result", string(result)),
	}

	metrics.lockDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}
