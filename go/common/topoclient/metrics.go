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
	"errors"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/pb/mtrpc"
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

// Operation is the topo connection operation recorded by topoclient.operation.duration.
type Operation string

const (
	OpListDir             Operation = "list_dir"
	OpCreate              Operation = "create"
	OpUpdate              Operation = "update"
	OpGet                 Operation = "get"
	OpGetVersion          Operation = "get_version"
	OpList                Operation = "list"
	OpDelete              Operation = "delete"
	OpPutEphemeral        Operation = "put_ephemeral"
	OpClaimEphemeral      Operation = "claim_ephemeral"
	OpLock                Operation = "lock"
	OpLockWithTTL         Operation = "lock_with_ttl"
	OpLockName            Operation = "lock_name"
	OpLockNameWithTTL     Operation = "lock_name_with_ttl"
	OpTryLock             Operation = "try_lock"
	OpTryLockName         Operation = "try_lock_name"
	OpTryLockWithLease    Operation = "try_lock_with_lease"
	OpRevokeLockWithLease Operation = "revoke_lock_with_lease"
	OpWatch               Operation = "watch"
	OpWatchRecursive      Operation = "watch_recursive"
)

// OperationResult is the outcome of a topo operation.
type OperationResult string

const (
	OpResultSuccess OperationResult = "success"
	// OpResultExpectedError covers benign outcomes such as not found or version conflicts.
	OpResultExpectedError OperationResult = "expected_error"
	OpResultTimeout       OperationResult = "timeout"
	OpResultError         OperationResult = "error"
)

// ConnectionFailureReason describes why a topo connection was considered failed.
type ConnectionFailureReason string

const (
	ConnFailNoConnection     ConnectionFailureReason = "no_connection"
	ConnFailDeadlineExceeded ConnectionFailureReason = "deadline_exceeded"
	ConnFailCanceled         ConnectionFailureReason = "canceled"
	ConnFailUnavailable      ConnectionFailureReason = "unavailable"
	ConnFailReconnectFailed  ConnectionFailureReason = "reconnect_failed"
)

// RegistrationPhase identifies where a component registration failure happened.
type RegistrationPhase string

const (
	RegistrationPhaseInitial    RegistrationPhase = "initial"
	RegistrationPhaseRetry      RegistrationPhase = "retry"
	RegistrationPhaseReassert   RegistrationPhase = "reassert"
	RegistrationPhaseUnregister RegistrationPhase = "unregister"
)

// WatchReconnectReason describes why a watch loop had to reconnect.
type WatchReconnectReason string

const (
	WatchReconnectConnectionFailed WatchReconnectReason = "connection_failed"
	WatchReconnectWatchStartFailed WatchReconnectReason = "watch_start_failed"
	WatchReconnectStreamEnded      WatchReconnectReason = "stream_ended"
)

// classifyResult maps an operation error to a bounded result label.
func classifyResult(err error) OperationResult {
	if err == nil {
		return OpResultSuccess
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, &TopoError{Code: Timeout}) ||
		mterrors.Code(err) == mtrpc.Code_DEADLINE_EXCEEDED || status.Code(err) == codes.DeadlineExceeded {
		return OpResultTimeout
	}
	if errors.Is(err, context.Canceled) || mterrors.Code(err) == mtrpc.Code_CANCELED || status.Code(err) == codes.Canceled {
		return OpResultExpectedError
	}
	for _, code := range []ErrorCode{NoNode, NodeExists, BadVersion, NoUpdateNeeded, Interrupted} {
		if errors.Is(err, &TopoError{Code: code}) {
			return OpResultExpectedError
		}
	}
	return OpResultError
}

// Metrics holds all OpenTelemetry metrics for the topo package.
type Metrics struct {
	meter                metric.Meter
	lockDuration         metric.Float64Histogram
	operationDuration    metric.Float64Histogram
	connectionFailures   metric.Int64Counter
	registrationFailures metric.Int64Counter
	watchReconnects      metric.Int64Counter
	leaseLost            metric.Int64Counter
}

// metricsPtr holds the singleton instance of Metrics for the topo package.
// It is atomic so tests can swap it while background goroutines record.
var metricsPtr atomic.Pointer[Metrics]

func metrics() *Metrics { return metricsPtr.Load() }

func init() {
	metricsPtr.Store(newMetrics())
}

// newMetrics initializes OpenTelemetry metrics for the topo package.
func newMetrics() *Metrics {
	return newMetricsWithMeter(otel.Meter("github.com/multigres/multigres/go/common/topoclient"))
}

func newMetricsWithMeter(meter metric.Meter) *Metrics {
	m := &Metrics{meter: meter}

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

	m.operationDuration, err = m.meter.Float64Histogram(
		"topoclient.operation.duration",
		metric.WithDescription("Duration of topo connection operations"),
		metric.WithUnit("s"),
	)
	if err != nil {
		m.operationDuration = noop.Float64Histogram{}
	}

	m.connectionFailures, err = m.meter.Int64Counter(
		"topoclient.connection.failures",
		metric.WithDescription("Number of topo reconnects started, by the reason the connection was considered failed"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		m.connectionFailures = noop.Int64Counter{}
	}

	m.registrationFailures, err = m.meter.Int64Counter(
		"topoclient.registration.failures",
		metric.WithDescription("Number of failed component registration attempts in the topology"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		m.registrationFailures = noop.Int64Counter{}
	}

	m.watchReconnects, err = m.meter.Int64Counter(
		"topoclient.watch.reconnects",
		metric.WithDescription("Number of topo watch reconnects"),
		metric.WithUnit("{reconnect}"),
	)
	if err != nil {
		m.watchReconnects = noop.Int64Counter{}
	}

	m.leaseLost, err = m.meter.Int64Counter(
		"topoclient.lease.lost",
		metric.WithDescription("Number of leases lost while held"),
		metric.WithUnit("{lease}"),
	)
	if err != nil {
		m.leaseLost = noop.Int64Counter{}
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

	metrics().lockDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// recordOperation records the duration and result of a raw topo conn call.
func recordOperation(ctx context.Context, op Operation, start time.Time, err error) {
	metrics().operationDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		attribute.String("operation", string(op)),
		attribute.String("result", string(classifyResult(err))),
	))
}

func recordConnectionFailure(ctx context.Context, reason ConnectionFailureReason) {
	metrics().connectionFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", string(reason))))
}

// RecordRegistrationFailure records a failed component registration attempt.
func RecordRegistrationFailure(ctx context.Context, phase RegistrationPhase) {
	metrics().registrationFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", string(phase))))
}

func recordWatchReconnect(ctx context.Context, reason WatchReconnectReason) {
	metrics().watchReconnects.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", string(reason))))
}

func recordLeaseLost(ctx context.Context) {
	metrics().leaseLost.Add(ctx, 1)
}
