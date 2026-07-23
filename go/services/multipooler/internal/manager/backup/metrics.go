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

package backup

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/manager"

// Metrics holds OTel counter instruments for pgBackRest backup outcomes.
type Metrics struct {
	backupAttempts  metric.Int64Counter
	backupSuccesses metric.Int64Counter
	backupFailures  metric.Int64Counter

	restoreAttempts  metric.Int64Counter
	restoreSuccesses metric.Int64Counter
	restoreFailures  metric.Int64Counter

	leaseLost metric.Int64Counter

	backupDuration       metric.Float64Histogram
	backupVerifyDuration metric.Float64Histogram
	restoreDuration      metric.Float64Histogram
	backupLockWait       metric.Float64Histogram

	// Observable gauges, registered against a HealthTracker via
	// RegisterHealthCallback.
	lastSuccessAge       metric.Float64ObservableGauge
	completeCount        metric.Int64ObservableGauge
	failuresSinceSuccess metric.Int64ObservableGauge
	ready                metric.Int64ObservableGauge
	walArchiveLag        metric.Float64ObservableGauge
	inProgress           metric.Int64ObservableGauge
	inProgressDuration   metric.Float64ObservableGauge
	leaseHeld            metric.Int64ObservableGauge
}

// NewMetrics creates and registers the pgBackRest backup counters and declares
// the HealthTracker-backed observable gauges. It always returns a non-nil
// *Metrics; any registration errors are collected and returned. The gauges are
// not observed until RegisterHealthCallback wires them to a tracker.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{}
	meter := otel.Meter(meterName)

	var errs []error

	var err error
	m.backupAttempts, err = meter.Int64Counter(
		"pgbackrest.backup.attempts",
		metric.WithDescription("Total number of backup attempts"),
	)
	errs = append(errs, err)

	m.backupSuccesses, err = meter.Int64Counter(
		"pgbackrest.backup.successes",
		metric.WithDescription("Total number of successful backups"),
	)
	errs = append(errs, err)

	m.backupFailures, err = meter.Int64Counter(
		"pgbackrest.backup.failures",
		metric.WithDescription("Total number of failed backups"),
	)
	errs = append(errs, err)

	m.restoreAttempts, err = meter.Int64Counter(
		"pgbackrest.restore.attempts",
		metric.WithDescription("Total number of restore attempts"),
	)
	errs = append(errs, err)

	m.restoreSuccesses, err = meter.Int64Counter(
		"pgbackrest.restore.successes",
		metric.WithDescription("Total number of successful restores"),
	)
	errs = append(errs, err)

	m.restoreFailures, err = meter.Int64Counter(
		"pgbackrest.restore.failures",
		metric.WithDescription("Total number of failed restores"),
	)
	errs = append(errs, err)

	m.leaseLost, err = meter.Int64Counter(
		"pgbackrest.backup.lease.lost",
		metric.WithDescription("Total number of times this pooler lost the backup lease mid-operation (stolen or expired)"),
	)
	errs = append(errs, err)

	m.backupDuration, err = meter.Float64Histogram(
		"pgbackrest.backup.duration",
		metric.WithDescription("Duration of the pgbackrest backup command in seconds"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.backupVerifyDuration, err = meter.Float64Histogram(
		"pgbackrest.backup.verify.duration",
		metric.WithDescription("Duration of the pgbackrest verify command in seconds"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.restoreDuration, err = meter.Float64Histogram(
		"pgbackrest.restore.duration",
		metric.WithDescription("Duration of the full restore operation in seconds"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.backupLockWait, err = meter.Float64Histogram(
		"pgbackrest.backup.lock_wait",
		metric.WithDescription("Time spent waiting for the action lock before backup in seconds"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.lastSuccessAge, err = meter.Float64ObservableGauge(
		"pgbackrest.backup.last_success_age_seconds",
		metric.WithDescription("Seconds since the newest COMPLETE backup finished; not emitted if no backup exists"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.completeCount, err = meter.Int64ObservableGauge(
		"pgbackrest.backup.complete_count",
		metric.WithDescription("Number of COMPLETE backups visible in the repo"),
	)
	errs = append(errs, err)

	m.failuresSinceSuccess, err = meter.Int64ObservableGauge(
		"pgbackrest.backup.failures_since_success",
		metric.WithDescription("Consecutive backup failures since the last success"),
	)
	errs = append(errs, err)

	m.ready, err = meter.Int64ObservableGauge(
		"pgbackrest.backup.ready",
		metric.WithDescription("1 if backups can run (repo reachable, stanza present, archiving configured & healthy), 0 otherwise; carries a 'reason' attribute"),
	)
	errs = append(errs, err)

	m.walArchiveLag, err = meter.Float64ObservableGauge(
		"pgbackrest.wal.archive_lag_seconds",
		metric.WithDescription("Seconds since the last WAL segment was archived (primary only); not emitted if unknown"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.inProgress, err = meter.Int64ObservableGauge(
		"pgbackrest.backup.in_progress",
		metric.WithDescription("1 if a backup is currently running, 0 otherwise"),
	)
	errs = append(errs, err)

	m.inProgressDuration, err = meter.Float64ObservableGauge(
		"pgbackrest.backup.in_progress_duration_seconds",
		metric.WithDescription("Seconds the in-progress backup has been running, 0 if none"),
		metric.WithUnit("s"),
	)
	errs = append(errs, err)

	m.leaseHeld, err = meter.Int64ObservableGauge(
		"pgbackrest.backup.lease_held",
		metric.WithDescription("1 if this pooler currently holds the backup lease, 0 otherwise"),
	)
	errs = append(errs, err)

	return m, errors.Join(errs...)
}

// RegisterHealthCallback wires the observable gauges to tracker, so each scrape
// reads a single consistent Snapshot. It is split from NewMetrics so the
// tracker is supplied after construction (and captured by the callback closure
// rather than stored on Metrics). Safe to call on a nil receiver.
func (m *Metrics) RegisterHealthCallback(tracker *HealthTracker) error {
	if m == nil {
		return nil
	}
	meter := otel.Meter(meterName)
	_, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			// One consistent read; derive ages from timestamps at observe
			// time so they stay accurate between polls.
			snap := tracker.Snapshot()
			now := time.Now()

			if !snap.LastSuccessStop.IsZero() {
				o.ObserveFloat64(m.lastSuccessAge, now.Sub(snap.LastSuccessStop).Seconds())
			}
			o.ObserveInt64(m.completeCount, snap.CompleteCount)
			o.ObserveInt64(m.failuresSinceSuccess, snap.FailuresSinceSuccess)

			readyVal := int64(0)
			if snap.Ready {
				readyVal = 1
			}
			o.ObserveInt64(m.ready, readyVal, metric.WithAttributes(attribute.String("reason", snap.Reason)))

			if !snap.LastArchived.IsZero() {
				o.ObserveFloat64(m.walArchiveLag, now.Sub(snap.LastArchived).Seconds())
			}

			inProgressVal := int64(0)
			var inProgressDur float64
			if !snap.InProgressStart.IsZero() {
				inProgressVal = 1
				inProgressDur = now.Sub(snap.InProgressStart).Seconds()
			}
			o.ObserveInt64(m.inProgress, inProgressVal)
			o.ObserveFloat64(m.inProgressDuration, inProgressDur)

			leaseVal := int64(0)
			if snap.LeaseHeld {
				leaseVal = 1
			}
			o.ObserveInt64(m.leaseHeld, leaseVal)
			return nil
		},
		m.lastSuccessAge, m.completeCount, m.failuresSinceSuccess, m.ready,
		m.walArchiveLag, m.inProgress, m.inProgressDuration, m.leaseHeld,
	)
	return err
}

// IncBackupAttempts increments the backup attempts counter.
// Safe to call on a nil receiver or with a nil instrument.
func (m *Metrics) IncBackupAttempts(ctx context.Context) {
	if m != nil && m.backupAttempts != nil {
		m.backupAttempts.Add(ctx, 1)
	}
}

// IncBackupSuccesses increments the backup successes counter.
// Safe to call on a nil receiver or with a nil instrument.
func (m *Metrics) IncBackupSuccesses(ctx context.Context) {
	if m != nil && m.backupSuccesses != nil {
		m.backupSuccesses.Add(ctx, 1)
	}
}

// IncBackupFailures increments the backup failures counter.
// Safe to call on a nil receiver or with a nil instrument.
func (m *Metrics) IncBackupFailures(ctx context.Context) {
	if m != nil && m.backupFailures != nil {
		m.backupFailures.Add(ctx, 1)
	}
}

// IncLeaseLost increments the backup lease-lost counter.
// Safe to call on a nil receiver or with a nil instrument.
func (m *Metrics) IncLeaseLost(ctx context.Context) {
	if m != nil && m.leaseLost != nil {
		m.leaseLost.Add(ctx, 1)
	}
}

func (m *Metrics) IncRestoreAttempts(ctx context.Context) {
	if m != nil && m.restoreAttempts != nil {
		m.restoreAttempts.Add(ctx, 1)
	}
}

func (m *Metrics) IncRestoreSuccesses(ctx context.Context) {
	if m != nil && m.restoreSuccesses != nil {
		m.restoreSuccesses.Add(ctx, 1)
	}
}

func (m *Metrics) IncRestoreFailures(ctx context.Context) {
	if m != nil && m.restoreFailures != nil {
		m.restoreFailures.Add(ctx, 1)
	}
}

func (m *Metrics) RecordBackupDuration(ctx context.Context, seconds float64) {
	if m != nil && m.backupDuration != nil {
		m.backupDuration.Record(ctx, seconds)
	}
}

func (m *Metrics) RecordBackupVerifyDuration(ctx context.Context, seconds float64) {
	if m != nil && m.backupVerifyDuration != nil {
		m.backupVerifyDuration.Record(ctx, seconds)
	}
}

func (m *Metrics) RecordRestoreDuration(ctx context.Context, seconds float64) {
	if m != nil && m.restoreDuration != nil {
		m.restoreDuration.Record(ctx, seconds)
	}
}

func (m *Metrics) RecordBackupLockWait(ctx context.Context, seconds float64) {
	if m != nil && m.backupLockWait != nil {
		m.backupLockWait.Record(ctx, seconds)
	}
}
