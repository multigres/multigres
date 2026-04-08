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

package manager

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/manager"

// Metrics holds OTel counter instruments for pgBackRest backup outcomes.
type Metrics struct {
	backupAttempts  metric.Int64Counter
	backupSuccesses metric.Int64Counter
	backupFailures  metric.Int64Counter

	restoreAttempts  metric.Int64Counter
	restoreSuccesses metric.Int64Counter
	restoreFailures  metric.Int64Counter

	backupDuration       metric.Float64Histogram
	backupVerifyDuration metric.Float64Histogram
	restoreDuration      metric.Float64Histogram
	backupLockWait       metric.Float64Histogram
}

// NewMetrics creates and registers the pgBackRest backup counters.
// It always returns a non-nil *Metrics; any registration errors are collected and returned.
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

	return m, errors.Join(errs...)
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
