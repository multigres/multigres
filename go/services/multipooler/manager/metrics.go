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
}

// NewMetrics creates and registers the pgBackRest backup counters.
// It always returns a non-nil *Metrics; any registration errors are collected and returned.
func NewMetrics() (*Metrics, error) {
	m := &Metrics{}
	meter := otel.Meter(meterName)

	var errs []error

	var err error
	m.backupAttempts, err = meter.Int64Counter(
		"pgbackrest_backup_attempts_total",
		metric.WithDescription("Total number of backup attempts"),
	)
	errs = append(errs, err)

	m.backupSuccesses, err = meter.Int64Counter(
		"pgbackrest_backup_successes_total",
		metric.WithDescription("Total number of successful backups"),
	)
	errs = append(errs, err)

	m.backupFailures, err = meter.Int64Counter(
		"pgbackrest_backup_failures_total",
		metric.WithDescription("Total number of failed backups"),
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
