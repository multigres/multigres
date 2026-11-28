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

package schematracker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const meterName = "github.com/multigres/multigres/go/services/multipooler/internal/schematracker"

// metrics counts token changes and failed computations.
type metrics struct {
	changes metric.Int64Counter
	errors  metric.Int64Counter
}

// newMetrics creates the tracker's instruments. One that fails to initialise
// falls back to noop, and the failure is logged.
func newMetrics(logger *slog.Logger) *metrics {
	meter := otel.Meter(meterName)
	m := &metrics{}
	var errs []error
	var err error

	m.changes, err = meter.Int64Counter(
		"mg.pooler.schema_token.changes",
		metric.WithDescription("Number of times the schema token changed, i.e. a schema change was detected"),
	)
	if err != nil {
		m.changes = noop.Int64Counter{}
		errs = append(errs, fmt.Errorf("mg.pooler.schema_token.changes: %w", err))
	}

	m.errors, err = meter.Int64Counter(
		"mg.pooler.schema_token.errors",
		metric.WithDescription("Number of schema token computations that failed"),
	)
	if err != nil {
		m.errors = noop.Int64Counter{}
		errs = append(errs, fmt.Errorf("mg.pooler.schema_token.errors: %w", err))
	}

	if err := errors.Join(errs...); err != nil {
		logger.Warn("failed to initialise some schema tracker metrics", "error", err)
	}
	return m
}

func (m *metrics) recordChange(ctx context.Context) { m.changes.Add(ctx, 1) }

func (m *metrics) recordError(ctx context.Context) { m.errors.Add(ctx, 1) }
