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

package plancache

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// CacheMetrics holds OTel metrics for plan cache observability.
type CacheMetrics struct {
	hits   metric.Int64Counter
	misses metric.Int64Counter
}

// NewCacheMetrics initialises OTel metrics for the plan cache.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewCacheMetrics() (*CacheMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/plancache")
	m := &CacheMetrics{}
	var errs []error

	hits, err := meter.Int64Counter(
		"mg.plancache.hits",
		metric.WithDescription("Total number of plan cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.plancache.hits counter: %w", err))
		m.hits = noop.Int64Counter{}
	} else {
		m.hits = hits
	}

	misses, err := meter.Int64Counter(
		"mg.plancache.misses",
		metric.WithDescription("Total number of plan cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.plancache.misses counter: %w", err))
		m.misses = noop.Int64Counter{}
	} else {
		m.misses = misses
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// RecordHit records a plan cache hit.
func (m *CacheMetrics) RecordHit(ctx context.Context) {
	m.hits.Add(ctx, 1)
}

// RecordMiss records a plan cache miss.
func (m *CacheMetrics) RecordMiss(ctx context.Context) {
	m.misses.Add(ctx, 1)
}
