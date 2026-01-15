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

package connpoolmanager

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/multigres/multigres/go/multipooler/pools/connpool"
)

// Metrics holds OpenTelemetry metrics for connection pool management.
type Metrics struct {
	// regularConnCount tracks PostgreSQL connection states for regular pools
	regularConnCount connpool.ConnectionCount

	// reservedConnCount tracks PostgreSQL connection states for reserved pools
	reservedConnCount connpool.ConnectionCount
}

// NewMetrics initializes OpenTelemetry metrics for connection pool management.
// Individual metrics that fail to initialize will use noop implementations and be included
// in the returned error. The returned Metrics instance is always usable (with noop fallbacks
// for failed metrics), and the error indicates which specific metrics failed to initialize.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/multipooler/connpoolmanager")

	m := &Metrics{}

	var errs []error

	// ConnectionCount for regular pools
	regularCount, err := connpool.NewConnectionCount(meter)
	if err != nil {
		errs = append(errs, fmt.Errorf("regular pool ConnectionCount: %w", err))
		m.regularConnCount = connpool.ConnectionCount{} // Use zero value (noop) on error
	} else {
		m.regularConnCount = regularCount
	}

	// ConnectionCount for reserved pools
	reservedCount, err := connpool.NewConnectionCount(meter)
	if err != nil {
		errs = append(errs, fmt.Errorf("reserved pool ConnectionCount: %w", err))
		m.reservedConnCount = connpool.ConnectionCount{} // Use zero value (noop) on error
	} else {
		m.reservedConnCount = reservedCount
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}

	return m, nil
}

// RegularConnCount returns the ConnectionCount metric for regular pools.
func (m *Metrics) RegularConnCount() connpool.ConnectionCount {
	return m.regularConnCount
}

// ReservedConnCount returns the ConnectionCount metric for reserved pools.
func (m *Metrics) ReservedConnCount() connpool.ConnectionCount {
	return m.reservedConnCount
}
