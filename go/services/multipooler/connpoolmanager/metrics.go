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
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
)

// Metrics holds OpenTelemetry metrics for connection pool management.
type Metrics struct {
	meter metric.Meter

	// regularConnCount tracks PostgreSQL connection states for regular pools
	regularConnCount connpool.ConnectionCount

	// reservedConnCount tracks PostgreSQL connection states for reserved pools
	reservedConnCount connpool.ConnectionCount

	// --- Observable gauges for PgBouncer-equivalent metrics ---

	// poolerUp reports whether the pooler is operational (1 = up, 0 = down).
	poolerUp metric.Int64ObservableGauge

	// poolCount is the number of user connection pools.
	poolCount metric.Int64ObservableGauge

	// userCount is the number of distinct users with pools.
	userCount metric.Int64ObservableGauge

	// databaseCount is the number of databases (always 1 per multipooler instance).
	databaseCount metric.Int64ObservableGauge

	// serverConnections is the number of server connections by state (active/idle).
	serverConnections metric.Int64ObservableGauge

	// clientWaitingConnections is the number of clients waiting for a server connection.
	clientWaitingConnections metric.Int64ObservableGauge

	// reservedActiveConnections is the number of active reserved connections (in-transaction).
	reservedActiveConnections metric.Int64ObservableGauge

	// configMaxServerConnections is the configured maximum server connections (global capacity).
	configMaxServerConnections metric.Int64ObservableGauge

	// poolCapacity is the per-user allocated pool capacity (regular + reserved).
	poolCapacity metric.Int64ObservableGauge

	// poolCurrentConnections is the per-user active server connections (regular + reserved).
	poolCurrentConnections metric.Int64ObservableGauge

	// --- Observable counters for cumulative pool traffic metrics ---

	// clientWaitTimeTotal is the cumulative time clients spent waiting for a server connection.
	clientWaitTimeTotal metric.Float64ObservableCounter

	// queriesPooledTotal is the total number of connections borrowed (Get() calls).
	queriesPooledTotal metric.Int64ObservableCounter
}

// NewMetrics initializes OpenTelemetry metrics for connection pool management.
// Individual metrics that fail to initialize will use noop implementations and be included
// in the returned error. The returned Metrics instance is always usable (with noop fallbacks
// for failed metrics), and the error indicates which specific metrics failed to initialize.
func NewMetrics() (*Metrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multipooler/connpoolmanager")

	m := &Metrics{meter: meter}

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

	// Pooler health gauge
	m.poolerUp, err = meter.Int64ObservableGauge(
		"mg.pooler.up",
		metric.WithDescription("Whether the connection pooler is operational (1 = up, 0 = down)"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.up gauge: %w", err))
	}

	// Pool count gauge
	m.poolCount, err = meter.Int64ObservableGauge(
		"mg.pooler.pools",
		metric.WithDescription("Number of active connection pools"),
		metric.WithUnit("{pool}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.pools gauge: %w", err))
	}

	// User count gauge
	m.userCount, err = meter.Int64ObservableGauge(
		"mg.pooler.users",
		metric.WithDescription("Number of distinct users with connection pools"),
		metric.WithUnit("{user}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.users gauge: %w", err))
	}

	// Database count gauge
	m.databaseCount, err = meter.Int64ObservableGauge(
		"mg.pooler.databases",
		metric.WithDescription("Number of databases managed by the pooler"),
		metric.WithUnit("{database}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.databases gauge: %w", err))
	}

	// Server connections gauge (with state attribute)
	m.serverConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.server.connections",
		metric.WithDescription("Number of server (PostgreSQL) connections by state"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.server.connections gauge: %w", err))
	}

	// Client waiting connections gauge
	m.clientWaitingConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.client.waiting_connections",
		metric.WithDescription("Number of clients waiting for a server connection"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.client.waiting_connections gauge: %w", err))
	}

	// Reserved active connections gauge
	m.reservedActiveConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.reserved.active_connections",
		metric.WithDescription("Number of active reserved connections (clients in transactions)"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.reserved.active_connections gauge: %w", err))
	}

	// Config max server connections gauge
	m.configMaxServerConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.config.max_server_connections",
		metric.WithDescription("Configured maximum number of server connections (global capacity)"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.config.max_server_connections gauge: %w", err))
	}

	// Per-user pool capacity gauge
	m.poolCapacity, err = meter.Int64ObservableGauge(
		"mg.pooler.pool.capacity",
		metric.WithDescription("Allocated pool capacity per user (regular + reserved)"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.pool.capacity gauge: %w", err))
	}

	// Per-user current connections gauge
	m.poolCurrentConnections, err = meter.Int64ObservableGauge(
		"mg.pooler.pool.current_connections",
		metric.WithDescription("Current server connections per user (regular + reserved)"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.pool.current_connections gauge: %w", err))
	}

	// Client wait time counter
	m.clientWaitTimeTotal, err = meter.Float64ObservableCounter(
		"mg.pooler.client.wait_time_total",
		metric.WithDescription("Total time clients spent waiting for a server connection"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.client.wait_time_total counter: %w", err))
	}

	// Queries pooled counter
	m.queriesPooledTotal, err = meter.Int64ObservableCounter(
		"mg.pooler.queries_pooled_total",
		metric.WithDescription("Total number of connections borrowed from pools"),
		metric.WithUnit("{query}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pooler.queries_pooled_total counter: %w", err))
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}

	return m, nil
}

// RegisterManagerCallbacks registers OTel observable callbacks that read pool statistics.
// The callbacks are invoked by the OTel SDK during metric collection.
//
// Parameters:
//   - statsGetter: returns a snapshot of all pool statistics
//   - poolCountGetter: returns the number of active user pools
//   - globalCapacityGetter: returns the configured global connection capacity
//   - isClosedGetter: returns whether the manager is closed
func (m *Metrics) RegisterManagerCallbacks(
	statsGetter func() ManagerStats,
	poolCountGetter func() int,
	globalCapacityGetter func() int64,
	isClosedGetter func() bool,
) error {
	// Collect all instruments that were successfully initialized.
	var instruments []metric.Observable
	for _, inst := range []metric.Observable{
		m.poolerUp,
		m.poolCount,
		m.userCount,
		m.databaseCount,
		m.serverConnections,
		m.clientWaitingConnections,
		m.reservedActiveConnections,
		m.configMaxServerConnections,
		m.poolCapacity,
		m.poolCurrentConnections,
		m.clientWaitTimeTotal,
		m.queriesPooledTotal,
	} {
		if inst != nil {
			instruments = append(instruments, inst)
		}
	}

	if len(instruments) == 0 {
		return nil
	}

	_, err := m.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			// Pooler health.
			if m.poolerUp != nil {
				var up int64
				if !isClosedGetter() {
					up = 1
				}
				o.ObserveInt64(m.poolerUp, up)
			}

			// Pool/user count.
			poolCount := poolCountGetter()
			if m.poolCount != nil {
				o.ObserveInt64(m.poolCount, int64(poolCount))
			}
			if m.userCount != nil {
				o.ObserveInt64(m.userCount, int64(poolCount))
			}

			// Database count (always 1 per multipooler instance).
			if m.databaseCount != nil {
				var dbCount int64
				if !isClosedGetter() {
					dbCount = 1
				}
				o.ObserveInt64(m.databaseCount, dbCount)
			}

			// Read global capacity config.
			if m.configMaxServerConnections != nil {
				o.ObserveInt64(m.configMaxServerConnections, globalCapacityGetter())
			}

			// Aggregate stats across all user pools.
			stats := statsGetter()
			var totalActive, totalIdle int64
			var totalWaiting int
			var totalReservedActive int
			var totalWaitTime float64
			var totalGetCount int64

			for user, userStats := range stats.UserPools {
				// Regular pool: server connections
				totalActive += userStats.Regular.Borrowed
				totalIdle += userStats.Regular.Idle

				// Reserved pool: underlying server connections
				totalActive += userStats.Reserved.RegularPool.Borrowed
				totalIdle += userStats.Reserved.RegularPool.Idle

				// Reserved active (clients in transactions)
				totalReservedActive += userStats.Reserved.Active

				// Aggregate cumulative metrics
				totalWaiting += userStats.Waiting
				totalWaitTime += userStats.WaitTime.Seconds()
				totalGetCount += userStats.GetCount

				// Per-user pool capacity and current connections.
				userAttr := metric.WithAttributes(attribute.String("user", user))

				if m.poolCapacity != nil {
					capacity := userStats.Regular.Capacity + userStats.Reserved.RegularPool.Capacity
					o.ObserveInt64(m.poolCapacity, capacity, userAttr)
				}
				if m.poolCurrentConnections != nil {
					active := userStats.Regular.Active + userStats.Reserved.RegularPool.Active
					o.ObserveInt64(m.poolCurrentConnections, active, userAttr)
				}
			}

			// Also include admin pool connections.
			totalActive += stats.Admin.Borrowed
			totalIdle += stats.Admin.Idle

			if m.serverConnections != nil {
				o.ObserveInt64(m.serverConnections, totalActive,
					metric.WithAttributes(attribute.String("state", "active")))
				o.ObserveInt64(m.serverConnections, totalIdle,
					metric.WithAttributes(attribute.String("state", "idle")))
			}

			if m.clientWaitingConnections != nil {
				o.ObserveInt64(m.clientWaitingConnections, int64(totalWaiting))
			}

			if m.reservedActiveConnections != nil {
				o.ObserveInt64(m.reservedActiveConnections, int64(totalReservedActive))
			}

			if m.clientWaitTimeTotal != nil {
				o.ObserveFloat64(m.clientWaitTimeTotal, totalWaitTime)
			}

			if m.queriesPooledTotal != nil {
				o.ObserveInt64(m.queriesPooledTotal, totalGetCount)
			}

			return nil
		},
		instruments...,
	)
	return err
}

// RegularConnCount returns the ConnectionCount metric for regular pools.
func (m *Metrics) RegularConnCount() connpool.ConnectionCount {
	return m.regularConnCount
}

// ReservedConnCount returns the ConnectionCount metric for reserved pools.
func (m *Metrics) ReservedConnCount() connpool.ConnectionCount {
	return m.reservedConnCount
}
