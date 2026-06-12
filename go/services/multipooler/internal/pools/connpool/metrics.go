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

package connpool

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.37.0/dbconv"
)

// Attribute keys from OTel semantic conventions:
// - semconv.DBClientConnectionPoolNameKey = "db.client.connection.pool.name"
// - semconv.DBClientConnectionStateKey = "db.client.connection.state"
const (
	attrKeyPoolName = "db.client.connection.pool.name"
	attrKeyState    = "db.client.connection.state"
)

// ConnectionCount wraps an Int64UpDownCounter for tracking connection counts by state.
// This is a workaround for a bug in dbconv.ClientConnectionCount where pool name and
// state attributes aren't added when no extra attributes are provided.
// Consider contributing a fix upstream to opentelemetry-go/semconv/v1.37.0/dbconv.
type ConnectionCount struct {
	counter metric.Int64UpDownCounter
}

// NewConnectionCount creates a ConnectionCount instrument using the standard
// db.client.connection.count metric name and description from OTel semconv.
func NewConnectionCount(m metric.Meter) (ConnectionCount, error) {
	// Metric name and description from dbconv.ClientConnectionCount
	counter, err := m.Int64UpDownCounter(
		"db.client.connection.count",
		metric.WithDescription("The number of connections that are currently in state described by the state attribute."),
		metric.WithUnit("{connection}"),
	)
	return ConnectionCount{counter: counter}, err
}

// Add records a connection count change for the given pool and state.
func (c ConnectionCount) Add(ctx context.Context, delta int64, poolName string, state dbconv.ClientConnectionStateAttr) {
	if c.counter == nil {
		return
	}
	c.counter.Add(ctx, delta, metric.WithAttributes(
		attribute.String(attrKeyPoolName, poolName),
		attribute.String(attrKeyState, string(state)),
	))
}
