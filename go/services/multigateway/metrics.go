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

package multigateway

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// GatewayMetrics holds OTel metrics for the multigateway service.
type GatewayMetrics struct {
	meter             metric.Meter
	clientConnections metric.Int64ObservableGauge
}

// NewGatewayMetrics initializes OTel metrics for the multigateway service.
func NewGatewayMetrics() (*GatewayMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway")

	m := &GatewayMetrics{meter: meter}
	var errs []error

	var err error
	m.clientConnections, err = meter.Int64ObservableGauge(
		"mg.gateway.client.connections",
		metric.WithDescription("Number of active client connections to the gateway"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.client.connections gauge: %w", err))
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// RegisterClientConnectionsCallback registers a callback that reports the current
// number of active client connections.
func (m *GatewayMetrics) RegisterClientConnectionsCallback(getter func() int) error {
	if m.clientConnections == nil || getter == nil {
		return nil
	}

	_, err := m.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(m.clientConnections, int64(getter()))
			return nil
		},
		m.clientConnections,
	)
	return err
}
