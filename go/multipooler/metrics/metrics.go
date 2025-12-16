// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metrics provides OpenTelemetry metrics for multipooler.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
)

// Collector holds all OpenTelemetry metrics for multipooler.
type Collector struct {
	meter             metric.Meter
	rpcServerDuration RPCServerDuration
}

// RPCServerDuration wraps a Float64Histogram for recording gRPC server call durations.
// Follows OTel semantic conventions:
//   - https://opentelemetry.io/docs/specs/semconv/rpc/rpc-metrics/
//   - https://opentelemetry.io/docs/specs/semconv/rpc/grpc/
type RPCServerDuration struct {
	metric.Float64Histogram
}

// Record records a gRPC call duration following OTel semantic conventions.
func (m RPCServerDuration) Record(ctx context.Context, duration time.Duration, service, method string, statusCode codes.Code) {
	m.Float64Histogram.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("rpc.service", service),
			attribute.String("rpc.method", method),
			attribute.Int("rpc.grpc.status_code", int(statusCode)),
		))
}

// NewCollector initializes OpenTelemetry metrics for multipooler.
// Individual metrics that fail to initialize will use noop implementations and be included
// in the returned error. The caller should log or handle these errors as appropriate.
func NewCollector() (*Collector, error) {
	c := &Collector{
		meter: otel.Meter("github.com/multigres/multigres/go/multipooler"),
	}

	var errs []error

	rpcServerDurationHistogram, err := c.meter.Float64Histogram(
		"rpc.server.duration",
		metric.WithDescription("Duration of inbound gRPC calls"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("rpc.server.duration histogram: %w", err))
		c.rpcServerDuration = RPCServerDuration{noop.Float64Histogram{}}
	} else {
		c.rpcServerDuration = RPCServerDuration{rpcServerDurationHistogram}
	}

	if len(errs) > 0 {
		return c, errors.Join(errs...)
	}
	return c, nil
}

// RPCServerDuration returns the RPC server duration histogram.
func (c *Collector) RPCServerDuration() RPCServerDuration {
	return c.rpcServerDuration
}
