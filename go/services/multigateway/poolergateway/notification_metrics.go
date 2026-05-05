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

package poolergateway

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// NotificationMetrics holds OTel metrics for the gateway notification system.
type NotificationMetrics struct {
	streams      metric.Int64UpDownCounter
	notifDropped metric.Int64Counter
}

// StreamAdd increments the stream gauge when a new gRPC notification stream is opened.
func (m *NotificationMetrics) StreamAdd(ctx context.Context) {
	if m == nil {
		return
	}
	m.streams.Add(ctx, 1)
}

// StreamRemove decrements the stream gauge when a gRPC notification stream is closed.
func (m *NotificationMetrics) StreamRemove(ctx context.Context) {
	if m == nil {
		return
	}
	m.streams.Add(ctx, -1)
}

// NotificationDropped increments the drop counter when a notification cannot be
// delivered because a subscriber's channel buffer is full.
func (m *NotificationMetrics) NotificationDropped(ctx context.Context) {
	if m == nil {
		return
	}
	m.notifDropped.Add(ctx, 1)
}

// NewNotificationMetrics initialises OTel metrics for gateway notifications.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewNotificationMetrics() (*NotificationMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway/poolergateway")
	m := &NotificationMetrics{}
	var errs []error

	streams, err := meter.Int64UpDownCounter(
		"mg.gateway.notification.streams",
		metric.WithDescription("Active gRPC notification streams (one per unique PG channel)"),
		metric.WithUnit("{stream}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.notification.streams: %w", err))
		m.streams = noop.Int64UpDownCounter{}
	} else {
		m.streams = streams
	}

	drop, err := meter.Int64Counter(
		"mg.gateway.notifications.dropped",
		metric.WithDescription("Notifications dropped due to full subscriber channels"),
		metric.WithUnit("{notification}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.notifications.dropped: %w", err))
		m.notifDropped = noop.Int64Counter{}
	} else {
		m.notifDropped = drop
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
