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

package pubsub

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// PubSubMetrics holds OTel metrics for the PubSub notification listener.
// All metrics are recorded from the single-threaded event loop in Listener.run(),
// so no synchronization is needed for recording.
type PubSubMetrics struct {
	channels        metric.Int64UpDownCounter
	subscribers     metric.Int64UpDownCounter
	notifDropped    metric.Int64Counter
	reconnects      metric.Int64Counter
	reconnectGapDur metric.Float64Histogram
}

// ChannelAdd increments the channel gauge when a new PG channel gets its first subscriber.
func (m *PubSubMetrics) ChannelAdd(ctx context.Context) {
	if m == nil {
		return
	}
	m.channels.Add(ctx, 1)
}

// ChannelRemove decrements the channel gauge when a PG channel loses its last subscriber.
func (m *PubSubMetrics) ChannelRemove(ctx context.Context) {
	if m == nil {
		return
	}
	m.channels.Add(ctx, -1)
}

// SubscriberAdd increments the subscriber gauge when a new unique subscriber connects.
func (m *PubSubMetrics) SubscriberAdd(ctx context.Context) {
	if m == nil {
		return
	}
	m.subscribers.Add(ctx, 1)
}

// SubscriberRemove decrements the subscriber gauge when a subscriber disconnects from all channels.
func (m *PubSubMetrics) SubscriberRemove(ctx context.Context) {
	if m == nil {
		return
	}
	m.subscribers.Add(ctx, -1)
}

// NotificationDropped increments the drop counter when a notification cannot be
// delivered because a subscriber's channel buffer is full.
func (m *PubSubMetrics) NotificationDropped(ctx context.Context) {
	if m == nil {
		return
	}
	m.notifDropped.Add(ctx, 1)
}

// Reconnect increments the reconnect counter when the PG connection is lost
// and a reconnect is scheduled.
func (m *PubSubMetrics) Reconnect(ctx context.Context) {
	if m == nil {
		return
	}
	m.reconnects.Add(ctx, 1)
}

// ReconnectGapDuration records the duration between disconnect and successful
// reconnect. This is a proxy for the notification loss window since PG
// LISTEN/NOTIFY is fire-and-forget.
func (m *PubSubMetrics) ReconnectGapDuration(ctx context.Context, durationSec float64) {
	if m == nil {
		return
	}
	m.reconnectGapDur.Record(ctx, durationSec)
}

// NewPubSubMetrics initialises OTel metrics for the PubSub listener.
// Individual metrics that fail to initialise use noop implementations
// and are included in the returned error.
func NewPubSubMetrics() (*PubSubMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multipooler/pubsub")
	m := &PubSubMetrics{}
	var errs []error

	ch, err := meter.Int64UpDownCounter(
		"mg.pubsub.channels",
		metric.WithDescription("Number of PG channels with active LISTEN"),
		metric.WithUnit("{channel}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pubsub.channels: %w", err))
		m.channels = noop.Int64UpDownCounter{}
	} else {
		m.channels = ch
	}

	sub, err := meter.Int64UpDownCounter(
		"mg.pubsub.subscribers",
		metric.WithDescription("Number of unique notification subscribers"),
		metric.WithUnit("{subscriber}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pubsub.subscribers: %w", err))
		m.subscribers = noop.Int64UpDownCounter{}
	} else {
		m.subscribers = sub
	}

	drop, err := meter.Int64Counter(
		"mg.pubsub.notifications.dropped",
		metric.WithDescription("Notifications dropped due to full subscriber channels"),
		metric.WithUnit("{notification}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pubsub.notifications.dropped: %w", err))
		m.notifDropped = noop.Int64Counter{}
	} else {
		m.notifDropped = drop
	}

	recon, err := meter.Int64Counter(
		"mg.pubsub.reconnects",
		metric.WithDescription("PG connection reconnect events"),
		metric.WithUnit("{reconnect}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pubsub.reconnects: %w", err))
		m.reconnects = noop.Int64Counter{}
	} else {
		m.reconnects = recon
	}

	gap, err := meter.Float64Histogram(
		"mg.pubsub.reconnect.gap_duration",
		metric.WithDescription("Time from disconnect to successful reconnect with re-LISTEN completion"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 2, 5, 10, 30, 60),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.pubsub.reconnect.gap_duration: %w", err))
		m.reconnectGapDur = noop.Float64Histogram{}
	} else {
		m.reconnectGapDur = gap
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}
