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
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// GRPCNotificationManager implements sqltypes.NotificationManager by calling
// the pooler's StreamNotifications gRPC. It manages per-channel streams
// and fans out notifications to subscriber channels.
//
// NOTE: Currently opens one gRPC stream per unique PG channel. If the number
// of distinct channels grows large, consider consolidating into a single
// bidirectional stream per gateway-pooler pair with dynamic channel add/remove.
// The StreamNotifications RPC already accepts repeated channels, so the
// multipooler side supports multi-channel subscriptions on a single stream.
// HTTP/2 multiplexes all streams over one TCP connection, but each stream
// still requires a goroutine, so this may need revisiting at scale.
type GRPCNotificationManager struct {
	getClient func() multipoolerpb.MultiPoolerServiceClient
	logger    *slog.Logger
	metrics   *NotificationMetrics

	mu sync.Mutex
	// channels tracks: pgChannel -> list of subscriber notifCh
	channels map[string][]chan *sqltypes.Notification
	// streams tracks: pgChannel -> cancel func for the gRPC stream
	streams map[string]context.CancelFunc
}

// NewGRPCNotificationManager creates a notification manager backed by gRPC.
func NewGRPCNotificationManager(
	getClient func() multipoolerpb.MultiPoolerServiceClient,
	logger *slog.Logger,
	metrics *NotificationMetrics,
) *GRPCNotificationManager {
	return &GRPCNotificationManager{
		getClient: getClient,
		logger:    logger,
		metrics:   metrics,
		channels:  make(map[string][]chan *sqltypes.Notification),
		streams:   make(map[string]context.CancelFunc),
	}
}

// Subscribe registers notifCh to receive notifications for pgChannel.
func (m *GRPCNotificationManager) Subscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
	var ready chan struct{}

	m.mu.Lock()
	m.channels[pgChannel] = append(m.channels[pgChannel], notifCh)

	// If this is the first subscriber for this channel, start a gRPC stream.
	if len(m.channels[pgChannel]) == 1 {
		//nolint:gocritic // Long-lived gRPC stream for notification fan-out, not tied to any request.
		ctx, cancel := context.WithCancel(context.Background())
		m.streams[pgChannel] = cancel
		m.metrics.StreamAdd(ctx)
		ready = make(chan struct{})
		go m.streamNotifications(ctx, pgChannel, ready)
	}
	m.mu.Unlock()

	// Wait for the stream to be established before returning,
	// so that a subsequent NOTIFY will be captured.
	// This wait happens outside the lock to avoid the unlock/relock race window.
	if ready != nil {
		<-ready
	}
}

// Unsubscribe removes notifCh from pgChannel subscribers.
func (m *GRPCNotificationManager) Unsubscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs := m.channels[pgChannel]
	for i, ch := range subs {
		if ch == notifCh {
			m.channels[pgChannel] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	// If no more subscribers, cancel the gRPC stream.
	if len(m.channels[pgChannel]) == 0 {
		delete(m.channels, pgChannel)
		if cancel, ok := m.streams[pgChannel]; ok {
			cancel()
			delete(m.streams, pgChannel)
			//nolint:gocritic // Metric recording at unsubscribe time, no request context available.
			m.metrics.StreamRemove(context.Background())
		}
	}
}

// UnsubscribeAll removes notifCh from all channels.
func (m *GRPCNotificationManager) UnsubscribeAll(notifCh chan *sqltypes.Notification) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for pgChannel, subs := range m.channels {
		for i, ch := range subs {
			if ch == notifCh {
				m.channels[pgChannel] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		if len(m.channels[pgChannel]) == 0 {
			delete(m.channels, pgChannel)
			if cancel, ok := m.streams[pgChannel]; ok {
				cancel()
				delete(m.streams, pgChannel)
				//nolint:gocritic // Metric recording at unsubscribe time, no request context available.
				m.metrics.StreamRemove(context.Background())
			}
		}
	}
}

// streamNotifications opens a StreamNotifications gRPC stream and fans out
// notifications to all subscribers for the given channel. On stream failure,
// it retries with backoff until the context is cancelled (last subscriber left).
func (m *GRPCNotificationManager) streamNotifications(ctx context.Context, pgChannel string, ready chan struct{}) {
	firstAttempt := true

	for {
		err := m.runStream(ctx, pgChannel, firstAttempt, ready)
		if firstAttempt {
			firstAttempt = false
		}
		if ctx.Err() != nil {
			return // cancelled — no more subscribers
		}
		if err != nil {
			m.logger.ErrorContext(ctx, "notification stream failed, will retry",
				"channel", pgChannel, "error", err)
		}

		// Backoff before retry.
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// runStream establishes a single gRPC stream and processes notifications until
// an error occurs or the context is cancelled. On the first attempt, it signals
// the ready channel after the stream is established.
func (m *GRPCNotificationManager) runStream(
	ctx context.Context, pgChannel string, firstAttempt bool, ready chan struct{},
) error {
	client := m.getClient()
	if client == nil {
		if firstAttempt {
			close(ready)
		}
		return errNoClient
	}

	stream, err := client.StreamNotifications(ctx, &multipoolerpb.StreamNotificationsRequest{
		Channels: []string{pgChannel},
	})
	if err != nil {
		if firstAttempt {
			close(ready)
		}
		return err
	}

	// Wait for the ready signal (empty first message) from the pooler.
	// This ensures LISTEN is active on PG before the gateway returns LISTEN OK to the client.
	if _, err := stream.Recv(); err != nil {
		if firstAttempt {
			close(ready)
		}
		return err
	}
	if firstAttempt {
		close(ready)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		if resp.Notification == nil {
			continue
		}
		notif := &sqltypes.Notification{
			PID:     resp.Notification.Pid,
			Channel: resp.Notification.Channel,
			Payload: resp.Notification.Payload,
		}

		m.mu.Lock()
		for _, ch := range m.channels[pgChannel] {
			select {
			case ch <- notif:
			default:
				m.logger.WarnContext(ctx, "notification channel full", "channel", pgChannel)
				m.metrics.NotificationDropped(ctx)
			}
		}
		m.mu.Unlock()
	}
}

// errNoClient is a sentinel error for when no gRPC client is available.
var errNoClient = errors.New("no gRPC client available for notifications")
