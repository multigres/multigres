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
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

const notificationAckTimeout = 10 * time.Second

// GRPCNotificationManager implements handler.NotificationManager by keeping one
// ordered gRPC notification stream per gateway client session (notifCh). A
// single stream carries all channels for that session, so cross-channel
// notifications are delivered in the same order PostgreSQL emitted them.
type GRPCNotificationManager struct {
	getClient func() multipoolerpb.MultipoolerServiceClient
	logger    *slog.Logger
	metrics   *NotificationMetrics

	mu       sync.Mutex
	sessions map[chan *sqltypes.Notification]*notificationSession
}

// NewGRPCNotificationManager creates a notification manager backed by gRPC.
func NewGRPCNotificationManager(
	getClient func() multipoolerpb.MultipoolerServiceClient,
	logger *slog.Logger,
	metrics *NotificationMetrics,
) *GRPCNotificationManager {
	return &GRPCNotificationManager{
		getClient: getClient,
		logger:    logger,
		metrics:   metrics,
		sessions:  make(map[chan *sqltypes.Notification]*notificationSession),
	}
}

// Subscribe registers notifCh to receive notifications for pgChannel.
func (m *GRPCNotificationManager) Subscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
	s := m.sessionFor(notifCh)
	if err := s.update([]string{pgChannel}, nil, false); err != nil {
		m.logger.Error("notification subscribe failed", "channel", pgChannel, "error", err)
	}
}

// Unsubscribe removes notifCh from pgChannel subscribers.
func (m *GRPCNotificationManager) Unsubscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
	s := m.lookupSession(notifCh)
	if s == nil {
		return
	}
	if err := s.update(nil, []string{pgChannel}, false); err != nil {
		m.logger.Error("notification unsubscribe failed", "channel", pgChannel, "error", err)
	}
	if s.empty() {
		m.removeSession(notifCh)
	}
}

// UnsubscribeAll removes notifCh from all channels.
func (m *GRPCNotificationManager) UnsubscribeAll(notifCh chan *sqltypes.Notification) {
	s := m.lookupSession(notifCh)
	if s == nil {
		return
	}
	if err := s.update(nil, nil, true); err != nil {
		m.logger.Error("notification unsubscribe all failed", "error", err)
	}
	m.removeSession(notifCh)
}

func (m *GRPCNotificationManager) sessionFor(notifCh chan *sqltypes.Notification) *notificationSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.sessions[notifCh]; s != nil {
		return s
	}
	s := &notificationSession{
		manager: m,
		notifCh: notifCh,
		active:  make(map[string]bool),
	}
	m.sessions[notifCh] = s
	m.metrics.StreamAdd(context.TODO())
	return s
}

func (m *GRPCNotificationManager) lookupSession(notifCh chan *sqltypes.Notification) *notificationSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[notifCh]
}

func (m *GRPCNotificationManager) removeSession(notifCh chan *sqltypes.Notification) {
	m.mu.Lock()
	s := m.sessions[notifCh]
	delete(m.sessions, notifCh)
	m.mu.Unlock()
	if s != nil {
		s.stop()
		m.metrics.StreamRemove(context.TODO())
	}
}

type notificationSession struct {
	manager *GRPCNotificationManager
	notifCh chan *sqltypes.Notification

	mu     sync.Mutex
	cancel context.CancelFunc
	stream multipoolerpb.MultipoolerService_NotificationStreamClient
	ready  chan struct{}
	errs   chan error
	active map[string]bool
}

func (s *notificationSession) update(subscribe, unsubscribe []string, unsubscribeAll bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureStreamLocked(); err != nil {
		return err
	}
	if err := s.sendAndWaitLocked(&multipoolerpb.NotificationStreamRequest{
		SubscribeChannels:   subscribe,
		UnsubscribeChannels: unsubscribe,
		UnsubscribeAll:      unsubscribeAll,
	}); err != nil {
		// One reconnect/retry covers stale streams after pooler failover or a
		// transient stream error without adding a second buffering layer.
		if restartErr := s.restartLocked(); restartErr != nil {
			return fmt.Errorf("%w; reconnect failed: %w", err, restartErr)
		}
		if err = s.sendAndWaitLocked(&multipoolerpb.NotificationStreamRequest{
			SubscribeChannels:   subscribe,
			UnsubscribeChannels: unsubscribe,
			UnsubscribeAll:      unsubscribeAll,
		}); err != nil {
			return err
		}
	}

	if unsubscribeAll {
		clear(s.active)
	}
	for _, ch := range unsubscribe {
		delete(s.active, ch)
	}
	for _, ch := range subscribe {
		s.active[ch] = true
	}
	return nil
}

func (s *notificationSession) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active) == 0
}

func (s *notificationSession) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.cancel = nil
	s.stream = nil
}

func (s *notificationSession) ensureStreamLocked() error {
	if s.stream != nil {
		return nil
	}
	return s.startLocked()
}

func (s *notificationSession) restartLocked() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.cancel = nil
	s.stream = nil
	return s.startLocked()
}

func (s *notificationSession) startLocked() error {
	client := s.manager.getClient()
	if client == nil {
		return errNoClient
	}
	ctx, cancel := context.WithCancel(context.TODO())
	stream, err := client.NotificationStream(ctx)
	if err != nil {
		cancel()
		return err
	}
	s.cancel = cancel
	s.stream = stream
	s.ready = make(chan struct{}, 16)
	s.errs = make(chan error, 1)
	go s.recvLoop(ctx, stream, s.ready, s.errs)

	if len(s.active) == 0 {
		return nil
	}
	channels := make([]string, 0, len(s.active))
	for ch := range s.active {
		channels = append(channels, ch)
	}
	return s.sendAndWaitLocked(&multipoolerpb.NotificationStreamRequest{SubscribeChannels: channels})
}

func (s *notificationSession) sendAndWaitLocked(req *multipoolerpb.NotificationStreamRequest) error {
	if err := s.stream.Send(req); err != nil {
		return err
	}
	return s.waitReadyLocked()
}

func (s *notificationSession) waitReadyLocked() error {
	timer := time.NewTimer(notificationAckTimeout)
	defer timer.Stop()
	select {
	case <-s.ready:
		return nil
	case err := <-s.errs:
		return err
	case <-timer.C:
		return errors.New("notification stream ready timeout")
	}
}

func (s *notificationSession) recvLoop(
	ctx context.Context,
	stream multipoolerpb.MultipoolerService_NotificationStreamClient,
	ready chan<- struct{},
	errs chan<- error,
) {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			select {
			case errs <- err:
			default:
			}
			return
		}
		if resp.GetReady() {
			select {
			case ready <- struct{}{}:
			default:
			}
			continue
		}
		if resp.Notification == nil {
			continue
		}
		notif := &sqltypes.Notification{
			PID:     resp.Notification.Pid,
			Channel: resp.Notification.Channel,
			Payload: resp.Notification.Payload,
		}
		select {
		case s.notifCh <- notif:
		default:
			s.manager.logger.WarnContext(ctx, "notification channel full", "channel", notif.Channel)
			s.manager.metrics.NotificationDropped(ctx)
		}
	}
}

// errNoClient is a sentinel error for when no gRPC client is available.
var errNoClient = errors.New("no gRPC client available for notifications")
