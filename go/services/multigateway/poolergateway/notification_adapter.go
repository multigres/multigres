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
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

const notificationAckTimeout = 10 * time.Second

// NotificationClientFunc returns the pooler that serves the LISTEN stream of
// database's application traffic, and a key naming where that is (its
// tablegroup and shard after routing rules), or an error when there is none.
type NotificationClientFunc func(database string) (client multipoolerpb.MultipoolerServiceClient, key string, err error)

// GRPCNotificationManager implements handler.NotificationManager by keeping one
// ordered gRPC notification stream per gateway client session (notifCh). A
// single stream carries all channels for that session, so cross-channel
// notifications are delivered in the same order PostgreSQL emitted them.
//
// A session's stream goes to its database's application traffic: the stream
// follows the database's routing rules, and moves when they change (see
// OnRoutingChanged).
type GRPCNotificationManager struct {
	getClient NotificationClientFunc
	logger    *slog.Logger
	metrics   *NotificationMetrics

	mu       sync.Mutex
	sessions map[chan *sqltypes.Notification]*notificationSession
}

// NewGRPCNotificationManager creates a notification manager backed by gRPC.
func NewGRPCNotificationManager(
	getClient NotificationClientFunc,
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

// Subscribe registers notifCh to receive notifications for pgChannel of
// database. A session's notifCh always belongs to one database.
func (m *GRPCNotificationManager) Subscribe(database, pgChannel string, notifCh chan *sqltypes.Notification) {
	s := m.sessionFor(database, notifCh)
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

// OnLeaderServing wakes every session that has no stream so it retries
// against the leader the load balancer just observed serving. Wired to the
// load balancer's leader-serving hook, which fires on each leader health
// update, including the periodic heartbeat; that heartbeat is the only
// retry cadence a session has, so there is no timer here. Cheap and
// non-blocking: it signals sessions and returns, so the health stream
// goroutine never waits on a session's stream handshake.
func (m *GRPCNotificationManager) OnLeaderServing(_ *clustermetadatapb.ShardRef) {
	m.mu.Lock()
	sessions := make([]*notificationSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.wakeUp()
	}
}

// OnRoutingChanged moves the streams of database's sessions whose
// application traffic now goes elsewhere: each is closed and reopened where
// the routing now sends it. Notifications sent on the old side in between are
// not delivered; until fences exist, routing changes only while the
// application is quiesced.
func (m *GRPCNotificationManager) OnRoutingChanged(database string) {
	m.mu.Lock()
	var sessions []*notificationSession
	for _, s := range m.sessions {
		if s.database == database {
			sessions = append(sessions, s)
		}
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.rerouteIfMoved()
	}
}

func (m *GRPCNotificationManager) sessionFor(database string, notifCh chan *sqltypes.Notification) *notificationSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.sessions[notifCh]; s != nil {
		return s
	}
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gocritic // Session-owned lifetime; no request context available.
	s := &notificationSession{
		manager:       m,
		database:      database,
		notifCh:       notifCh,
		ctx:           ctx,
		cancelSession: cancel,
		wake:          make(chan struct{}, 1),
		active:        make(map[string]bool),
	}
	m.sessions[notifCh] = s
	go s.reconnectLoop()
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
	}
}

// notificationSession owns the single gRPC stream for one client session.
//
// All stream creation happens under mu, and only when stream is nil, so at
// most one stream is open at a time and notifications stay ordered. A
// stream that ends while channels are active is reopened by reconnectLoop,
// which is purely event-driven: it is woken by the stream ending and by the
// load balancer observing the leader serving (OnLeaderServing). The stream
// metric counts streams that have completed their (re)subscribe handshake.
type notificationSession struct {
	manager  *GRPCNotificationManager
	database string
	notifCh  chan *sqltypes.Notification

	// ctx lives for the whole session. stop cancels it, which also cancels
	// the current stream and ends reconnectLoop.
	ctx           context.Context
	cancelSession context.CancelFunc
	// wake carries at most one pending reconnect request; extra wake-ups
	// while one is pending coalesce.
	wake chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc // cancels the current stream only
	stream multipoolerpb.MultipoolerService_NotificationStreamClient
	// streamKey is where the current stream goes (see NotificationClientFunc).
	streamKey string
	ready     chan struct{}
	errs      chan error
	active    map[string]bool
}

func (s *notificationSession) update(subscribe, unsubscribe []string, unsubscribeAll bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// active is what the client asked for, applied before any send so a
	// LISTEN that lands while no leader is reachable is still restored by
	// reconnectLoop. A failed send closes the stream, so an open stream is
	// always in sync with active, and the next stream resubscribes exactly
	// active (the pooler drops every subscription when its stream ends).
	if unsubscribeAll {
		clear(s.active)
	}
	for _, ch := range unsubscribe {
		delete(s.active, ch)
	}
	for _, ch := range subscribe {
		s.active[ch] = true
	}

	var sendErr error
	if s.stream != nil {
		sendErr = s.sendAndWaitLocked(&multipoolerpb.NotificationStreamRequest{
			SubscribeChannels:   subscribe,
			UnsubscribeChannels: unsubscribe,
			UnsubscribeAll:      unsubscribeAll,
		})
		if sendErr == nil {
			return nil
		}
		s.closeStreamLocked()
	}
	if len(s.active) == 0 {
		return nil
	}
	// One in-line open keeps LISTEN/UNLISTEN synchronous for the client; it
	// subscribes the whole active set, which already includes this update. If
	// it fails, the next leader health update wakes reconnectLoop to retry.
	if err := s.startLocked(); err != nil {
		if sendErr != nil {
			return fmt.Errorf("%w; reconnect failed: %w", sendErr, err)
		}
		return err
	}
	return nil
}

func (s *notificationSession) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active) == 0
}

func (s *notificationSession) stop() {
	s.cancelSession()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeStreamLocked()
}

// closeStreamLocked tears down the current stream, if any.
func (s *notificationSession) closeStreamLocked() {
	if s.stream == nil {
		return
	}
	s.cancel()
	s.cancel, s.stream = nil, nil
	s.manager.metrics.StreamRemove(context.Background()) //nolint:gocritic // Metric recording at stream close; no request context available.
}

// startLocked opens a stream to the current leader and resubscribes the
// session's active channels. On failure no stream is left behind.
func (s *notificationSession) startLocked() error {
	client, key, err := s.manager.getClient(s.database)
	if err != nil {
		return err
	}
	if client == nil {
		return errNoClient
	}
	ctx, cancel := context.WithCancel(s.ctx)
	stream, err := client.NotificationStream(ctx)
	if err != nil {
		cancel()
		return err
	}
	s.cancel, s.stream, s.streamKey = cancel, stream, key
	s.ready = make(chan struct{}, 16)
	s.errs = make(chan error, 1)
	go s.recvLoop(ctx, stream, s.ready, s.errs)

	if len(s.active) > 0 {
		channels := make([]string, 0, len(s.active))
		for ch := range s.active {
			channels = append(channels, ch)
		}
		if err := s.sendAndWaitLocked(&multipoolerpb.NotificationStreamRequest{SubscribeChannels: channels}); err != nil {
			cancel()
			s.cancel, s.stream = nil, nil
			return err
		}
	}
	s.manager.metrics.StreamAdd(context.Background()) //nolint:gocritic // Metric recording at stream open; no request context available.
	return nil
}

// rerouteIfMoved closes the session's stream when the database's application
// traffic no longer goes where the stream does, and has reconnectLoop open it
// where it goes now.
func (s *notificationSession) rerouteIfMoved() {
	s.mu.Lock()
	moved := false
	if s.stream != nil {
		_, key, err := s.manager.getClient(s.database)
		moved = err != nil || key != s.streamKey
		if moved {
			s.closeStreamLocked()
		}
	}
	s.mu.Unlock()
	if moved {
		s.manager.logger.Info("notification stream moved with the database's routing", "database", s.database)
		s.wakeUp()
	}
}

// wakeUp asks reconnectLoop to attempt a reconnect. Never blocks.
func (s *notificationSession) wakeUp() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// reconnectLoop reopens the stream once per wake-up while the session has
// active channels and no stream. Each attempt resolves the leader afresh
// through the client getter, so after a failover it lands on the new leader
// once the load balancer has observed it. There is no timer: an attempt
// that fails waits for the next wake-up, which the leader's next health
// update (at the latest its periodic heartbeat) provides. Runs for the
// session's lifetime and exits when the session is stopped.
func (s *notificationSession) reconnectLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		}
		s.mu.Lock()
		if s.ctx.Err() != nil || s.stream != nil || len(s.active) == 0 {
			s.mu.Unlock()
			continue
		}
		err := s.startLocked()
		n := len(s.active)
		s.mu.Unlock()
		if err != nil {
			s.manager.logger.Warn("notification stream reconnect failed; waiting for next leader health update", "error", err)
			continue
		}
		s.manager.logger.Info("notification stream re-established", "channels", n)
	}
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
			if ctx.Err() != nil {
				return // closed by closeStreamLocked or stop
			}
			// EOF and errors alike mean the pooler ended the stream: a demoted
			// leader stops its listener, closes every subscriber channel and
			// returns from the handler. Wake any in-flight update, then hand
			// the stream over to the reconnect loop.
			select {
			case errs <- err:
			default:
			}
			s.mu.Lock()
			if s.stream == stream {
				s.closeStreamLocked()
				s.wakeUp()
			}
			s.mu.Unlock()
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
