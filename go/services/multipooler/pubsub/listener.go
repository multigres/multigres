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

// Package pubsub implements a shared PG LISTEN/NOTIFY listener
// that fans out notifications to multiple gateway client sessions.
package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
)

// request types for the serialized event loop.
type requestType int

const (
	reqSubscribe requestType = iota
	reqUnsubscribe
	reqUnsubscribeAll
)

type request struct {
	typ     requestType
	channel string
	subCh   chan *sqltypes.Notification
	done    chan struct{} // closed when request is processed
}

// Listener manages a single dedicated PG connection for LISTEN/NOTIFY,
// with channel refcounting and fan-out to subscribers.
//
// Architecture: split read/write on a single TCP connection.
// TCP is full-duplex; bufferedReader and bufferedWriter are independent bufio
// instances. The reader goroutine exclusively reads; the event loop exclusively
// writes. Because the reader never holds the connection, the event loop can
// send LISTEN/UNLISTEN at any time without tearing down and re-establishing
// the connection, which would lose notifications for existing channels.
type Listener struct {
	poolManager connpoolmanager.PoolManager
	logger      *slog.Logger
	metrics     *PubSubMetrics

	// requests is the serialized command channel for the event loop.
	requests chan request

	// mu protects startedCh.
	mu sync.Mutex

	// cancel stops the background goroutines.
	cancel context.CancelFunc

	// wg tracks background goroutines.
	wg sync.WaitGroup

	// stopped is closed when the event loop exits. Public methods select on this
	// to avoid blocking forever when the Listener is not running.
	stopped chan struct{}

	// startedCh is closed when the listener is running and accepting subscriptions.
	// Re-created (open) each time the listener stops, so waiters on the previous
	// channel are not falsely unblocked after a restart.
	// Callers use AwaitRunning to block until the listener is started.
	startedCh chan struct{}
}

// NewListener creates a new PubSubListener. Call Start() to begin.
func NewListener(poolManager connpoolmanager.PoolManager, logger *slog.Logger, metrics *PubSubMetrics) *Listener {
	stopped := make(chan struct{})
	close(stopped) // initially stopped
	return &Listener{
		poolManager: poolManager,
		logger:      logger,
		metrics:     metrics,
		requests:    make(chan request, 64),
		stopped:     stopped,
		startedCh:   make(chan struct{}), // initially open (not yet running)
	}
}

// Start begins the background event loop. Idempotent — no-op if already running.
func (l *Listener) Start(ctx context.Context) {
	if l.cancel != nil {
		return // already running
	}
	l.stopped = make(chan struct{})
	ctx, l.cancel = context.WithCancel(ctx)
	l.wg.Add(1)
	// Signal waiters before starting the goroutine so that AwaitRunning callers
	// unblock as soon as the event loop is guaranteed to start accepting requests.
	l.mu.Lock()
	close(l.startedCh)
	l.mu.Unlock()
	go l.run(ctx)
}

// Stop shuts down the listener and waits for goroutines to exit. Idempotent.
func (l *Listener) Stop() {
	if l.cancel == nil {
		return // not running
	}
	l.cancel()
	l.wg.Wait()
	l.cancel = nil
	// Re-create startedCh so future AwaitRunning calls block until the next Start.
	l.mu.Lock()
	l.startedCh = make(chan struct{})
	l.mu.Unlock()
}

// AwaitRunning blocks until the listener has been started or ctx is cancelled.
// This is used by components that must subscribe to channels only after the
// listener is running (e.g. schemaTracker), encoding the dependency in the
// component itself rather than relying on registration order.
func (l *Listener) AwaitRunning(ctx context.Context) {
	l.mu.Lock()
	ch := l.startedCh
	l.mu.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

// OnStateChange implements manager.StateAware. The listener runs only when the
// multipooler is PRIMARY+SERVING; it is stopped on any other state transition.
func (l *Listener) OnStateChange(_ context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error {
	if poolerType == clustermetadatapb.PoolerType_PRIMARY && servingStatus == clustermetadatapb.PoolerServingStatus_SERVING {
		//nolint:gocritic // Long-lived background listener; must not use the transient state-change ctx.
		l.Start(context.Background())
	} else {
		l.Stop()
	}
	return nil
}

// Unsubscribe removes a subscriber from the given channel.
// Returns immediately if the Listener is stopped.
func (l *Listener) Unsubscribe(channel string, subCh chan *sqltypes.Notification) {
	req := request{
		typ:     reqUnsubscribe,
		channel: channel,
		subCh:   subCh,
		done:    make(chan struct{}),
	}
	select {
	case l.requests <- req:
		<-req.done
	case <-l.stopped:
	}
}

// UnsubscribeAll removes a subscriber from all channels.
// Returns immediately if the Listener is stopped.
func (l *Listener) UnsubscribeAll(subCh chan *sqltypes.Notification) {
	req := request{
		typ:   reqUnsubscribeAll,
		subCh: subCh,
		done:  make(chan struct{}),
	}
	select {
	case l.requests <- req:
		<-req.done
	case <-l.stopped:
	}
}

// SubscribeCh registers an existing notification channel to receive notifications
// for the given PG channel name. Unlike Subscribe, this lets multiple PG channels
// feed into the same notification channel.
// Returns immediately if the Listener is stopped.
func (l *Listener) SubscribeCh(channel string, notifCh chan *sqltypes.Notification) {
	req := request{
		typ:     reqSubscribe,
		channel: channel,
		subCh:   notifCh,
		done:    make(chan struct{}),
	}
	select {
	case l.requests <- req:
		<-req.done
	case <-l.stopped:
	}
}

// readerMessage carries messages from the reader goroutine to the event loop.
type readerMessage struct {
	notification *sqltypes.Notification // non-nil for NotificationResponse
	cmdDone      bool                   // true for ReadyForQuery (command completed)
	err          error                  // non-nil for read errors
}

// run is the main event loop. It manages the PG connection, reads notifications,
// and processes subscribe/unsubscribe requests.
//
// Split read/write design:
//   - Reader goroutine: reads ALL messages from the socket (notifications, command
//     completions, errors). Sends parsed results to readerCh.
//   - Event loop (this function): processes subscribe/unsubscribe requests by writing
//     LISTEN/UNLISTEN commands directly on the live connection, then waits for the
//     reader to signal command completion via cmdDone.
//
// This eliminates the need to reconnect when subscribing/unsubscribing, preventing
// notification loss for existing channels.
func (l *Listener) run(ctx context.Context) {
	defer l.wg.Done()
	defer close(l.stopped)

	// Channel refcounts: channel name -> number of subscribers.
	channels := make(map[string]int)
	// Subscribers: channel name -> list of subscriber channels.
	subscribers := make(map[string][]chan *sqltypes.Notification)
	// All subscriber channels (for broadcast warnings).
	allSubs := make(map[chan *sqltypes.Notification]struct{})

	var conn *reserved.Conn
	var readerCh chan readerMessage // receives from reader goroutine
	var disconnectedAt time.Time    // tracks when connection was lost for gap duration metric

	// releaseConn releases the reserved connection. The readLoop goroutine
	// will get a read error on the closed socket and exit.
	// Setting readerCh = nil prevents the event loop from reading stale errors
	// from the old readLoop after the connection is released.
	releaseConn := func(reason reserved.ReleaseReason) {
		if conn != nil {
			conn.Release(reason)
			conn = nil
			readerCh = nil
			// Track disconnect time for reconnect gap duration metric.
			// Only set on error-path releases (not idle cleanup).
			if reason == reserved.ReleaseError && disconnectedAt.IsZero() {
				disconnectedAt = time.Now()
			}
		}
	}

	// connect establishes a new PG connection via the pool manager and starts the
	// reader goroutine. Used for initial connection and error recovery (reconnect).
	// On reconnect, re-LISTENs all active channels.
	connect := func() {
		releaseConn(reserved.ReleaseError)
		var err error
		conn, err = l.poolManager.NewReservedConn(ctx, nil, l.poolManager.PgUser())
		if err != nil {
			l.logger.ErrorContext(ctx, "pubsub: failed to acquire reserved connection", "error", err)
			conn = nil
			return
		}
		conn.AddReservationReason(protoutil.ReasonListen)
		conn.SetInactivityTimeout(0) // never timeout
		l.logger.InfoContext(ctx, "pubsub: connected to PG via pool")

		// Record reconnect gap duration if this was a reconnect (not initial connect).
		if !disconnectedAt.IsZero() {
			l.metrics.ReconnectGapDuration(ctx, time.Since(disconnectedAt).Seconds())
			disconnectedAt = time.Time{}
		}

		// Re-LISTEN all active channels via simple query protocol.
		// On initial connect this is a no-op (channels map is empty).
		for ch := range channels {
			_, err := conn.Query(ctx, "LISTEN "+ast.QuoteIdentifier(ch))
			if err != nil {
				l.logger.ErrorContext(ctx, "pubsub: failed to re-LISTEN", "channel", ch, "error", err)
				releaseConn(reserved.ReleaseError)
				return
			}
		}

		// Start reader goroutine with split read/write pattern.
		readerCh = make(chan readerMessage, 64)
		go l.readLoop(conn, readerCh)
	}

	// Connection is acquired lazily on first subscribe (not eagerly on startup)
	// to avoid holding an idle reserved connection in the pool.
	reconnectTimer := time.NewTimer(0)
	reconnectTimer.Stop()

	// pendingCmds tracks the number of LISTEN/UNLISTEN commands sent
	// that haven't received a ReadyForQuery yet. This must be a counter
	// (not a boolean) because reqUnsubscribeAll can send multiple
	// UNLISTEN commands in sequence.
	pendingCmds := 0

	for {
		select {
		case <-ctx.Done():
			releaseConn(reserved.ReleaseError)
			// Close all subscriber channels.
			for sub := range allSubs {
				close(sub)
			}
			return

		case req := <-l.requests:
			switch req.typ {
			case reqSubscribe:
				oldCount := channels[req.channel]
				channels[req.channel]++
				subscribers[req.channel] = append(subscribers[req.channel], req.subCh)
				_, existed := allSubs[req.subCh]
				allSubs[req.subCh] = struct{}{}
				if !existed {
					l.metrics.SubscriberAdd(ctx)
				}
				if oldCount == 0 {
					l.metrics.ChannelAdd(ctx)
				}

				if oldCount == 0 && conn != nil {
					// First subscriber for this channel — issue LISTEN on the live connection.
					if err := conn.SendQuery("LISTEN " + ast.QuoteIdentifier(req.channel)); err != nil {
						l.logger.ErrorContext(ctx, "pubsub: failed to send LISTEN",
							"channel", req.channel, "error", err)
						// Connection is broken, schedule reconnect.
						releaseConn(reserved.ReleaseError)
						reconnectTimer.Reset(2 * time.Second)
					} else {
						pendingCmds++
					}
				} else if oldCount == 0 && conn == nil {
					// No connection — acquire one. connect() will re-LISTEN
					// all channels in the map, including the one just added.
					connect()
					if conn == nil {
						reconnectTimer.Reset(2 * time.Second)
					}
				}

			case reqUnsubscribe:
				result := l.removeSub(subscribers, allSubs, channels, req.channel, req.subCh)
				if result.subscriberGone {
					l.metrics.SubscriberRemove(ctx)
				}
				if result.needsUnlisten {
					l.metrics.ChannelRemove(ctx)
				}
				if result.needsUnlisten && conn != nil {
					if err := conn.SendQuery("UNLISTEN " + ast.QuoteIdentifier(req.channel)); err != nil {
						l.logger.ErrorContext(ctx, "pubsub: failed to send UNLISTEN",
							"channel", req.channel, "error", err)
						releaseConn(reserved.ReleaseError)
						reconnectTimer.Reset(2 * time.Second)
					} else {
						pendingCmds++
					}
				}

			case reqUnsubscribeAll:
				var toUnlisten []string
				var toRemove []string
				for ch, subs := range subscribers {
					if slices.Contains(subs, req.subCh) {
						toRemove = append(toRemove, ch)
					}
				}
				for _, ch := range toRemove {
					result := l.removeSub(subscribers, allSubs, channels, ch, req.subCh)
					if result.subscriberGone {
						l.metrics.SubscriberRemove(ctx)
					}
					if result.needsUnlisten {
						l.metrics.ChannelRemove(ctx)
						toUnlisten = append(toUnlisten, ch)
					}
				}
				// Issue UNLISTEN for channels that hit refcount 0.
				if conn != nil {
					for _, ch := range toUnlisten {
						if err := conn.SendQuery("UNLISTEN " + ast.QuoteIdentifier(ch)); err != nil {
							l.logger.ErrorContext(ctx, "pubsub: failed to send UNLISTEN",
								"channel", ch, "error", err)
							releaseConn(reserved.ReleaseError)
							reconnectTimer.Reset(2 * time.Second)
							break
						}
						pendingCmds++
					}
				}
			}

			// If we sent commands, drain ReadyForQuery signals before
			// closing req.done. This ensures all LISTEN/UNLISTEN commands
			// have completed on PG before we return to the caller.
			if pendingCmds > 0 {
				if readerCh != nil {
					l.drainCommands(ctx, readerCh, subscribers, &conn, reconnectTimer, &pendingCmds, &disconnectedAt)
					// drainCommands may have released the connection on error.
					// Nil readerCh to avoid reading stale errors from the old readLoop.
					if conn == nil {
						readerCh = nil
					}
				} else {
					// Connection was released during command send — pending commands
					// are lost with the connection, reset the counter.
					pendingCmds = 0
				}
			}

			// Release the connection when no channels are subscribed.
			// ReleaseError taints the connection so the pool destroys it (closing the
			// socket), which stops the readLoop goroutine. We can't use ReleaseRollback
			// because the readLoop is still active on the socket — returning an active
			// socket to the pool would corrupt data.
			// On next subscribe, connect() will acquire a new reserved connection.
			if len(channels) == 0 && conn != nil {
				releaseConn(reserved.ReleaseError)
			}
			close(req.done)

		case msg := <-readerCh:
			if msg.err != nil {
				l.logger.ErrorContext(ctx, "pubsub: reader error, will reconnect", "error", msg.err)
				releaseConn(reserved.ReleaseError)
				l.metrics.Reconnect(ctx)
				reconnectTimer.Reset(2 * time.Second)
				continue
			}

			if msg.notification != nil {
				for _, sub := range subscribers[msg.notification.Channel] {
					select {
					case sub <- msg.notification:
					default:
						l.logger.WarnContext(ctx, "pubsub: subscriber channel full, dropping notification",
							"channel", msg.notification.Channel)
						l.metrics.NotificationDropped(ctx)
					}
				}
			}

			if msg.cmdDone && pendingCmds > 0 {
				pendingCmds--
			}

		case <-reconnectTimer.C:
			// Only reconnect if we have channels to listen on.
			if conn == nil && len(channels) > 0 {
				connect()
				if conn == nil {
					reconnectTimer.Reset(5 * time.Second)
				}
			}
		}
	}
}

// drainCommands reads from readerCh until all pending command completions
// (ReadyForQuery) have been received. This is called after sending
// LISTEN/UNLISTEN to ensure all commands have completed on PG before returning
// to the caller. Notifications received during this drain are delivered normally.
//
// pendingCmds is decremented for each ReadyForQuery received; the function
// returns when it reaches 0. This correctly handles reqUnsubscribeAll which
// may send multiple UNLISTEN commands in sequence.
func (l *Listener) drainCommands(
	ctx context.Context,
	readerCh chan readerMessage,
	subscribers map[string][]chan *sqltypes.Notification,
	conn **reserved.Conn,
	reconnectTimer *time.Timer,
	pendingCmds *int,
	disconnectedAt *time.Time,
) {
	for *pendingCmds > 0 {
		select {
		case <-ctx.Done():
			*pendingCmds = 0
			return
		case msg := <-readerCh:
			if msg.err != nil {
				l.logger.ErrorContext(ctx, "pubsub: reader error during command wait, will reconnect",
					"error", msg.err)
				if *conn != nil {
					(*conn).Release(reserved.ReleaseError)
					*conn = nil
				}
				*disconnectedAt = time.Now()
				l.metrics.Reconnect(ctx)
				reconnectTimer.Reset(2 * time.Second)
				*pendingCmds = 0
				return
			}

			if msg.notification != nil {
				for _, sub := range subscribers[msg.notification.Channel] {
					select {
					case sub <- msg.notification:
					default:
						l.logger.WarnContext(ctx, "pubsub: subscriber channel full, dropping notification",
							"channel", msg.notification.Channel)
						l.metrics.NotificationDropped(ctx)
					}
				}
			}

			if msg.cmdDone {
				*pendingCmds--
			}
		}
	}
}

// readLoop is the reader goroutine. It reads all messages from the PG connection
// and classifies them into notifications, command completions, or errors.
//
// Message handling:
//   - NotificationResponse ('A'): parsed and sent as notification
//   - CommandComplete ('C'): acknowledged (LISTEN/UNLISTEN response), not forwarded
//   - ReadyForQuery ('Z'): signals command completion to the event loop
//   - ErrorResponse ('E'): treated as connection error
//   - ParameterStatus ('S'), NoticeResponse ('N'): ignored
func (l *Listener) readLoop(conn *reserved.Conn, ch chan<- readerMessage) {
	for {
		msgType, body, err := conn.ReadRawMessage()
		if err != nil {
			ch <- readerMessage{err: err}
			return
		}

		switch msgType {
		case protocol.MsgNotificationResponse:
			notif, err := conn.ParseNotification(body)
			if err != nil {
				ch <- readerMessage{err: err}
				return
			}
			ch <- readerMessage{notification: notif}

		case protocol.MsgCommandComplete:
			// LISTEN/UNLISTEN acknowledgment — skip, wait for ReadyForQuery.

		case protocol.MsgReadyForQuery:
			// Command completed (LISTEN/UNLISTEN finished).
			ch <- readerMessage{cmdDone: true}

		case protocol.MsgErrorResponse:
			// PG returned an error for our LISTEN/UNLISTEN command.
			// Treat as connection-level failure and reconnect.
			ch <- readerMessage{err: errors.New("pubsub: received ErrorResponse from PG")}
			return

		case protocol.MsgParameterStatus, protocol.MsgNoticeResponse:
			// Ignore.

		default:
			l.logger.Warn("pubsub: unexpected message type from PG",
				"type", string([]byte{msgType}))
		}
	}
}

// removeSubResult holds the outcome of removeSub for metrics recording.
type removeSubResult struct {
	needsUnlisten  bool // channel refcount hit 0 — caller should issue UNLISTEN
	subscriberGone bool // subscriber fully removed from allSubs (no remaining channels)
}

// removeSub removes a single subscriber from a channel.
func (l *Listener) removeSub(
	subscribers map[string][]chan *sqltypes.Notification,
	allSubs map[chan *sqltypes.Notification]struct{},
	channels map[string]int,
	channel string,
	subCh chan *sqltypes.Notification,
) removeSubResult {
	subs := subscribers[channel]
	found := false
	for i, s := range subs {
		if s == subCh {
			subscribers[channel] = append(subs[:i], subs[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return removeSubResult{}
	}

	// Only remove from allSubs if the subscriber has no remaining subscriptions.
	subscriberGone := false
	stillSubscribed := false
	for _, otherSubs := range subscribers {
		if slices.Contains(otherSubs, subCh) {
			stillSubscribed = true
			break
		}
	}
	if !stillSubscribed {
		delete(allSubs, subCh)
		subscriberGone = true
	}

	channels[channel]--
	if channels[channel] <= 0 {
		delete(channels, channel)
		delete(subscribers, channel)
		return removeSubResult{needsUnlisten: true, subscriberGone: subscriberGone}
	}
	return removeSubResult{subscriberGone: subscriberGone}
}
