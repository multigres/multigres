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

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// Notification wraps a PG notification.
type Notification struct {
	PID     int32
	Channel string
	Payload string
}

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
	subCh   chan *Notification
	done    chan struct{} // closed when request is processed
}

// Listener manages a single dedicated PG connection for LISTEN/NOTIFY,
// with channel refcounting and fan-out to subscribers.
//
// Architecture: split read/write on a single TCP connection.
// TCP is full-duplex; bufferedReader and bufferedWriter are independent bufio
// instances. The reader goroutine exclusively reads; the event loop exclusively
// writes. This avoids reconnecting to issue LISTEN/UNLISTEN commands, which
// would cause notification loss for existing channels.
type Listener struct {
	config *client.Config
	logger *slog.Logger

	// requests is the serialized command channel for the event loop.
	requests chan request

	// cancel stops the background goroutines.
	cancel context.CancelFunc

	// wg tracks background goroutines.
	wg sync.WaitGroup
}

// NewListener creates a new PubSubListener. Call Start() to begin.
func NewListener(config *client.Config, logger *slog.Logger) *Listener {
	return &Listener{
		config:   config,
		logger:   logger,
		requests: make(chan request, 64),
	}
}

// Start begins the background event loop.
func (l *Listener) Start(ctx context.Context) {
	ctx, l.cancel = context.WithCancel(ctx)
	l.wg.Add(1)
	go l.run(ctx)
}

// Stop shuts down the listener and waits for goroutines to exit.
func (l *Listener) Stop() {
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
}

// Subscribe registers a subscriber for the given channel.
// Returns a channel that will receive notifications.
// The returned channel has buffer of 64 to avoid blocking the fan-out.
func (l *Listener) Subscribe(channel string) chan *Notification {
	ch := make(chan *Notification, 64)
	req := request{
		typ:     reqSubscribe,
		channel: channel,
		subCh:   ch,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
	return ch
}

// Unsubscribe removes a subscriber from the given channel.
func (l *Listener) Unsubscribe(channel string, subCh chan *Notification) {
	req := request{
		typ:     reqUnsubscribe,
		channel: channel,
		subCh:   subCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// UnsubscribeAll removes a subscriber from all channels.
func (l *Listener) UnsubscribeAll(subCh chan *Notification) {
	req := request{
		typ:   reqUnsubscribeAll,
		subCh: subCh,
		done:  make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// SubscribeCh registers an existing notification channel to receive notifications
// for the given PG channel name. Unlike Subscribe, this lets multiple PG channels
// feed into the same notification channel.
func (l *Listener) SubscribeCh(channel string, notifCh chan *Notification) {
	req := request{
		typ:     reqSubscribe,
		channel: channel,
		subCh:   notifCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
}

// UnsubscribeCh removes a subscription for a specific PG channel using the provided channel.
func (l *Listener) UnsubscribeCh(channel string, notifCh chan *Notification) {
	req := request{
		typ:     reqUnsubscribe,
		channel: channel,
		subCh:   notifCh,
		done:    make(chan struct{}),
	}
	l.requests <- req
	<-req.done
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

	// Channel refcounts: channel name -> number of subscribers.
	channels := make(map[string]int)
	// Subscribers: channel name -> list of subscriber channels.
	subscribers := make(map[string][]chan *Notification)
	// All subscriber channels (for broadcast warnings).
	allSubs := make(map[chan *Notification]struct{})

	var conn *client.Conn
	var readerCh chan readerMessage // receives from reader goroutine

	// connect establishes a new PG connection and starts the reader goroutine.
	// Used for initial connection and error recovery (reconnect).
	// On reconnect, re-LISTENs all active channels.
	connect := func() {
		if conn != nil {
			conn.Close()
			conn = nil
		}
		var err error
		conn, err = client.Connect(ctx, ctx, l.config)
		if err != nil {
			l.logger.ErrorContext(ctx, "pubsub: failed to connect to PG", "error", err)
			conn = nil
			return
		}
		l.logger.InfoContext(ctx, "pubsub: connected to PG")

		// Re-LISTEN all active channels via simple query protocol.
		// On initial connect this is a no-op (channels map is empty).
		for ch := range channels {
			_, err := conn.Query(ctx, "LISTEN "+quoteIdent(ch))
			if err != nil {
				l.logger.ErrorContext(ctx, "pubsub: failed to re-LISTEN", "channel", ch, "error", err)
				conn.Close()
				conn = nil
				return
			}
		}

		// Start reader goroutine with split read/write pattern.
		readerCh = make(chan readerMessage, 64)
		go l.readLoop(conn, readerCh)
	}

	connect()

	reconnectTimer := time.NewTimer(0)
	reconnectTimer.Stop()

	// If the initial connect failed, schedule a retry.
	if conn == nil {
		reconnectTimer.Reset(2 * time.Second)
	}

	// pendingCmds tracks the number of LISTEN/UNLISTEN commands sent
	// that haven't received a ReadyForQuery yet. This must be a counter
	// (not a boolean) because reqUnsubscribeAll can send multiple
	// UNLISTEN commands in sequence.
	pendingCmds := 0

	for {
		select {
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
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
				allSubs[req.subCh] = struct{}{}

				if oldCount == 0 && conn != nil {
					// First subscriber for this channel — issue LISTEN on the live connection.
					if err := conn.SendQuery("LISTEN " + quoteIdent(req.channel)); err != nil {
						l.logger.ErrorContext(ctx, "pubsub: failed to send LISTEN",
							"channel", req.channel, "error", err)
						// Connection is broken, schedule reconnect.
						conn.Close()
						conn = nil
						reconnectTimer.Reset(2 * time.Second)
					} else {
						pendingCmds++
					}
				} else if oldCount == 0 && conn == nil {
					reconnectTimer.Reset(1 * time.Second)
				}

			case reqUnsubscribe:
				needsUnlisten := l.removeSub(subscribers, allSubs, channels, req.channel, req.subCh)
				if needsUnlisten && conn != nil {
					if err := conn.SendQuery("UNLISTEN " + quoteIdent(req.channel)); err != nil {
						l.logger.ErrorContext(ctx, "pubsub: failed to send UNLISTEN",
							"channel", req.channel, "error", err)
						conn.Close()
						conn = nil
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
					if l.removeSub(subscribers, allSubs, channels, ch, req.subCh) {
						toUnlisten = append(toUnlisten, ch)
					}
				}
				// Issue UNLISTEN for channels that hit refcount 0.
				if conn != nil {
					for _, ch := range toUnlisten {
						if err := conn.SendQuery("UNLISTEN " + quoteIdent(ch)); err != nil {
							l.logger.ErrorContext(ctx, "pubsub: failed to send UNLISTEN",
								"channel", ch, "error", err)
							conn.Close()
							conn = nil
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
				l.drainCommands(ctx, readerCh, subscribers, &conn, reconnectTimer, &pendingCmds)
			}
			close(req.done)

		case msg := <-readerCh:
			if msg.err != nil {
				l.logger.ErrorContext(ctx, "pubsub: reader error, will reconnect", "error", msg.err)
				if conn != nil {
					conn.Close()
					conn = nil
				}
				reconnectTimer.Reset(2 * time.Second)
				continue
			}

			if msg.notification != nil {
				notif := &Notification{
					PID:     msg.notification.PID,
					Channel: msg.notification.Channel,
					Payload: msg.notification.Payload,
				}
				for _, sub := range subscribers[msg.notification.Channel] {
					select {
					case sub <- notif:
					default:
						l.logger.WarnContext(ctx, "pubsub: subscriber channel full, dropping notification",
							"channel", msg.notification.Channel)
					}
				}
			}

			if msg.cmdDone && pendingCmds > 0 {
				pendingCmds--
			}

		case <-reconnectTimer.C:
			if conn == nil {
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
	subscribers map[string][]chan *Notification,
	conn **client.Conn,
	reconnectTimer *time.Timer,
	pendingCmds *int,
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
					(*conn).Close()
					*conn = nil
				}
				reconnectTimer.Reset(2 * time.Second)
				*pendingCmds = 0
				return
			}

			if msg.notification != nil {
				notif := &Notification{
					PID:     msg.notification.PID,
					Channel: msg.notification.Channel,
					Payload: msg.notification.Payload,
				}
				for _, sub := range subscribers[msg.notification.Channel] {
					select {
					case sub <- notif:
					default:
						l.logger.WarnContext(ctx, "pubsub: subscriber channel full, dropping notification",
							"channel", msg.notification.Channel)
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
func (l *Listener) readLoop(conn *client.Conn, ch chan<- readerMessage) {
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

// removeSub removes a single subscriber from a channel.
// Returns true if the channel's refcount hit 0 and an UNLISTEN should be issued.
func (l *Listener) removeSub(
	subscribers map[string][]chan *Notification,
	allSubs map[chan *Notification]struct{},
	channels map[string]int,
	channel string,
	subCh chan *Notification,
) bool {
	subs := subscribers[channel]
	for i, s := range subs {
		if s == subCh {
			subscribers[channel] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	delete(allSubs, subCh)

	channels[channel]--
	if channels[channel] <= 0 {
		delete(channels, channel)
		delete(subscribers, channel)
		return true // caller should issue UNLISTEN
	}
	return false
}

// quoteIdent quotes a PG identifier for use in LISTEN/UNLISTEN.
func quoteIdent(s string) string {
	return `"` + doubleQuoteEscape(s) + `"`
}

func doubleQuoteEscape(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			result = append(result, '"', '"')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}
