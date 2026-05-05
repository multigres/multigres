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

package handler

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// handlerSubSync implements SubscriptionSync by delegating to a NotificationManager
// and managing the async notification delivery pipeline (notifCh → asyncCh → client).
type handlerSubSync struct {
	notifMgr       NotificationManager
	logger         *slog.Logger
	forwardCancel  context.CancelFunc
	onNotifDropped func(ctx context.Context) // called when a notification is dropped due to full channel
}

// SyncSubscriptions applies subscription changes to the notification manager and
// manages the async notification delivery pipeline for the connection.
//
// The subscribes/unsubscribes lists specify individual channel changes;
// unsubscribeAll removes all subscriptions. The ctx must be connection-scoped
// (not query-scoped) since it may start long-lived goroutines.
func (s *handlerSubSync) SyncSubscriptions(
	ctx context.Context,
	conn *server.Conn,
	state *MultiGatewayConnectionState,
	subscribes, unsubscribes []string,
	unsubscribeAll bool,
) {
	notifCh := ensureNotifCh(state)

	if unsubscribeAll {
		s.notifMgr.UnsubscribeAll(notifCh)
	}

	for _, ch := range unsubscribes {
		s.notifMgr.Unsubscribe(ch, notifCh)
	}

	for _, ch := range subscribes {
		s.notifMgr.Subscribe(ch, notifCh)
	}

	// Start async notification pusher if we have listen channels and no pusher yet.
	listenCount := len(state.GetListenChannels())
	if listenCount > 0 && state.AsyncNotifCh == nil {
		asyncCh := conn.EnableAsyncNotifications(ctx)
		state.AsyncNotifCh = asyncCh
		fwdCtx, cancel := context.WithCancel(ctx)
		s.forwardCancel = cancel
		go s.forwardNotifications(fwdCtx, notifCh, asyncCh)
	}

	// Stop async pusher and release notification channel if no more listen channels.
	if listenCount == 0 && state.AsyncNotifCh != nil {
		s.forwardCancel()
		s.forwardCancel = nil
		conn.StopAsyncNotifications()
		state.AsyncNotifCh = nil
		state.NotifCh = nil
	}
}

// ensureNotifCh creates the notification channel for a connection if needed.
func ensureNotifCh(state *MultiGatewayConnectionState) chan *sqltypes.Notification {
	if state.NotifCh == nil {
		state.NotifCh = make(chan *sqltypes.Notification, 256)
	}
	return state.NotifCh
}

// forwardNotifications reads from the handler notifCh and forwards to the
// server.Conn async pusher channel.
func (s *handlerSubSync) forwardNotifications(
	ctx context.Context,
	notifCh chan *sqltypes.Notification,
	asyncCh chan<- *sqltypes.Notification,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case notif, ok := <-notifCh:
			if !ok {
				return
			}
			select {
			case asyncCh <- notif:
			default:
				s.logger.WarnContext(ctx, "async notification channel full, dropping notification",
					"channel", notif.Channel)
				if s.onNotifDropped != nil {
					s.onNotifDropped(ctx)
				}
			}
		}
	}
}
