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

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// NotificationManager abstracts LISTEN/NOTIFY subscription management.
type NotificationManager interface {
	// Subscribe registers a notification channel to receive notifications
	// for the given PG channel name. The same notifCh can be used for
	// multiple PG channels (all notifications delivered to one place).
	Subscribe(pgChannel string, notifCh chan *sqltypes.Notification)

	// Unsubscribe removes a subscription for a specific PG channel.
	Unsubscribe(pgChannel string, notifCh chan *sqltypes.Notification)

	// UnsubscribeAll removes all subscriptions for a given notifCh.
	UnsubscribeAll(notifCh chan *sqltypes.Notification)
}

// noopNotificationManager is used when no PubSubListener is configured.
type noopNotificationManager struct{}

func (n *noopNotificationManager) Subscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
}

func (n *noopNotificationManager) Unsubscribe(pgChannel string, notifCh chan *sqltypes.Notification) {
}
func (n *noopNotificationManager) UnsubscribeAll(notifCh chan *sqltypes.Notification) {}

// DefaultNotificationManager returns a no-op manager.
func DefaultNotificationManager() NotificationManager {
	return &noopNotificationManager{}
}

// SubscriptionSync coordinates LISTEN/NOTIFY subscriptions for a connection.
// Implemented by the handler and stored on connection state so that engine
// primitives can apply subscription changes before reporting success to the
// client. This ensures subscriptions are active before the client learns
// that LISTEN (or a COMMIT containing LISTEN) succeeded, preserving
// PostgreSQL's atomicity guarantees.
type SubscriptionSync interface {
	// SyncSubscriptions applies subscription changes to the notification manager.
	// subscribes/unsubscribes list individual channels; unsubscribeAll removes all.
	// ctx must be the connection-scoped context (not query-scoped) since it may
	// start long-lived goroutines.
	SyncSubscriptions(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, subscribes, unsubscribes []string, unsubscribeAll bool)
}
