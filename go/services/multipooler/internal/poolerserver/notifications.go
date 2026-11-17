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

package poolerserver

import (
	"context"
	"errors"
	"io"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// ServeNotifications serves one client session's LISTEN/NOTIFY stream until
// ctx ends, recv fails or the pubsub listener stops, whatever the transport:
// the gRPC NotificationStream handler and, in minigres, the gateway's direct
// stream. recv returns the session's subscription updates (io.EOF when the
// client is done); send delivers ready acknowledgements and notifications. It
// returns nil when the client or the listener ended the stream.
func (s *QueryPoolerServer) ServeNotifications(
	ctx context.Context,
	recv func() (*multipoolerpb.NotificationStreamRequest, error),
	send func(*multipoolerpb.NotificationStreamResponse) error,
) error {
	listener := s.PubSubListener()
	if listener == nil {
		return errors.New("PubSubListener not initialized")
	}
	if !listener.Running() {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pubsub listener not running: pooler is not the serving leader")
	}

	notifCh := make(chan *sqltypes.Notification, 256)
	subscribed := make(map[string]bool)
	defer func() {
		for ch := range subscribed {
			listener.Unsubscribe(ch, notifCh)
		}
	}()

	reqCh := make(chan *multipoolerpb.NotificationStreamRequest)
	errCh := make(chan error, 1)
	go func() {
		for {
			req, err := recv()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case reqCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case req := <-reqCh:
			if req.GetUnsubscribeAll() {
				for ch := range subscribed {
					listener.Unsubscribe(ch, notifCh)
					delete(subscribed, ch)
				}
			}
			for _, ch := range req.GetUnsubscribeChannels() {
				if subscribed[ch] {
					listener.Unsubscribe(ch, notifCh)
					delete(subscribed, ch)
				}
			}
			for _, ch := range req.GetSubscribeChannels() {
				if subscribed[ch] {
					continue
				}
				// The listener may have stopped since the Running check
				// above. An unregistered notifCh is never closed, so
				// acking here would leave this stream open, and the
				// gateway subscribed to nothing, until the client went
				// away. Refuse instead so the gateway reconnects.
				if !listener.SubscribeCh(ch, notifCh) {
					return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pubsub listener stopped: pooler is no longer the serving leader")
				}
				subscribed[ch] = true
			}
			if err := send(&multipoolerpb.NotificationStreamResponse{Ready: true}); err != nil {
				return err
			}
		case notif := <-notifCh:
			if notif == nil {
				return nil
			}
			if err := send(&multipoolerpb.NotificationStreamResponse{
				Notification: &query.PgNotification{
					Pid:     notif.PID,
					Channel: notif.Channel,
					Payload: notif.Payload,
				},
			}); err != nil {
				return err
			}
		}
	}
}
