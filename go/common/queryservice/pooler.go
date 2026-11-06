// Copyright 2025 Supabase, Inc.
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

package queryservice

import (
	"context"

	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Pooler is what the gateway needs from one multipooler, whatever the
// transport: the query service plus the calls that are not queries. It is
// implemented over gRPC (Multigres) and by direct calls to a pooler in the same
// process (minigres). QueryService stays separate because the pooler's
// executor and the gateway's router implement it but have no health or
// credentials of their own.
type Pooler interface {
	QueryService

	// GetAuthCredentials returns the SCRAM hash and replication attribute of a
	// role, for client authentication.
	GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error)

	// SubscribeHealth subscribes to the pooler's health until ctx ends. Each
	// call to recv returns the next health message, starting with the current
	// state; it returns io.EOF when the pooler ends the subscription, or an
	// error when ctx ends or the subscription fails.
	SubscribeHealth(ctx context.Context) (recv func() (*multipoolerpb.StreamPoolerHealthResponse, error), err error)

	// NotificationStream opens a LISTEN/NOTIFY stream for one client session,
	// until ctx ends. Subscription updates and notifications share the stream
	// so notifications across channels keep PostgreSQL's order.
	NotificationStream(ctx context.Context) (NotificationStream, error)
}

// NotificationStream is one client session's LISTEN/NOTIFY stream to a pooler.
// Send updates the session's subscriptions; Recv returns a reply with
// ready=true once an update is active on PostgreSQL, or a notification. Recv
// returns io.EOF or an error when the stream ends. The gRPC stream for
// MultipoolerService.NotificationStream implements it.
type NotificationStream interface {
	Send(*multipoolerpb.NotificationStreamRequest) error
	Recv() (*multipoolerpb.NotificationStreamResponse, error)
}
