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

package multipooler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/topoclient"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
)

// Bootstrap retry timing: a pooler waits for a default primary that is not reachable yet.
var (
	bootstrapAttemptTimeout = 5 * time.Second
	bootstrapBackoffBase    = 100 * time.Millisecond
	bootstrapBackoffMax     = 5 * time.Second
)

// warnInsecureCredentialTransport warns when a backing connection with a password was fetched
// from the default primary over a gRPC transport without TLS: the credential of the external
// database then crossed the network in the clear. The credential is not logged.
func warnInsecureCredentialTransport(ctx context.Context, logger *slog.Logger, conn *backingconn.Conn, tls bool) {
	if conn.Password == "" || tls {
		return
	}
	logger.WarnContext(ctx, "the password of the backing connection was fetched from the default primary over a gRPC transport without TLS; configure TLS between the poolers",
		"connection", conn.Name)
}

// defaultPrimarySource fetches a backing connection from the default primary of the
// database, found through topology.
type defaultPrimarySource struct {
	ts        topoclient.Store
	database  string
	transport grpc.DialOption
}

// TableGroupBinding asks the default primary how the tablegroup is served. The reply is taken as
// it is; fetchBinding decides whether it is usable.
func (d *defaultPrimarySource) TableGroupBinding(ctx context.Context, tablegroupID int64) (*backingconn.Conn, bool, error) {
	var (
		conn    *backingconn.Conn
		managed bool
	)
	err := metadataclient.WithDefaultPrimary(ctx, d.ts, d.database, d.transport, func(client multipoolerservicepb.MultipoolerServiceClient) error {
		reply, err := client.GetBackingConnection(ctx, &multipoolerservicepb.GetBackingConnectionRequest{
			Database:     d.database,
			TablegroupId: tablegroupID,
		})
		if err != nil {
			return err
		}
		managed = reply.GetManaged()
		if c := reply.GetConnection(); c != nil {
			conn = backingconn.FromProto(c)
		}
		return nil
	})
	return conn, managed, err
}

// retryBootstrap runs attempt until it succeeds or ctx ends, each try time-bounded and followed,
// when it fails, by a growing pause. It fails only when ctx ends, joined with the failure being
// waited out. onFailure is told of each failed attempt, counted from 1.
func retryBootstrap(ctx context.Context, attempt func(context.Context) error, onFailure func(attempt int, err error)) error {
	for n := 0; ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, bootstrapAttemptTimeout)
		err := attempt(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		onFailure(n+1, err)
		// The shift is capped so a long wait cannot overflow it.
		backoff := min(bootstrapBackoffBase<<min(n, 20), bootstrapBackoffMax)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
	}
}
