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
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/metadataclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
)

// Bootstrap retry bounds. A restarted pooler may come up before the default
// primary is reachable; it retries for a bounded time and then exits so its
// supervisor restarts it. It never serves in the meantime.
var (
	bootstrapAttempts       = 8
	bootstrapAttemptTimeout = 5 * time.Second
	bootstrapBackoffBase    = 100 * time.Millisecond
	bootstrapBackoffMax     = 2 * time.Second
)

// bootstrapBackingConnection runs the first steps of an unmanaged pooler's
// bootstrap, in the order the design requires:
//
//  1. register in topology as DISABLED/UNMANAGED, before any metadata read, so
//     the pooler is visible in topology while it prepares;
//  2. discover the default primary through topology;
//  3. fetch the named backing connection from it, retrying for a bounded time.
//
// A failure returns an error and the pooler never opens. initial is the
// pooler's topology record; it must already carry identity, ports and the
// UNMANAGED mode.
func bootstrapBackingConnection(ctx context.Context, ts topoclient.Store, initial *clustermetadatapb.Multipooler, name string, source connectionSource, logger *slog.Logger) (*backingConnection, error) {
	if err := manager.RegisterPreparing(ctx, ts, initial); err != nil {
		return nil, fmt.Errorf("register preparing pooler: %w", err)
	}

	var conn *backingConnection
	err := retryBootstrap(ctx, func(ctx context.Context) error {
		var err error
		conn, err = fetchBackingConnection(ctx, source, name)
		return err
	}, func(attempt int, err error) {
		logger.WarnContext(ctx, "backing connection bootstrap attempt failed", "attempt", attempt, "error", err)
	})
	if err != nil {
		return nil, errors.Join(errors.New("backing connection bootstrap unavailable; pooler stays closed"), err)
	}
	return conn, nil
}

// warnInsecureCredentialTransport warns when a backing connection with a password was fetched
// from the default primary over a gRPC transport without TLS: the credential of the external
// database then crossed the network in the clear. The credential is not logged.
func warnInsecureCredentialTransport(ctx context.Context, logger *slog.Logger, conn *backingConnection, tls bool) {
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

func (d *defaultPrimarySource) BackingConnection(ctx context.Context, name string) (*backingConnection, error) {
	var conn *backingConnection
	err := metadataclient.WithDefaultPrimary(ctx, d.ts, d.database, d.transport, func(client multipoolerservicepb.MultipoolerServiceClient) error {
		reply, err := client.GetBackingConnection(ctx, &multipoolerservicepb.GetBackingConnectionRequest{
			Database:       d.database,
			ConnectionName: name,
		})
		if err != nil {
			return err
		}
		conn, err = backingConnectionFromProto(name, reply.GetConnection())
		return err
	})
	return conn, err
}

// backingConnectionFromProto validates a connection returned by the default
// primary. A reply for a different name is rejected.
func backingConnectionFromProto(name string, c *multipoolerservicepb.BackingConnection) (*backingConnection, error) {
	if c.GetName() != name {
		return nil, fmt.Errorf("%w: reply is for %q, want %q", errInvalidBackingConnection, c.GetName(), name)
	}
	conn := &backingConnection{
		Name:           name,
		Host:           c.GetHost(),
		Port:           int(c.GetPort()),
		Database:       c.GetDatabase(),
		User:           c.GetUser(),
		Password:       c.GetPassword(),
		SSLMode:        c.GetSslMode(),
		SSLRootCert:    c.GetSslRootCert(),
		SSLNegotiation: c.GetSslNegotiation(),
	}
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidBackingConnection, err)
	}
	return conn, nil
}

// retryBootstrap runs attempt with bounded, cancellation-aware retries.
func retryBootstrap(ctx context.Context, attempt func(context.Context) error, onFailure func(attempt int, err error)) error {
	var err error
	for n := range bootstrapAttempts {
		if err = ctx.Err(); err != nil {
			return err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, bootstrapAttemptTimeout)
		err = attempt(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		if onFailure != nil {
			onFailure(n+1, err)
		}
		if n == bootstrapAttempts-1 {
			break
		}
		backoff := min(bootstrapBackoffBase<<n, bootstrapBackoffMax)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
