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

package poolerserver

import (
	"context"

	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/notificationpid"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// PoolerController defines the control interface for query serving.
// This follows the Vitess Controller pattern (see vitess/go/vt/vttablet/tabletserver/controller.go)
//
// The controller is responsible for:
// - Managing query serving state (SERVING, not-serving)
// - Handling query execution through the executor
// - Providing health status
//
// Read-only vs read-write behavior is determined by the PoolerType (PRIMARY vs REPLICA),
// not by the serving status. The MultipoolerManager creates and controls the lifecycle
// of the PoolerController, similar to how TabletManager controls TabletServer in Vitess.
type PoolerController interface {
	// OnStateChange transitions the query service to match the new serving state.
	// This is called by StateManager during state transitions.
	//
	// state.RoutingRole is the write-safety role. The query server admits leader-
	// bound traffic (WRITABLE and CONSISTENT) only when it is PRIMARY (out of
	// recovery AND the non-revoked committed + highest-known leader), which closes
	// the window between pg_promote() and the new rule committing to WAL. Sidecar
	// writers use the same role to avoid mutations on replicas/standbys.
	//
	// state.ServingStatus determines whether queries are accepted at all:
	//   - SERVING: Accept queries
	//   - not-serving: Reject all queries
	//
	// Returns error if the transition fails.
	OnStateChange(ctx context.Context, state servingstate.State) error

	// StartRequest checks whether a request should be admitted, based on its
	// RequestKind and the pooler's drain phase. It returns MTF01 (which the
	// gateway buffers and retries on the new primary) when rejected. Existing
	// reserved-connection ops are always admitted (the connection's existence is
	// the real gate); during a graceful drain, single queries are served until
	// the reserved pool has drained, while new reservations are rejected
	// throughout. See StartRequest for the full admission matrix.
	StartRequest(target *query.Target, kind RequestKind) error

	// AwaitStateChange blocks until the pooler's routing role and serving status
	// match the given targets, or ctx is cancelled. Used by the health streamer to
	// ensure the query server is ready before broadcasting the new state.
	AwaitStateChange(ctx context.Context, routingRole servingstate.RoutingRole, servingStatus clustermetadatapb.PoolerServingStatus)

	// IsServing returns true if the query service is currently serving requests.
	IsServing() bool

	// IsHealthy returns nil if the controller is healthy and able to serve queries.
	// Returns an error describing the problem if unhealthy.
	//
	// Health checks typically include:
	//   - Database connection is alive
	//   - Controller is initialized
	//   - No internal errors
	IsHealthy() error

	// Executor returns the query executor for handling queries.
	// Returns an error if the controller is not initialized or not opened.
	Executor() (queryservice.QueryService, error)

	// InternalQueryService returns an InternalQueryService for simple internal queries.
	// This is used by internal components like heartbeat that need to execute
	// queries using the connection pool.
	InternalQueryService() executor.InternalQueryService

	// RegisterGRPCServices registers gRPC services with the server.
	// This is called by MultipoolerManager during startup.
	RegisterGRPCServices()

	// SetPubSubListener sets the shared LISTEN/NOTIFY listener.
	SetPubSubListener(l *pubsub.Listener)

	// PubSubListener returns the shared LISTEN/NOTIFY listener (may be nil).
	PubSubListener() *pubsub.Listener

	// NotificationPIDMapper returns the mapper shared by query execution and
	// pubsub for NotificationResponse PID rewriting.
	NotificationPIDMapper() *notificationpid.Mapper
}

// Ensure Multipooler implements PoolerController at compile time
var _ PoolerController = (*QueryPoolerServer)(nil)
