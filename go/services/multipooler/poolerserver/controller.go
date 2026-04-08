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
	"github.com/multigres/multigres/go/services/multipooler/executor"
	"github.com/multigres/multigres/go/services/multipooler/pubsub"
)

// PoolerController defines the control interface for query serving.
// This follows the Vitess Controller pattern (see vitess/go/vt/vttablet/tabletserver/controller.go)
//
// The controller is responsible for:
// - Managing query serving state (SERVING, NOT_SERVING)
// - Handling query execution through the executor
// - Providing health status
//
// Read-only vs read-write behavior is determined by the PoolerType (PRIMARY vs REPLICA),
// not by the serving status. The MultiPoolerManager creates and controls the lifecycle
// of the PoolerController, similar to how TabletManager controls TabletServer in Vitess.
type PoolerController interface {
	// OnStateChange transitions the query service to match the new serving state.
	// This is called by StateManager during state transitions.
	//
	// The poolerType determines query behavior:
	//   - PRIMARY: Accept reads + writes
	//   - REPLICA: Accept reads only
	//   - DRAINED: Offline, no user queries
	//
	// The servingStatus determines whether queries are accepted at all:
	//   - SERVING: Accept queries (constrained by poolerType)
	//   - NOT_SERVING: Reject all queries
	//
	// Returns error if the transition fails.
	OnStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus) error

	// StartRequest checks whether a new request should be admitted.
	// It validates that the target's pooler type matches this pooler's actual type
	// (returning MTF01 on mismatch to trigger gateway buffering), and checks
	// serving status. During graceful shutdown, allowOnShutdown=true permits
	// requests on existing reserved connections so in-flight transactions can complete.
	StartRequest(target *query.Target, allowOnShutdown bool) error

	// AwaitStateChange blocks until the pooler's type and serving status match
	// the given targets, or ctx is cancelled. Used by the health streamer to
	// ensure the query server is ready before broadcasting the new state.
	AwaitStateChange(ctx context.Context, poolerType clustermetadatapb.PoolerType, servingStatus clustermetadatapb.PoolerServingStatus)

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
	// This is called by MultiPoolerManager during startup.
	RegisterGRPCServices()

	// SetPubSubListener sets the shared LISTEN/NOTIFY listener.
	SetPubSubListener(l *pubsub.Listener)

	// PubSubListener returns the shared LISTEN/NOTIFY listener (may be nil).
	PubSubListener() *pubsub.Listener
}

// Ensure MultiPooler implements PoolerController at compile time
var _ PoolerController = (*QueryPoolerServer)(nil)
