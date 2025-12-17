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
)

// PoolerController defines the control interface for query serving.
// This follows the Vitess Controller pattern (see vitess/go/vt/vttablet/tabletserver/controller.go)
//
// The controller is responsible for:
// - Managing query serving state (SERVING, NOT_SERVING, SERVING_RDONLY, DRAINED)
// - Handling query execution through the executor
// - Providing health status
//
// The MultiPoolerManager creates and controls the lifecycle of the PoolerController,
// similar to how TabletManager controls TabletServer in Vitess.
type PoolerController interface {
	// SetServingType transitions the query service to the required serving state.
	//
	// Serving states:
	//   - SERVING: Accept all queries (read and write)
	//   - NOT_SERVING: Reject all queries
	//   - SERVING_RDONLY: Accept only read queries
	//   - DRAINED: Gracefully drain existing connections, reject new queries
	//
	// Parameters:
	//   - ctx: Context for the operation
	//   - servingStatus: The target serving status
	//
	// Returns error if the transition fails.
	SetServingType(ctx context.Context, servingStatus clustermetadatapb.PoolerServingStatus) error

	// IsServing returns true if the query service is currently serving requests.
	// This returns true for both SERVING and SERVING_RDONLY states.
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

	// RegisterGRPCServices registers gRPC services with the server.
	// This is called by MultiPoolerManager during startup.
	RegisterGRPCServices()
}

// Ensure MultiPooler implements PoolerController at compile time
var _ PoolerController = (*QueryPoolerServer)(nil)
