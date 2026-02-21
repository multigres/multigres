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
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// HealthState contains the current health state of the pooler.
// This is used by StreamPoolerHealth to send health updates to clients.
type HealthState struct {
	// Target identifies the tablegroup, shard, and pooler type this pooler serves.
	Target *querypb.Target

	// PoolerID identifies this multipooler instance.
	PoolerID *clustermetadatapb.ID

	// ServingStatus is the current serving state of the pooler.
	ServingStatus clustermetadatapb.PoolerServingStatus

	// PrimaryObservation contains this pooler's view of who the primary is.
	// May be nil if no primary observation is available.
	PrimaryObservation *PrimaryObservation

	// RecommendedStalenessTimeout is the duration clients should use
	// to detect a stale/dead health stream.
	RecommendedStalenessTimeout time.Duration
}

// PrimaryObservation represents a pooler's view of who the primary is.
type PrimaryObservation struct {
	// PrimaryID is the ID of the pooler this node believes is the primary.
	// May be this pooler's own ID if it believes itself to be primary.
	PrimaryID *clustermetadatapb.ID

	// Term is the consensus term at which this observation was made.
	// Higher terms indicate more recent observations.
	Term int64
}

// HealthProvider provides health information for the pooler.
// This interface is implemented by the MultiPoolerManager and used by
// the gRPC service to support the StreamPoolerHealth RPC.
//
// The manager is the single source of truth for health state. This interface
// allows the gRPC service to query current state and subscribe to changes
// without duplicating state or requiring explicit update calls.
type HealthProvider interface {
	// GetHealthState returns the current health state of the pooler.
	GetHealthState(ctx context.Context) (*HealthState, error)

	// SubscribeHealth subscribes to health state changes.
	// Returns the current health state and a channel that receives updates
	// when the health state changes. The channel is closed when the context
	// is cancelled or the provider shuts down.
	//
	// Implementations should send updates on state changes and periodic
	// heartbeats to allow clients to detect stream liveness.
	SubscribeHealth(ctx context.Context) (*HealthState, <-chan *HealthState, error)
}
