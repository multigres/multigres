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

package store

import (
	"time"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// PoolerHealth represents runtime state of a MultiPooler instance.
// This stores:
// - The MultiPooler record from topology
// - Timestamps for staleness detection
// - Flattened health metrics from Status RPC
// - Computed fields for quick access
type PoolerHealth struct {
	// MultiPooler record from topology service
	MultiPooler *clustermetadata.MultiPooler

	// Timestamps (critical for staleness detection)
	LastCheckAttempted  time.Time
	LastCheckSuccessful time.Time
	LastSeen            time.Time

	// Computed fields (cached)
	IsUpToDate       bool
	IsLastCheckValid bool

	// Health status from Status RPC (populated after successful health check)
	// This is the type the pooler reports itself as, which may differ from
	// the topology type if there's a failover in progress or type mismatch.
	PoolerType clustermetadata.PoolerType

	// Primary-specific fields (populated when PoolerType == PRIMARY)
	PrimaryLSN                string                                                        // Current WAL LSN position (PostgreSQL format: X/XXXXXXXX)
	PrimaryReady              bool                                                          // Whether server is accepting connections
	PrimaryConnectedFollowers []*clustermetadata.ID                                         // Follower servers currently connected via replication
	PrimarySyncConfig         *multipoolermanagerdatapb.SynchronousReplicationConfiguration // Sync replication config

	// Replica-specific fields (populated when PoolerType == REPLICA)
	ReplicaLastReplayLSN           string                                    // Last WAL position replayed during recovery (X/XXXXXXXX)
	ReplicaLastReceiveLSN          string                                    // Last WAL position received and synced to disk (X/XXXXXXXX)
	ReplicaIsWalReplayPaused       bool                                      // Result of pg_is_wal_replay_paused()
	ReplicaWalReplayPauseState     string                                    // Result of pg_get_wal_replay_pause_state()
	ReplicaLagMillis               int64                                     // Replication lag in milliseconds (0 if not available)
	ReplicaLastXactReplayTimestamp string                                    // Result of pg_last_xact_replay_timestamp()
	ReplicaPrimaryConnInfo         *multipoolermanagerdatapb.PrimaryConnInfo // Primary connection info (includes primary hostname/port)
}
