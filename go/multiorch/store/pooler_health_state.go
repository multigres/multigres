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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdata "github.com/multigres/multigres/go/pb/multiorchdata"
)

// IsInitialized returns true if the pooler has been initialized.
// A pooler is considered initialized if:
// - It's reachable (IsLastCheckValid)
// - For PRIMARY: PrimaryStatus.Lsn is non-empty
// - For REPLICA: ReplicationStatus has non-empty LastReplayLsn or LastReceiveLsn
func IsInitialized(p *multiorchdata.PoolerHealthState) bool {
	if !p.IsLastCheckValid {
		return false // unreachable nodes are uninitialized
	}

	if p.MultiPooler == nil {
		return false
	}

	if p.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY {
		return p.PrimaryStatus != nil && p.PrimaryStatus.Lsn != ""
	}

	// For replica
	if p.ReplicationStatus != nil {
		return p.ReplicationStatus.LastReplayLsn != "" || p.ReplicationStatus.LastReceiveLsn != ""
	}
	return false
}
