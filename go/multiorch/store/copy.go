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
	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// DeepCopy creates a deep copy of PoolerHealth to avoid concurrent access issues.
func (p *PoolerHealth) DeepCopy() *PoolerHealth {
	if p == nil {
		return nil
	}

	copy := &PoolerHealth{
		// Copy value types directly
		Database:                       p.Database,
		TableGroup:                     p.TableGroup,
		Shard:                          p.Shard,
		Hostname:                       p.Hostname,
		TopoPoolerType:                 p.TopoPoolerType,
		ServingStatus:                  p.ServingStatus,
		LastCheckAttempted:             p.LastCheckAttempted,
		LastCheckSuccessful:            p.LastCheckSuccessful,
		LastSeen:                       p.LastSeen,
		IsUpToDate:                     p.IsUpToDate,
		IsLastCheckValid:               p.IsLastCheckValid,
		PoolerType:                     p.PoolerType,
		PrimaryLSN:                     p.PrimaryLSN,
		PrimaryReady:                   p.PrimaryReady,
		ReplicaLastReplayLSN:           p.ReplicaLastReplayLSN,
		ReplicaLastReceiveLSN:          p.ReplicaLastReceiveLSN,
		ReplicaIsWalReplayPaused:       p.ReplicaIsWalReplayPaused,
		ReplicaWalReplayPauseState:     p.ReplicaWalReplayPauseState,
		ReplicaLagMillis:               p.ReplicaLagMillis,
		ReplicaLastXactReplayTimestamp: p.ReplicaLastXactReplayTimestamp,
	}

	// Deep copy protobufs using proto.Clone
	if p.ID != nil {
		copy.ID = proto.Clone(p.ID).(*clustermetadatapb.ID)
	}

	if p.KeyRange != nil {
		copy.KeyRange = proto.Clone(p.KeyRange).(*clustermetadatapb.KeyRange)
	}

	// Deep copy map
	if p.PortMap != nil {
		copy.PortMap = make(map[string]int32, len(p.PortMap))
		for k, v := range p.PortMap {
			copy.PortMap[k] = v
		}
	}

	if p.PrimarySyncConfig != nil {
		copy.PrimarySyncConfig = proto.Clone(p.PrimarySyncConfig).(*multipoolermanagerdatapb.SynchronousReplicationConfiguration)
	}

	if p.ReplicaPrimaryConnInfo != nil {
		copy.ReplicaPrimaryConnInfo = proto.Clone(p.ReplicaPrimaryConnInfo).(*multipoolermanagerdatapb.PrimaryConnInfo)
	}

	// Deep copy slice of pointers
	if p.PrimaryConnectedFollowers != nil {
		copy.PrimaryConnectedFollowers = make([]*clustermetadatapb.ID, len(p.PrimaryConnectedFollowers))
		for i, id := range p.PrimaryConnectedFollowers {
			if id != nil {
				copy.PrimaryConnectedFollowers[i] = proto.Clone(id).(*clustermetadatapb.ID)
			}
		}
	}

	return copy
}
