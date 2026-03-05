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

package buffer

import (
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// BufferListener implements PoolerChangeListener to stop buffering when a new
// PRIMARY pooler is discovered via topology.
type BufferListener struct {
	buffer *Buffer
}

// NewBufferListener creates a BufferListener for the given buffer.
func NewBufferListener(buf *Buffer) *BufferListener {
	return &BufferListener{buffer: buf}
}

// OnPoolerChanged is called when a pooler is added or updated in topology.
// If it's a PRIMARY pooler, we stop buffering for that shard since a new
// PRIMARY is now available.
func (bl *BufferListener) OnPoolerChanged(pooler *clustermetadatapb.MultiPooler) {
	if pooler.GetType() == clustermetadatapb.PoolerType_PRIMARY {
		bl.buffer.StopBuffering(commontypes.ShardKey{
			TableGroup: pooler.GetTableGroup(),
			Shard:      pooler.GetShard(),
		})
	}
}

// OnPoolerRemoved is called when a pooler is removed from topology.
// Removal doesn't stop buffering — we only stop when a new PRIMARY appears.
func (bl *BufferListener) OnPoolerRemoved(_ *clustermetadatapb.MultiPooler) {
	// No action needed.
}
