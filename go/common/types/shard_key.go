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

package types

import (
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ShardKeyString is the stable string representation of a shard key.
// Use as map keys, log fields, and error context.
type ShardKeyString string

// FormatShardKey returns the ShardKeyString for a proto ShardKey.
func FormatShardKey(k *clustermetadatapb.ShardKey) ShardKeyString {
	if k == nil {
		return ""
	}
	return ShardKeyString(fmt.Sprintf("%s/%s/%s", k.Database, k.TableGroup, k.Shard))
}
