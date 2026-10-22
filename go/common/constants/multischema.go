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

package constants

import "fmt"

// DefaultTableGroup is the tablegroup value that indicates this multipooler
// serves the default database where multischema global tables should be created.
const DefaultTableGroup = "default"

// Shard names are "<start>-<end>" (see keyrange.ParseShardName). These are the
// spellings of the two open ends of the keyspace.
const (
	// ShardNameMinBound is the shard-name spelling of the start of the keyspace.
	ShardNameMinBound = "0"
	// ShardNameMaxBound is the shard-name spelling of the end of the keyspace.
	ShardNameMaxBound = "inf"
)

// DefaultShard is the only shard of the default tablegroup: the full keyspace.
const DefaultShard = ShardNameMinBound + "-" + ShardNameMaxBound

// ValidateMVPTableGroupAndShard validates that the tablegroup and shard values
// match the MVP constraints. Returns an error if they don't.
//
// MVP Limitation: Currently, we only support the default tablegroup with shard "0-inf".
func ValidateMVPTableGroupAndShard(tableGroup, shard string) error {
	if tableGroup != DefaultTableGroup {
		return fmt.Errorf("only default tablegroup is supported, got: %s", tableGroup)
	}
	if shard != DefaultShard {
		return fmt.Errorf("only shard %s is supported for default tablegroup, got: %s", DefaultShard, shard)
	}
	return nil
}
