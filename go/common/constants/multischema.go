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

// IsCatalogShard reports whether the given tablegroup and shard identify the
// shard that hosts the multischema catalog tables (tablegroup,
// tablegroup_table, shard): the default tablegroup's only shard.
func IsCatalogShard(tableGroup, shard string) bool {
	return tableGroup == DefaultTableGroup && shard == DefaultShard
}
