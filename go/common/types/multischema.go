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

import "fmt"

// DefaultTableGroup is the tablegroup value that indicates this multipooler
// serves the default database where multischema global tables should be created.
const DefaultTableGroup = "default"

// DefaultShard is the only shard supported for the default tablegroup in the MVP.
const DefaultShard = "0-inf"

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
