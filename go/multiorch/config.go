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

package multiorch

import (
	"fmt"
	"strings"
)

// WatchTarget represents a target to watch in the format:
// - "database" - watch entire database
// - "database/tablegroup" - watch specific tablegroup
// - "database/tablegroup/shard" - watch specific shard
type WatchTarget struct {
	Database   string
	TableGroup string // empty if watching entire database
	Shard      string // empty if watching database or tablegroup level
}

// String returns the string representation of the target.
func (t WatchTarget) String() string {
	if t.Shard != "" {
		return fmt.Sprintf("%s/%s/%s", t.Database, t.TableGroup, t.Shard)
	}
	if t.TableGroup != "" {
		return fmt.Sprintf("%s/%s", t.Database, t.TableGroup)
	}
	return t.Database
}

// MatchesDatabase returns true if this target watches the given database.
func (t WatchTarget) MatchesDatabase(db string) bool {
	return t.Database == db
}

// MatchesTableGroup returns true if this target watches the given database/tablegroup.
// Returns true if watching entire database or specific tablegroup.
func (t WatchTarget) MatchesTableGroup(db, tablegroup string) bool {
	if t.Database != db {
		return false
	}
	// Watching entire database matches all tablegroups
	if t.TableGroup == "" {
		return true
	}
	return t.TableGroup == tablegroup
}

// MatchesShard returns true if this target watches the given database/tablegroup/shard.
// Returns true if watching entire database, entire tablegroup, or specific shard.
func (t WatchTarget) MatchesShard(db, tablegroup, shard string) bool {
	if t.Database != db {
		return false
	}
	// Watching entire database matches all shards
	if t.TableGroup == "" {
		return true
	}
	if t.TableGroup != tablegroup {
		return false
	}
	// Watching entire tablegroup matches all shards
	if t.Shard == "" {
		return true
	}
	return t.Shard == shard
}

// ParseShardWatchTarget parses a shard watch target string.
// Format: "database" or "database/tablegroup" or "database/tablegroup/shard"
//
// Validation rules:
// - Database is always required
// - If shard is provided, tablegroup must be provided
// - Empty parts are not allowed
func ParseShardWatchTarget(s string) (WatchTarget, error) {
	if s == "" {
		return WatchTarget{}, fmt.Errorf("empty shard watch target")
	}

	parts := strings.Split(s, "/")
	if len(parts) > 3 {
		return WatchTarget{}, fmt.Errorf("invalid shard watch target format: %s (expected db, db/tablegroup, or db/tablegroup/shard)", s)
	}

	// Database is always required
	if parts[0] == "" {
		return WatchTarget{}, fmt.Errorf("database cannot be empty")
	}

	target := WatchTarget{Database: parts[0]}

	// Validate and set tablegroup if provided
	if len(parts) >= 2 {
		if parts[1] == "" {
			return WatchTarget{}, fmt.Errorf("tablegroup cannot be empty when specified")
		}
		target.TableGroup = parts[1]
	}

	// Validate and set shard if provided
	if len(parts) == 3 {
		if parts[2] == "" {
			return WatchTarget{}, fmt.Errorf("shard cannot be empty when specified")
		}
		if target.TableGroup == "" {
			return WatchTarget{}, fmt.Errorf("tablegroup must be specified when shard is provided")
		}
		target.Shard = parts[2]
	}

	return target, nil
}

// ParseShardWatchTargets parses multiple shard watch target strings.
func ParseShardWatchTargets(targets []string) ([]WatchTarget, error) {
	result := make([]WatchTarget, 0, len(targets))
	for _, t := range targets {
		parsed, err := ParseShardWatchTarget(t)
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}
