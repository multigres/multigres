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

package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseShardWatchTarget(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      WatchTarget
		wantError bool
	}{
		{
			name:  "database only",
			input: "postgres",
			want:  WatchTarget{Database: "postgres"},
		},
		{
			name:  "database and tablegroup",
			input: "postgres/default",
			want: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
			},
		},
		{
			name:  "database, tablegroup, and shard",
			input: "postgres/default/80-",
			want: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "80-",
			},
		},
		{
			name:      "empty string",
			input:     "",
			wantError: true,
		},
		{
			name:      "too many parts",
			input:     "postgres/default/shard/extra",
			wantError: true,
		},
		{
			name:      "empty database",
			input:     "/default",
			wantError: true,
		},
		{
			name:      "empty tablegroup",
			input:     "postgres/",
			wantError: true,
		},
		{
			name:      "empty tablegroup with shard",
			input:     "postgres//shard",
			wantError: true,
		},
		{
			name:      "empty shard",
			input:     "postgres/default/",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseShardWatchTarget(tt.input)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShardWatchTarget_String(t *testing.T) {
	tests := []struct {
		name   string
		target WatchTarget
		want   string
	}{
		{
			name:   "database only",
			target: WatchTarget{Database: "postgres"},
			want:   "postgres",
		},
		{
			name: "database and tablegroup",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
			},
			want: "postgres/default",
		},
		{
			name: "database, tablegroup, and shard",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "80-",
			},
			want: "postgres/default/80-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.target.String()
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShardWatchTarget_Matches(t *testing.T) {
	tests := []struct {
		name           string
		target         WatchTarget
		testDB         string
		testTableGroup string
		testShard      string
		wantDatabase   bool
		wantTableGroup bool
		wantShard      bool
	}{
		{
			name:           "database level - matches all",
			target:         WatchTarget{Database: "postgres"},
			testDB:         "postgres",
			testTableGroup: "default",
			testShard:      "80-",
			wantDatabase:   true,
			wantTableGroup: true,
			wantShard:      true,
		},
		{
			name:           "database level - different database",
			target:         WatchTarget{Database: "postgres"},
			testDB:         "other",
			testTableGroup: "default",
			testShard:      "80-",
			wantDatabase:   false,
			wantTableGroup: false,
			wantShard:      false,
		},
		{
			name: "tablegroup level - matches tablegroup and shards",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
			},
			testDB:         "postgres",
			testTableGroup: "default",
			testShard:      "80-",
			wantDatabase:   true,
			wantTableGroup: true,
			wantShard:      true,
		},
		{
			name: "tablegroup level - different tablegroup",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
			},
			testDB:         "postgres",
			testTableGroup: "other",
			testShard:      "80-",
			wantDatabase:   true,
			wantTableGroup: false,
			wantShard:      false,
		},
		{
			name: "shard level - exact match",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "80-",
			},
			testDB:         "postgres",
			testTableGroup: "default",
			testShard:      "80-",
			wantDatabase:   true,
			wantTableGroup: true,
			wantShard:      true,
		},
		{
			name: "shard level - different shard",
			target: WatchTarget{
				Database:   "postgres",
				TableGroup: "default",
				Shard:      "80-",
			},
			testDB:         "postgres",
			testTableGroup: "default",
			testShard:      "40-80",
			wantDatabase:   true,
			wantTableGroup: true,
			wantShard:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDB := tt.target.MatchesDatabase(tt.testDB)
			require.Equal(t, tt.wantDatabase, gotDB, "MatchesDatabase")

			gotTG := tt.target.MatchesTableGroup(tt.testDB, tt.testTableGroup)
			require.Equal(t, tt.wantTableGroup, gotTG, "MatchesTableGroup")

			gotShard := tt.target.MatchesShard(tt.testDB, tt.testTableGroup, tt.testShard)
			require.Equal(t, tt.wantShard, gotShard, "MatchesShard")
		})
	}
}

func TestParseShardWatchTargets(t *testing.T) {
	targets := []string{
		"postgres",
		"myapp/default",
		"analytics/logs/80-",
	}

	got, err := ParseShardWatchTargets(targets)
	require.NoError(t, err)
	require.Len(t, got, 3)

	require.Equal(t, "postgres", got[0].Database)
	require.Empty(t, got[0].TableGroup)
	require.Empty(t, got[0].Shard)

	require.Equal(t, "myapp", got[1].Database)
	require.Equal(t, "default", got[1].TableGroup)
	require.Empty(t, got[1].Shard)

	require.Equal(t, "analytics", got[2].Database)
	require.Equal(t, "logs", got[2].TableGroup)
	require.Equal(t, "80-", got[2].Shard)
}

func TestParseShardWatchTargets_Error(t *testing.T) {
	targets := []string{
		"postgres",
		"invalid/too/many/parts",
	}

	_, err := ParseShardWatchTargets(targets)
	require.Error(t, err)
}
