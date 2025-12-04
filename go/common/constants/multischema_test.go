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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateMVPTableGroupAndShard(t *testing.T) {
	tests := []struct {
		name        string
		tableGroup  string
		shard       string
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid default tablegroup and shard",
			tableGroup: DefaultTableGroup,
			shard:      DefaultShard,
			wantErr:    false,
		},
		{
			name:        "invalid tablegroup",
			tableGroup:  "custom",
			shard:       DefaultShard,
			wantErr:     true,
			errContains: "only default tablegroup is supported",
		},
		{
			name:        "invalid shard",
			tableGroup:  DefaultTableGroup,
			shard:       "0-100",
			wantErr:     true,
			errContains: "only shard 0-inf is supported",
		},
		{
			name:        "both invalid",
			tableGroup:  "custom",
			shard:       "0-100",
			wantErr:     true,
			errContains: "only default tablegroup is supported",
		},
		{
			name:        "empty tablegroup",
			tableGroup:  "",
			shard:       DefaultShard,
			wantErr:     true,
			errContains: "only default tablegroup is supported",
		},
		{
			name:        "empty shard",
			tableGroup:  DefaultTableGroup,
			shard:       "",
			wantErr:     true,
			errContains: "only shard 0-inf is supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMVPTableGroupAndShard(tt.tableGroup, tt.shard)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDefaultConstants(t *testing.T) {
	// Ensure the default constants have the expected values
	assert.Equal(t, "default", DefaultTableGroup)
	assert.Equal(t, "0-inf", DefaultShard)
}
