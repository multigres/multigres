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

package keyrange

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestParseShardName(t *testing.T) {
	tests := []struct {
		name  string
		start []byte
		end   []byte
	}{
		{name: "0-inf"},
		{name: "0-80", end: []byte{0x80}},
		{name: "80-inf", start: []byte{0x80}},
		{name: "0-40", end: []byte{0x40}},
		{name: "40-80", start: []byte{0x40}, end: []byte{0x80}},
		{name: "8040-c0", start: []byte{0x80, 0x40}, end: []byte{0xc0}},
		{name: "80-8040", start: []byte{0x80}, end: []byte{0x80, 0x40}},
		{name: "0-0001", end: []byte{0x00, 0x01}},
		{name: "ff-inf", start: []byte{0xff}},
		{name: "0-ff01", end: []byte{0xff, 0x01}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr, err := ParseShardName(tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.start, kr.GetStart())
			assert.Equal(t, tt.end, kr.GetEnd())
			assert.Equal(t, tt.name, ShardName(kr), "round trip")
		})
	}
}

func TestParseShardName_Rejects(t *testing.T) {
	tests := []struct {
		name        string
		errContains string
	}{
		{name: "", errContains: "expected <start>-<end>"},
		{name: "0", errContains: "expected <start>-<end>"},
		{name: "0inf", errContains: "expected <start>-<end>"},
		{name: "-inf", errContains: "empty bound"},
		{name: "0-", errContains: "empty bound"},
		{name: "00-80", errContains: "must not end in a zero byte"},
		{name: "0000-80", errContains: "must not end in a zero byte"},
		{name: "8000-inf", errContains: "must not end in a zero byte"},
		{name: "0-8000", errContains: "must not end in a zero byte"},
		{name: "0-8", errContains: "even number of hex digits"},
		{name: "080-inf", errContains: "even number of hex digits"},
		{name: "0-8A", errContains: "lowercase hex"},
		{name: "0-AB", errContains: "lowercase hex"},
		{name: "0-zz", errContains: "lowercase hex"},
		{name: "80-40", errContains: "start must be less than end"},
		{name: "80-80", errContains: "start must be less than end"},
		{name: "8040-80", errContains: "start must be less than end"},
		{name: "0-0", errContains: "even number of hex digits"},
		{name: "inf-0", errContains: "start"},
		{name: "inf-inf", errContains: "start"},
		{name: "0-inf-inf", errContains: "end"},
		{name: "0-80-c0", errContains: "end"},
		{name: " 0-80", errContains: "start"},
		{name: "0-INF", errContains: "end"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr, err := ParseShardName(tt.name)
			require.Error(t, err)
			assert.Nil(t, kr)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

func TestShardName(t *testing.T) {
	tests := []struct {
		kr   *clustermetadatapb.KeyRange
		want string
	}{
		{kr: nil, want: "0-inf"},
		{kr: &clustermetadatapb.KeyRange{}, want: "0-inf"},
		{kr: &clustermetadatapb.KeyRange{Start: []byte{}, End: []byte{}}, want: "0-inf"},
		{kr: &clustermetadatapb.KeyRange{End: []byte{0x80}}, want: "0-80"},
		{kr: &clustermetadatapb.KeyRange{Start: []byte{0x80}}, want: "80-inf"},
		{kr: &clustermetadatapb.KeyRange{Start: []byte{0x80, 0x40}, End: []byte{0xc0}}, want: "8040-c0"},
		{kr: &clustermetadatapb.KeyRange{Start: []byte{0xab}, End: []byte{0xcd, 0xef}}, want: "ab-cdef"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := ShardName(tt.kr)
			assert.Equal(t, tt.want, got)

			// Every name ShardName produces for a canonical range parses back
			// to the same range.
			kr, err := ParseShardName(got)
			require.NoError(t, err)
			assert.Equal(t, got, ShardName(kr))
		})
	}
}

func TestValidateTableGroupAndShard(t *testing.T) {
	tests := []struct {
		name        string
		tableGroup  string
		shard       string
		wantErr     bool
		errContains string
	}{
		{
			name:       "default tablegroup and shard",
			tableGroup: constants.DefaultTableGroup,
			shard:      constants.DefaultShard,
		},
		{
			name:       "non-default tablegroup with full range",
			tableGroup: "commerce",
			shard:      "0-inf",
		},
		{
			name:       "non-default tablegroup with partial range",
			tableGroup: "commerce",
			shard:      "0-80",
		},
		{
			name:       "non-default tablegroup with bounded range",
			tableGroup: "commerce",
			shard:      "40-80",
		},
		{
			name:        "default tablegroup with non-default shard",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "80-inf",
			wantErr:     true,
			errContains: "tablegroup default only supports shard 0-inf",
		},
		{
			name:        "default tablegroup with empty shard",
			tableGroup:  constants.DefaultTableGroup,
			shard:       "",
			wantErr:     true,
			errContains: "tablegroup default only supports shard 0-inf",
		},
		{
			name:        "non-default tablegroup with garbage shard",
			tableGroup:  "commerce",
			shard:       "garbage",
			wantErr:     true,
			errContains: "invalid shard name",
		},
		{
			name:        "non-default tablegroup with non-canonical shard",
			tableGroup:  "commerce",
			shard:       "00-80",
			wantErr:     true,
			errContains: "invalid shard name",
		},
		{
			name:        "non-default tablegroup with empty shard",
			tableGroup:  "commerce",
			shard:       "",
			wantErr:     true,
			errContains: "invalid shard name",
		},
		{
			name:        "empty tablegroup",
			tableGroup:  "",
			shard:       constants.DefaultShard,
			wantErr:     true,
			errContains: "tablegroup is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTableGroupAndShard(tt.tableGroup, tt.shard)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
