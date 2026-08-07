// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardsetup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseOrphanedShmIDs(t *testing.T) {
	// Real macOS `ipcs -mo` layout: leading type column, NATTCH last.
	macOS := `IPC status from <running system> as of Mon Jul 27 09:34:41 CEST 2026
T     ID     KEY        MODE       OWNER    GROUP NATTCH
Shared Memory:
m 262144 0x00145a28 --rw-------     mats    staff      9
m 2949121 0x02ab1812 --rw-------     mats    staff      0
m 5373954 0x02ab21c2 --rw-------     other   staff      0
`

	// Real Linux `ipcs -m` layout: no type column, NATTCH is the 6th field, an
	// optional status column may follow, and IPC_PRIVATE segments have key 0.
	linux := `------ Shared Memory Segments --------
key        shmid      owner  perms  bytes    nattch  status
0x0052e2c1 32768      mats   600    524288   0
0x0052e2c2 32769      mats   600    524288   2
0x0052e2c3 65538      mats   600    524288   0       dest
0x00000000 98307      mats   600    524288   0
0x0052e2c4 131076     other  600    524288   0
`

	tests := []struct {
		name            string
		out             string
		operatingSystem string
		owner           string
		want            []string
	}{
		{
			name:            "macOS reaps owner's unattached, keeps attached and other owners",
			out:             macOS,
			operatingSystem: "darwin",
			owner:           "mats",
			want:            []string{"2949121"}, // 262144 attached (9), 5373954 other owner
		},
		{
			name:            "linux reaps owner's unattached keyed, skips attached/private/other-owner",
			out:             linux,
			operatingSystem: "linux",
			owner:           "mats",
			// 32768 (nattch 0) and 65538 (nattch 0, status dest) qualify.
			// 32769 attached, 98307 IPC_PRIVATE (key 0), 131076 other owner.
			want: []string{"32768", "65538"},
		},
		{
			name:            "no matches for unknown owner",
			out:             linux,
			operatingSystem: "linux",
			owner:           "nobody",
			want:            nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseOrphanedShmIDs(tt.out, tt.operatingSystem, tt.owner)
			assert.Equal(t, tt.want, got)
		})
	}
}
