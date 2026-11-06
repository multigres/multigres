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

package backup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSidecarCommands_PinnedText(t *testing.T) {
	assert.Equal(t,
		"curl -sS --fail-with-body --unix-socket /run/pgbackrest-wal/wal.sock --connect-timeout 5 --max-time 600 -X POST http://pgbackrest/wal/%f",
		SidecarArchiveCommand("/run/pgbackrest-wal/wal.sock"))
	assert.Equal(t,
		`pgctld restore-wrapper /var/lib/pooler/restore.pid --rename "%p.fetch" "%p" -- curl -sS --fail-with-body --unix-socket /run/pgbackrest-wal/wal.sock --connect-timeout 5 --max-time 600 -o "%p.fetch" http://pgbackrest/wal/%f`,
		SidecarRestoreCommand("/run/pgbackrest-wal/wal.sock", "/var/lib/pooler/restore.pid"))
}

func TestSidecarCommands_QuoteTrickyPaths(t *testing.T) {
	for _, tc := range []struct{ name, socket, pidfile, wantSocket, wantPid string }{
		{"space", "/tmp/my dir/pgbackrest.sock", "/tmp/my dir/pid", `'/tmp/my dir/pgbackrest.sock'`, `'/tmp/my dir/pid'`},
		{"single quote", "/tmp/it's/pgbackrest.sock", "/tmp/it's/pid", `'/tmp/it'\''s/pgbackrest.sock'`, `'/tmp/it'\''s/pid'`},
		{"shell metachars", "/tmp/a$b;c/pgbackrest.sock", "/tmp/a`b`/pid", `'/tmp/a$b;c/pgbackrest.sock'`, "'/tmp/a`b`/pid'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, SidecarArchiveCommand(tc.socket), "--unix-socket "+tc.wantSocket+" ")
			restore := SidecarRestoreCommand(tc.socket, tc.pidfile)
			assert.Contains(t, restore, "restore-wrapper "+tc.wantPid+" --rename")
			assert.Contains(t, restore, "--unix-socket "+tc.wantSocket+" ")
		})
	}
}
