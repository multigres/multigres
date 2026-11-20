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
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// repositoryInfoJSON builds a `pgbackrest info` document with the given repo
// status code and backups (label, shard).
func repositoryInfoJSON(statusCode int, backups ...[2]string) string {
	entries := make([]string, 0, len(backups))
	for _, b := range backups {
		entries = append(entries, fmt.Sprintf(
			`{"label":%q,"type":"full","error":false,"annotation":{"table_group":"tg1","shard":%q},"timestamp":{"start":1700000000,"stop":1700000100}}`,
			b[0], b[1]))
	}
	return fmt.Sprintf(`[{"name":"multigres","backup":[%s],"repo":[{"key":1,"status":{"code":%d,"message":"x"}}]}]`,
		strings.Join(entries, ","), statusCode)
}

func TestRepositoryInfo(t *testing.T) {
	tests := []struct {
		name        string
		json        string
		wantExists  bool
		wantTotal   int
		wantBackups int
	}{
		{name: "stanza never created", json: repositoryInfoJSON(commonbackup.RepoStatusMissingStanzaPath)},
		{name: "stanza without backups", json: repositoryInfoJSON(commonbackup.RepoStatusNoValidBackups), wantExists: true},
		{name: "stanza dir without info files", json: repositoryInfoJSON(commonbackup.RepoStatusMissingStanzaData), wantExists: true},
		{name: "no stanza entry in the output", json: "[]", wantExists: true},
		{
			name:        "this shard's backup",
			json:        repositoryInfoJSON(commonbackup.RepoStatusOK, [2]string{"20261116-100000F", "0"}),
			wantExists:  true,
			wantTotal:   1,
			wantBackups: 1,
		},
		{
			name:       "only another shard's backups are not listed but are counted",
			json:       repositoryInfoJSON(commonbackup.RepoStatusOK, [2]string{"20261116-100000F", "1"}, [2]string{"20261116-110000F", "2"}),
			wantExists: true,
			wantTotal:  2,
		},
		{
			name:        "this shard's backup among other shards'",
			json:        repositoryInfoJSON(commonbackup.RepoStatusOK, [2]string{"20261116-100000F", "0"}, [2]string{"20261116-110000F", "1"}),
			wantExists:  true,
			wantTotal:   2,
			wantBackups: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, pgbackrestInfoStub(tt.json))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")

			info, err := e.RepositoryInfo(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tt.wantExists, info.StanzaExists)
			assert.Equal(t, tt.wantTotal, info.TotalBackups)
			assert.Len(t, info.Backups, tt.wantBackups)
		})
	}
}

// TestRepositoryInfo_ErrorWhenInfoGivesNoPositiveAnswer covers failed info runs.
// They must be errors, never an empty result, because an empty result leads to
// deleting the stanza.
func TestRepositoryInfo_ErrorWhenInfoGivesNoPositiveAnswer(t *testing.T) {
	tests := []struct {
		name   string
		script string
	}{
		{name: "nonzero exit with no output", script: "#!/bin/bash\nexit 1\n"},
		{name: "killed by a signal with no output", script: "#!/bin/bash\nkill -9 $$\n"},
		{
			name:   "nonzero exit with an unrelated error",
			script: "#!/bin/bash\necho \"ERROR: [039]: unable to connect to repository\" >&2\nexit 1\n",
		},
		{
			name:   "nonzero exit with text that looks like a missing stanza",
			script: "#!/bin/bash\necho \"ERROR: [055]: unable to open missing file '/x/pgbackrest.conf' for read\" >&2\nexit 1\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, tt.script)
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")

			_, err := e.RepositoryInfo(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "pgbackrest info failed")
		})
	}
}

// TestRepositoryInfo_ErrorWhenInfoTimesOut checks an info killed by the expired
// context is an error, not an empty result.
func TestRepositoryInfo_ErrorWhenInfoTimesOut(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\nexec sleep 30\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	_, err := e.RepositoryInfo(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest info failed")
}

func TestRepositoryInfo_ErrorsWithoutConfig(t *testing.T) {
	t.Run("backup config not loaded", func(t *testing.T) {
		poolerDir := t.TempDir()
		e, _ := newTestEngine(t, poolerDir, "tg1", "0", "")

		_, err := e.RepositoryInfo(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backup config not loaded")
	})
}

// TestRepositoryInfo_ErrorWhenOutputCannotBeTrusted covers info runs that exit 0
// but leave the repository state unknown. They must be errors.
func TestRepositoryInfo_ErrorWhenOutputCannotBeTrusted(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		errContains string
	}{
		{name: "output that is not JSON", output: "not json", errContains: "failed to parse pgbackrest info JSON"},
		{name: "repository status other than ok", output: repositoryInfoJSON(99), errContains: "pgbackrest repo 1 status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, pgbackrestInfoStub(tt.output))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")

			_, err := e.RepositoryInfo(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

// RepositoryInfo reads the repository once, uncached, through one sidecar info
// whose call carries the InfoTimeout deadline (the sidecar derives --io-timeout
// from it), and never accepts output the sidecar had to drop.
func TestRepositoryInfo_SidecarCallIsBoundedAndNeverTruncated(t *testing.T) {
	fake := &fakeSidecar{reply: func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
		_, _ = io.WriteString(stdout, repositoryInfoJSON(commonbackup.RepoStatusOK, [2]string{"20261116-100000F", "0"}))
		return 0
	}}
	e := newSidecarTestEngine(t, fake)

	info, err := e.RepositoryInfo(t.Context())
	require.NoError(t, err)
	assert.True(t, info.StanzaExists)
	require.Len(t, fake.calls, 1)
	assert.Equal(t, pb.Command_COMMAND_INFO, fake.calls[0].cmd)
	assert.True(t, fake.calls[0].hasDeadline)

	truncated := &fakeSidecar{err: ErrOutputTruncated, reply: fake.reply}
	e = newSidecarTestEngine(t, truncated)
	_, err = e.RepositoryInfo(t.Context())
	require.Error(t, err)
}

// Every info the engine sends carries a deadline, whichever caller asked, so the
// sidecar can give pgBackRest an --io-timeout that ends before the context does.
func TestInfoCallsAlwaysCarryADeadline(t *testing.T) {
	fake := &fakeSidecar{reply: func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
		_, _ = io.WriteString(stdout, repositoryInfoJSON(commonbackup.RepoStatusOK))
		return 0
	}}
	e := newSidecarTestEngine(t, fake)

	_, err := e.ListBackups(t.Context())
	require.NoError(t, err)
	_, err = e.RepositoryInfo(t.Context())
	require.NoError(t, err)
	_, err = e.runInfoCmd(t.Context())
	require.NoError(t, err)

	require.Len(t, fake.calls, 3)
	for i, c := range fake.calls {
		assert.Equal(t, pb.Command_COMMAND_INFO, c.cmd)
		assert.True(t, c.hasDeadline, "info call %d", i)
	}
}
