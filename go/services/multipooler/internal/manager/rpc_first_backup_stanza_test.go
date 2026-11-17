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

package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// stanzaStub is a fake pgbackrest: it answers `info` with canned JSON, fails
// stanza-create with the configured error until the stanza is deleted (or always,
// with failAlways), fails any command in failCommands, and logs each command word.
type stanzaStub struct {
	callLog    string
	listingDir string
	pm         *MultipoolerManager
}

func newStanzaStub(t *testing.T, infoJSON, createErr string, failAlways bool, failCommands ...string) *stanzaStub {
	t.Helper()
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))

	callLog := filepath.Join(tmpDir, "calls")
	infoFile := filepath.Join(tmpDir, "info.json")
	require.NoError(t, os.WriteFile(infoFile, []byte(infoJSON), 0o644))
	deletedMarker := filepath.Join(tmpDir, "deleted")

	listingDir := filepath.Join(tmpDir, "listings")
	require.NoError(t, os.MkdirAll(listingDir, 0o755))

	script := fmt.Sprintf(`#!/bin/bash
last="${@: -1}"
cmd="$last"
case " $* " in
  *" repo-ls "*) cmd=repo-ls ;;
esac
echo "$cmd" >> %[1]s
case " %[6]s " in
  *" $cmd "*) echo "ERROR: injected failure of $cmd"; exit 1 ;;
esac
case "$cmd" in
  repo-ls)
    f=%[7]q/"${last//\//_}"
    if [ -f "$f" ]; then cat "$f"; else echo '{".":{"type":"path"}}'; fi ;;
  info) cat %[2]s ;;
  stanza-delete) touch %[3]s ;;
  stanza-create)
    if [ -n %[4]q ] && { [ %[5]t = true ] || [ ! -f %[3]s ]; }; then
      echo %[4]q
      exit 1
    fi ;;
esac
exit 0
`, callLog, infoFile, deletedMarker, createErr, failAlways, strings.Join(failCommands, " "), listingDir)
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(script), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	poolerDir := filepath.Join(tmpDir, "pooler")
	pgbackrestDir := filepath.Join(poolerDir, "pgbackrest")
	require.NoError(t, os.MkdirAll(pgbackrestDir, 0o755))
	configPath := filepath.Join(pgbackrestDir, "pgbackrest.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[global]\nlog-path=/tmp/pgbackrest/logs\n"), 0o600))

	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, tmpDir)
	pm.backup.SetConfigPath(configPath)
	return &stanzaStub{callLog: callLog, listingDir: listingDir, pm: pm}
}

// listArchive sets what `repo-ls` reports for a repository path; paths never
// set list as empty.
func (s *stanzaStub) listArchive(t *testing.T, path, listing string) {
	t.Helper()
	name := strings.ReplaceAll(path, "/", "_")
	require.NoError(t, os.WriteFile(filepath.Join(s.listingDir, name), []byte(listing), 0o644))
}

func (s *stanzaStub) markBootstrap(t *testing.T) {
	t.Helper()
	require.NoError(t, s.pm.topoClient.MarkBootstrapInProgress(t.Context(), s.pm.shardRef(), "earlier-attempt"))
}

func (s *stanzaStub) bootstrapMarked(t *testing.T) bool {
	t.Helper()
	marked, err := s.pm.topoClient.BootstrapInProgress(t.Context(), s.pm.shardRef())
	require.NoError(t, err)
	return marked
}

// archiveBeyondBootstrap makes the archive look like a live shard's: one archive
// id with more WAL segments than a bootstrap leaves.
func (s *stanzaStub) archiveBeyondBootstrap(t *testing.T) {
	t.Helper()
	s.listArchive(t, "archive/multigres", `{".":{"type":"path"},"17-1":{"type":"path"}}`)
	s.listArchive(t, "archive/multigres/17-1", `{".":{"type":"path"},"0000000100000000":{"type":"path"}}`)
	segments := []string{`".":{"type":"path"}`}
	for i := 1; i <= 4; i++ {
		segments = append(segments, fmt.Sprintf(`"00000001000000000000000%d-0123456789abcdef0123456789abcdef01234567.gz":{"type":"file"}`, i))
	}
	s.listArchive(t, "archive/multigres/17-1/0000000100000000", "{"+strings.Join(segments, ",")+"}")
}

func (s *stanzaStub) calls(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(s.callLog)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return strings.Fields(string(data))
}

func stanzaInfoJSON(statusCode int, backups ...[2]string) string {
	entries := make([]string, 0, len(backups))
	for _, b := range backups {
		entries = append(entries, fmt.Sprintf(
			`{"label":%q,"error":false,"annotation":{"table_group":%q,"shard":%q},"timestamp":{"start":1700000000,"stop":1700000100}}`,
			b[0], constants.DefaultTableGroup, b[1]))
	}
	return fmt.Sprintf(`[{"name":"multigres","backup":[%s],"repo":[{"key":1,"status":{"code":%d,"message":"x"}}]}]`,
		strings.Join(entries, ","), statusCode)
}

func TestPrepareStanzaLocked(t *testing.T) {
	const (
		missingStanzaPath = 1
		noValidBackups    = 2
		ok                = 0
	)

	tests := []struct {
		name        string
		info        string
		createErr   string
		failAlways  bool
		failOn      []string
		marked      bool
		liveArchive bool
		wantFound   bool
		wantErrCode mtrpcpb.Code
		wantErr     string
		wantCalls   []string
	}{
		{
			name:      "no stanza creates it without stopping archiving",
			info:      stanzaInfoJSON(missingStanzaPath),
			wantCalls: []string{"info", "stanza-create"},
		},
		{
			name:      "stanza without backups is reset first",
			marked:    true,
			info:      stanzaInfoJSON(noValidBackups),
			wantCalls: []string{"info", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:      "complete backup of this shard skips stanza work",
			info:      stanzaInfoJSON(ok, [2]string{"20261116-100000F", constants.DefaultShard}),
			wantFound: true,
			wantCalls: []string{"info"},
		},
		{
			name:        "another shard's backups are never deleted",
			info:        stanzaInfoJSON(ok, [2]string{"20261116-100000F", "other-shard"}),
			wantErrCode: mtrpcpb.Code_FAILED_PRECONDITION,
			wantErr:     "none is a complete backup of this shard",
			wantCalls:   []string{"info"},
		},
		{
			name:      "half-created stanza reported as missing is reset after stanza-create fails",
			marked:    true,
			info:      stanzaInfoJSON(missingStanzaPath),
			createErr: "ERROR: [055]: unable to open missing file backup.info",
			wantCalls: []string{"info", "stanza-create", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:        "stale stanza after a reset is not reset twice",
			marked:      true,
			info:        stanzaInfoJSON(noValidBackups),
			createErr:   "ERROR: [028]: backup and archive info files exist but do not match the database",
			failAlways:  true,
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to create pgbackrest stanza",
			wantCalls:   []string{"info", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:        "unrelated stanza-create failure does not delete anything",
			info:        stanzaInfoJSON(missingStanzaPath),
			createErr:   "ERROR: [039]: unable to connect to repository",
			failAlways:  true,
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to create pgbackrest stanza",
			wantCalls:   []string{"info", "stanza-create"},
		},
		{
			name:        "repository that cannot be read is not touched",
			info:        stanzaInfoJSON(noValidBackups),
			failOn:      []string{"info"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to check for existing backups",
			wantCalls:   []string{"info"},
		},
		{
			name:        "only incomplete backups are never deleted",
			info:        stanzaInfoJSONWithBackups(0, stanzaBackup{"20261116-100000F", constants.DefaultShard, true}),
			wantErrCode: mtrpcpb.Code_FAILED_PRECONDITION,
			wantErr:     "none is a complete backup of this shard",
			wantCalls:   []string{"info"},
		},
		{
			name:        "stanza is not created when stop fails before the reset",
			marked:      true,
			info:        stanzaInfoJSON(noValidBackups),
			failOn:      []string{"stop"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to remove stale pgbackrest stanza",
			wantCalls:   []string{"info", "repo-ls", "stop"},
		},
		{
			name:        "stanza is not created when the delete fails",
			marked:      true,
			info:        stanzaInfoJSON(noValidBackups),
			failOn:      []string{"stanza-delete"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to remove stale pgbackrest stanza",
			wantCalls:   []string{"info", "repo-ls", "stop", "stanza-delete", "start"},
		},
		{
			name:      "stale stanza found only by stanza-create is reset after error 028",
			marked:    true,
			info:      stanzaInfoJSON(missingStanzaPath),
			createErr: "ERROR: [028]: backup and archive info files exist but do not match the database",
			wantCalls: []string{"info", "stanza-create", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:        "retry stops when its delete fails",
			marked:      true,
			info:        stanzaInfoJSON(missingStanzaPath),
			createErr:   "ERROR: [055]: unable to open missing file backup.info",
			failOn:      []string{"stanza-delete"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to remove stale pgbackrest stanza",
			wantCalls:   []string{"info", "stanza-create", "repo-ls", "stop", "stanza-delete", "start"},
		},
		{
			name:        "stanza without backups but with a live archive is not reset",
			marked:      true,
			info:        stanzaInfoJSON(noValidBackups),
			liveArchive: true,
			wantErrCode: mtrpcpb.Code_FAILED_PRECONDITION,
			wantErr:     "refusing to reset it",
			wantCalls:   []string{"info", "repo-ls", "repo-ls", "repo-ls"},
		},
		{
			name:        "retry does not reset a stanza with a live archive",
			marked:      true,
			info:        stanzaInfoJSON(missingStanzaPath),
			createErr:   "ERROR: [028]: backup and archive info files exist but do not match the database",
			liveArchive: true,
			wantErrCode: mtrpcpb.Code_FAILED_PRECONDITION,
			wantErr:     "refusing to reset it",
			wantCalls:   []string{"info", "stanza-create", "repo-ls", "repo-ls", "repo-ls"},
		},
		{
			name:        "stanza without backups is not reset when its archive cannot be listed",
			marked:      true,
			info:        stanzaInfoJSON(noValidBackups),
			failOn:      []string{"repo-ls"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to check for existing backups",
			wantCalls:   []string{"info", "repo-ls"},
		},
		{
			name:        "retry does not reset when the archive cannot be listed",
			marked:      true,
			info:        stanzaInfoJSON(missingStanzaPath),
			createErr:   "ERROR: [055]: unable to open missing file backup.info",
			failOn:      []string{"repo-ls"},
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to inspect the pgbackrest archive",
			wantCalls:   []string{"info", "stanza-create", "repo-ls"},
		},
		{
			name:        "retry returns the error when stanza-create fails again",
			marked:      true,
			info:        stanzaInfoJSON(missingStanzaPath),
			createErr:   "ERROR: [055]: unable to open missing file backup.info",
			failAlways:  true,
			wantErrCode: mtrpcpb.Code_INTERNAL,
			wantErr:     "failed to create pgbackrest stanza",
			wantCalls:   []string{"info", "stanza-create", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStanzaStub(t, tt.info, tt.createErr, tt.failAlways, tt.failOn...)
			if tt.liveArchive {
				stub.archiveBeyondBootstrap(t)
			}
			if tt.marked {
				stub.markBootstrap(t)
			}

			found, err := stub.pm.prepareStanzaLocked(t.Context())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, mterrors.Code(err))
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantCalls, stub.calls(t))
		})
	}
}

func TestPrepareStanzaLocked_BootstrapMarker(t *testing.T) {
	const (
		missingStanzaPath = 1
		noValidBackups    = 2
		ok                = 0
	)
	stale028 := "ERROR: [028]: backup and archive info files exist but do not match the database"

	tests := []struct {
		name        string
		info        string
		createErr   string
		failAlways  bool
		marked      bool
		liveArchive bool
		wantErr     string
		wantMarked  bool
		wantCalls   []string
	}{
		{
			name:       "first bootstrap is marked before the stanza is created",
			info:       stanzaInfoJSON(missingStanzaPath),
			wantMarked: true,
			wantCalls:  []string{"info", "stanza-create"},
		},
		{
			name:       "marker stays when stanza-create fails for another reason",
			info:       stanzaInfoJSON(missingStanzaPath),
			createErr:  "ERROR: [039]: unable to connect to repository",
			failAlways: true,
			wantErr:    "failed to create pgbackrest stanza",
			wantMarked: true,
			wantCalls:  []string{"info", "stanza-create"},
		},
		{
			name:       "stanza with no backups after an unfinished bootstrap is reset and stays marked",
			info:       stanzaInfoJSON(noValidBackups),
			marked:     true,
			wantMarked: true,
			wantCalls:  []string{"info", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:      "stanza with no backups and no marker is never reset",
			info:      stanzaInfoJSON(noValidBackups),
			wantErr:   "no unfinished bootstrap is recorded",
			wantCalls: []string{"info"},
		},
		{
			name:       "half-created stanza after an unfinished bootstrap is reset",
			info:       stanzaInfoJSON(missingStanzaPath),
			createErr:  stale028,
			marked:     true,
			wantMarked: true,
			wantCalls:  []string{"info", "stanza-create", "repo-ls", "stop", "stanza-delete", "start", "stanza-create"},
		},
		{
			name:      "existing stanza from no bootstrap is left alone and leaves no marker",
			info:      stanzaInfoJSON(missingStanzaPath),
			createErr: stale028,
			wantErr:   "no unfinished bootstrap is recorded",
			wantCalls: []string{"info", "stanza-create"},
		},
		{
			name:        "stanza with a marker but a live archive keeps its marker",
			info:        stanzaInfoJSON(noValidBackups),
			marked:      true,
			liveArchive: true,
			wantErr:     "refusing to reset it",
			wantMarked:  true,
			wantCalls:   []string{"info", "repo-ls", "repo-ls", "repo-ls"},
		},
		{
			name:      "complete backup clears a stale marker",
			info:      stanzaInfoJSON(ok, [2]string{"20261116-100000F", constants.DefaultShard}),
			marked:    true,
			wantCalls: []string{"info"},
		},
		{
			name:      "complete backup without a marker is fine",
			info:      stanzaInfoJSON(ok, [2]string{"20261116-100000F", constants.DefaultShard}),
			wantCalls: []string{"info"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStanzaStub(t, tt.info, tt.createErr, tt.failAlways)
			if tt.liveArchive {
				stub.archiveBeyondBootstrap(t)
			}
			if tt.marked {
				stub.markBootstrap(t)
			}

			_, err := stub.pm.prepareStanzaLocked(t.Context())

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantMarked, stub.bootstrapMarked(t))
			assert.Equal(t, tt.wantCalls, stub.calls(t))
		})
	}
}

type stanzaBackup struct {
	label  string
	shard  string
	failed bool
}

func stanzaInfoJSONWithBackups(statusCode int, backups ...stanzaBackup) string {
	entries := make([]string, 0, len(backups))
	for _, b := range backups {
		entries = append(entries, fmt.Sprintf(
			`{"label":%q,"error":%t,"annotation":{"table_group":%q,"shard":%q},"timestamp":{"start":1700000000,"stop":1700000100}}`,
			b.label, b.failed, constants.DefaultTableGroup, b.shard))
	}
	return fmt.Sprintf(`[{"name":"multigres","backup":[%s],"repo":[{"key":1,"status":{"code":%d,"message":"x"}}]}]`,
		strings.Join(entries, ","), statusCode)
}

var repositoryStateCases = []struct {
	name          string
	info          string
	marked        bool
	liveArchive   bool
	want          backupState
	wantAvailable bool
	wantUnusable  bool
}{
	{name: "stanza never created", info: stanzaInfoJSONWithBackups(1), want: noStanza},
	{name: "stanza without backups after an unfinished bootstrap", info: stanzaInfoJSONWithBackups(2), marked: true, want: emptyStanza},
	{
		name:         "stanza without backups and no unfinished bootstrap",
		info:         stanzaInfoJSONWithBackups(2),
		want:         unmarkedStanza,
		wantUnusable: true,
	},
	{
		name:         "stanza without backups but with a live archive",
		info:         stanzaInfoJSONWithBackups(2),
		marked:       true,
		liveArchive:  true,
		want:         archiveInUse,
		wantUnusable: true,
	},
	{
		name:         "only incomplete backups",
		info:         stanzaInfoJSONWithBackups(0, stanzaBackup{"20261116-100000F", constants.DefaultShard, true}),
		want:         noUsableBackup,
		wantUnusable: true,
	},
	{
		name: "backups of other shards only",
		info: stanzaInfoJSONWithBackups(0,
			stanzaBackup{"20261116-100000F", "other-1", false},
			stanzaBackup{"20261116-110000F", "other-2", false}),
		want:         noUsableBackup,
		wantUnusable: true,
	},
	{
		name:          "one complete backup",
		info:          stanzaInfoJSONWithBackups(0, stanzaBackup{"20261116-100000F", constants.DefaultShard, false}),
		want:          completeBackupAvailable,
		wantAvailable: true,
	},
	{
		name: "complete behind a newer incomplete one",
		info: stanzaInfoJSONWithBackups(0,
			stanzaBackup{"20261116-100000F", constants.DefaultShard, false},
			stanzaBackup{"20261116-110000F", constants.DefaultShard, true}),
		want:          completeBackupAvailable,
		wantAvailable: true,
	},
	{
		name: "complete backup of this shard alongside other shards",
		info: stanzaInfoJSONWithBackups(0,
			stanzaBackup{"20261116-100000F", "other-1", false},
			stanzaBackup{"20261116-110000F", constants.DefaultShard, false}),
		want:          completeBackupAvailable,
		wantAvailable: true,
	},
}

func TestRepositoryBackupState(t *testing.T) {
	for _, tt := range repositoryStateCases {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStanzaStub(t, tt.info, "", false)
			if tt.liveArchive {
				stub.archiveBeyondBootstrap(t)
			}
			if tt.marked {
				stub.markBootstrap(t)
			}

			got, err := stub.pm.repositoryBackupState(t.Context())

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDiscoverPostgresState_ClassifiesRepository(t *testing.T) {
	for _, tt := range repositoryStateCases {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStanzaStub(t, tt.info, "", false)
			if tt.liveArchive {
				stub.archiveBeyondBootstrap(t)
			}
			if tt.marked {
				stub.markBootstrap(t)
			}
			stub.pm.pgctldClient = &mockPgctldClient{
				statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_NOT_INITIALIZED},
			}

			state, err := stub.pm.discoverPostgresState(t.Context())

			require.NoError(t, err)
			assert.Equal(t, tt.wantAvailable, state.backupsAvailable)
			assert.Equal(t, tt.wantUnusable, state.backupsUnusable)
			if tt.wantUnusable {
				assert.Equal(t, reasonBackupsUnusable, stub.pm.pgMonitorLastLoggedReason)
			} else {
				assert.NotEqual(t, reasonBackupsUnusable, stub.pm.pgMonitorLastLoggedReason)
			}
		})
	}
}
