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

package manager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/sandbox"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// sidecarMode is a restart option that points the config at the sidecar.
func sidecarMode(cfg *Config, _ *testutil.MockPgCtldService, poolerDir string) {
	applyTestSidecar(cfg, poolerDir)
}

// newNormalizeManager returns a manager whose postgresql.auto.conf holds seed.
func newNormalizeManager(t *testing.T, mode restartTestOption, seed string) (*MultipoolerManager, string) {
	t.Helper()
	pm, autoConf, _ := newRestartAsStandbyTestManager(t, mock.NewQueryService(), seed, mode)
	return pm, autoConf
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(b)
}

// autoConfValue returns the unquoted value of the last line setting name.
func autoConfValue(t *testing.T, content, name string) (string, bool) {
	t.Helper()
	var out string
	found := false
	for line := range strings.SplitSeq(content, "\n") {
		if !autoConfLineSets(line, name) {
			continue
		}
		_, v, ok := strings.Cut(line, "=")
		require.True(t, ok, line)
		v = strings.TrimSpace(v)
		require.True(t, strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'"), line)
		out = strings.ReplaceAll(v[1:len(v)-1], "''", "'")
		found = true
	}
	return out, found
}

func quoteConf(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// legacyArchiveCommand and legacyRestoreCommand are what a pod that ran
// pgBackRest in-process writes into postgresql.auto.conf: a data directory
// carried over from one names them.
func legacyArchiveCommand(configPath string) string {
	return "pgbackrest --stanza=multigres --config=" + configPath + " archive-push %p"
}

func legacyRestoreCommand(configPath, pidFile string) string {
	return "pgctld restore-wrapper '" + pidFile + "' -- pgbackrest --stanza='multigres' --config='" + configPath + `' archive-get %f "%p"`
}

func foreignLegacySeed(otherDir string) string {
	cfg := filepath.Join(otherDir, "pgbackrest", "pgbackrest.conf")
	return "shared_buffers = '128MB'\n" +
		"archive_command = " + quoteConf(legacyArchiveCommand(cfg)) + "\n" +
		"restore_command = " + quoteConf(legacyRestoreCommand(cfg, filepath.Join(otherDir, constants.RestoreCommandPIDFile))) + "\n"
}

func TestCommandsFor_Text(t *testing.T) {
	assert.Equal(t,
		`curl -sS --fail-with-body --unix-socket /run/pgbackrest-wal/wal.sock --connect-timeout 5 --max-time 600 -X POST http://pgbackrest/wal/%f`,
		commonbackup.SidecarArchiveCommand(testWALSocket))
	assert.Equal(t,
		`pgctld restore-wrapper /p/restore_command.pid --rename "%p.fetch" "%p" -- curl -sS --fail-with-body --unix-socket /run/pgbackrest-wal/wal.sock --connect-timeout 5 --max-time 600 -o "%p.fetch" http://pgbackrest/wal/%f`,
		commonbackup.SidecarRestoreCommand(testWALSocket, "/p/restore_command.pid"))
}

// pgBackRest's backup refuses an archive_command that does not contain
// "pgbackrest" ([068]); the sidecar command passes only because the socket path
// and URL carry the name.
func TestArchiveCommand_ContainsPgbackrest(t *testing.T) {
	assert.Contains(t, commonbackup.SidecarArchiveCommand(testWALSocket), "pgbackrest")
	// With a path that does not contain it, the URL host still does.
	assert.Contains(t, commonbackup.SidecarArchiveCommand("/run/w/s.sock"), "pgbackrest")
}

func TestCommandsFor_QuotesUnsafeSocketPath(t *testing.T) {
	got := commonbackup.SidecarArchiveCommand("/run/it's here/wal.sock")
	assert.Contains(t, got, `--unix-socket '/run/it'\''s here/wal.sock' `)
}

// The sidecar writes the same commands (its restore leaves a restore_command
// behind) from the same renderer, so tricky paths come out identical.
func TestCommandsFor_MatchTheSharedRenderer(t *testing.T) {
	for _, dir := range []string{"/run/w", "/tmp/with space", "/tmp/it's here", "/tmp/a$b;`c`"} {
		sock, pid := dir+"/wal.sock", dir+"/restore.pid"
		assert.Equal(t, commonbackup.SidecarArchiveCommand(sock), commonbackup.SidecarArchiveCommand(sock), dir)
		assert.Equal(t, commonbackup.SidecarRestoreCommand(sock, pid), commonbackup.SidecarRestoreCommand(sock, pid), dir)
	}
}

func TestNormalize_ConvertsLegacyToSidecar(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	require.NoError(t, os.WriteFile(autoConf, []byte(foreignLegacySeed(t.TempDir())), 0o600))

	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))

	got := readFile(t, autoConf)
	arch, ok := autoConfValue(t, got, "archive_command")
	require.True(t, ok)
	assert.Equal(t, commonbackup.SidecarArchiveCommand(testWALSocket), arch)
	rest, ok := autoConfValue(t, got, "restore_command")
	require.True(t, ok)
	assert.Equal(t, commonbackup.SidecarRestoreCommand(testWALSocket,
		filepath.Join(pm.record.PoolerDir(), constants.RestoreCommandPIDFile)), rest)
	assert.Contains(t, got, "shared_buffers = '128MB'", "unrelated settings are untouched")
}

func TestNormalize_NeverAddsRestoreCommand(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	other := filepath.Join(filepath.Dir(filepath.Dir(pm.record.PoolerDir())), "pooler-9", "data")
	seed := "archive_command = " + quoteConf(legacyArchiveCommand(other+"/pgbackrest.conf")) + "\n"
	require.NoError(t, os.WriteFile(autoConf, []byte(seed), 0o600))

	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))

	got := readFile(t, autoConf)
	assert.NotContains(t, got, "restore_command")
	_, ok := autoConfValue(t, got, "archive_command")
	assert.True(t, ok, "archive_command is rewritten, not dropped")
}

func TestNormalize_NeverAddsAnything(t *testing.T) {
	seed := "shared_buffers = '128MB'\n"
	pm, autoConf := newNormalizeManager(t, sidecarMode, seed)
	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
	assert.Equal(t, seed, readFile(t, autoConf))
}

func TestNormalize_RewritesForeignSocketPath(t *testing.T) {
	foreign := commonbackup.SidecarArchiveCommand("/tmp/other-pooler/run-wal/wal.sock") + "\n"
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	require.NoError(t, os.WriteFile(autoConf, []byte("archive_command = "+quoteConf(strings.TrimSpace(foreign))+"\n"), 0o600))

	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))

	arch, _ := autoConfValue(t, readFile(t, autoConf), "archive_command")
	assert.Equal(t, commonbackup.SidecarArchiveCommand(testWALSocket), arch)
}

func TestNormalize_LeavesHandWrittenCommandsAlone(t *testing.T) {
	seed := "archive_command = 'cp %p /mnt/archive/%f'\nrestore_command = 'cp /mnt/archive/%f %p'\n"
	pm, autoConf := newNormalizeManager(t, sidecarMode, seed)
	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
	assert.Equal(t, seed, readFile(t, autoConf))
}

func TestNormalize_NoAutoConfIsNotAnError(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	require.NoError(t, os.Remove(autoConf))
	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
}

func TestNormalize_Idempotent(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	require.NoError(t, os.WriteFile(autoConf, []byte(foreignLegacySeed(t.TempDir())), 0o600))
	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
	first := readFile(t, autoConf)
	info1, err := os.Stat(autoConf)
	require.NoError(t, err)
	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
	assert.Equal(t, first, readFile(t, autoConf))
	info2, err := os.Stat(autoConf)
	require.NoError(t, err)
	assert.True(t, os.SameFile(info1, info2), "a no-op normalisation must not rewrite the file")
}

func TestEditAutoConf_RefusesSymlink(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	outside := filepath.Join(t.TempDir(), "secret")
	const secret = "archive_command = 'pgbackrest archive-push'\n-----secret-----\n"
	require.NoError(t, os.WriteFile(outside, []byte(secret), 0o600))
	require.NoError(t, os.Remove(autoConf))
	require.NoError(t, os.Symlink(outside, autoConf))

	require.Error(t, pm.normalizeArchiveCommands(t.Context()))
	require.Error(t, pm.setAutoConfSetting(t.Context(), "primary_conninfo", "host=h"))
	require.Error(t, pm.dropAutoConfSettings(t.Context(), "restore_command"))

	assert.Equal(t, secret, readFile(t, outside), "the symlink target is untouched")
	info, err := os.Lstat(autoConf)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the link itself is not replaced")
}

// postgres can swap PGDATA's parent for a link too, not only the file.
func TestEditAutoConf_RefusesSymlinkedPgData(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "primary_conninfo = 'host=a'\n")
	pgData := filepath.Dir(autoConf)
	decoy := filepath.Join(t.TempDir(), "decoy")
	require.NoError(t, os.MkdirAll(decoy, 0o700))
	const planted = "primary_conninfo = 'host=planted'\n"
	require.NoError(t, os.WriteFile(filepath.Join(decoy, "postgresql.auto.conf"), []byte(planted), 0o600))
	require.NoError(t, os.Rename(pgData, pgData+".real"))
	require.NoError(t, os.Symlink(decoy, pgData))

	require.Error(t, pm.setAutoConfSetting(t.Context(), "primary_conninfo", "host=b"))
	assert.Equal(t, planted, readFile(t, filepath.Join(decoy, "postgresql.auto.conf")))
}

func TestEditAutoConf_KeepsModeAndLeavesNoTempFiles(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "")
	require.NoError(t, os.WriteFile(autoConf, []byte("a = 'b'\n"), 0o640))
	require.NoError(t, os.Chmod(autoConf, 0o640))
	require.NoError(t, pm.setAutoConfSetting(t.Context(), "primary_conninfo", "host=h"))
	info, err := os.Stat(autoConf)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	entries, err := os.ReadDir(filepath.Dir(autoConf))
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), "tmp", "temp file left behind")
	}
}

func TestEditAutoConf_SidecarRequiresMountRoot(t *testing.T) {
	// The zero AnchorMode demands a mount point; a temp dir is not one.
	pm, _ := newNormalizeManager(t, func(cfg *Config, m *testutil.MockPgCtldService, d string) {
		sidecarMode(cfg, m, d)
		cfg.DataVolumeAnchor = sandbox.AnchorMount
	}, "primary_conninfo = 'host=a'\n")
	require.Error(t, pm.setAutoConfSetting(t.Context(), "primary_conninfo", "host=b"))
}

// The demote path strips restore_command for a cohort member; the start that
// follows must not bring it back.
func TestDemoteThenStart_RestoreCommandStaysRemoved(t *testing.T) {
	mockQS := mock.NewQueryService()
	expectRewindPositionFloorMocks(mockQS)
	mockQS.AddQueryPattern("SELECT 1", mock.MakeQueryResult([]string{"?column?"}, [][]any{{1}}))
	mockQS.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQS.AddQueryPattern("ALTER SYSTEM SET primary_conninfo", mock.MakeQueryResult(nil, nil))
	mockQS.AddQueryPatternOnce("pg_conf_load_time", mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067200"}}))
	mockQS.AddQueryPattern("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	mockQS.AddQueryPattern("pg_conf_load_time", mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067201"}}))

	rewind := func(_ *Config, m *testutil.MockPgCtldService, _ string) {
		m.PgRewindResponse = &pgctldpb.PgRewindResponse{
			Message: "pg_rewind completed",
			Output:  "servers diverged at WAL location 0/5000000 on timeline 1",
		}
	}
	both := func(cfg *Config, m *testutil.MockPgCtldService, d string) { sidecarMode(cfg, m, d); rewind(cfg, m, d) }
	// pg_rewind copies the source's auto.conf: another pooler's commands,
	// restore_command included.
	pm, autoConf, atRestart := newRestartAsStandbyTestManager(t, mockQS, "", both)
	// A path normalisation recognises as another pooler's.
	other := filepath.Join(filepath.Dir(filepath.Dir(pm.record.PoolerDir())), "pooler-9", "data")
	require.NoError(t, os.WriteFile(autoConf, []byte(foreignLegacySeed(other)), 0o600))

	source := &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "source-pooler"},
		Hostname: "source-host",
		PortMap:  map[string]int32{"postgres": 5433},
	}
	// With an engine, runPgRewind installs a temporary restore_command and
	// drops it again on return, which would remove the copied one as well
	// and hide whether the post-rewind drop works. Without an engine it
	// does neither, so only the post-rewind drop can strip the seeded line.
	engine := pm.backup
	pm.backup = nil
	performed, err := rewindAsStandbyForTest(t, pm, source)
	pm.backup = engine
	require.NoError(t, err)
	require.True(t, performed)
	assert.NotContains(t, *atRestart, "restore_command", "demote removes restore_command before restart")
	_, hasArchive := autoConfValue(t, *atRestart, "archive_command")
	assert.True(t, hasArchive)

	require.NoError(t, pm.normalizeArchiveCommands(t.Context()))
	got := readFile(t, autoConf)
	assert.NotContains(t, got, "restore_command", "pre-start normalisation must not re-add it")
	arch, _ := autoConfValue(t, got, "archive_command")
	assert.Equal(t, pm.backup.ArchiveCommand(), arch)
}

// A symlink swapped in between the read and the mode lookup must fail the edit
// instead of lending its 0777 mode to the rewritten file.
func TestEditAutoConf_SymlinkSwappedBeforeModeLookupFails(t *testing.T) {
	pm, autoConf := newNormalizeManager(t, sidecarMode, "a = 'b'\n")
	outside := filepath.Join(t.TempDir(), "target")
	require.NoError(t, os.WriteFile(outside, []byte("t\n"), 0o600))
	testHookAutoConfBeforePerm = func() {
		require.NoError(t, os.Remove(autoConf))
		require.NoError(t, os.Symlink(outside, autoConf))
	}
	t.Cleanup(func() { testHookAutoConfBeforePerm = nil })

	require.Error(t, pm.setAutoConfSetting(t.Context(), "primary_conninfo", "host=h"))
	assert.Equal(t, "t\n", readFile(t, outside))
	info, err := os.Lstat(autoConf)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink)
}
