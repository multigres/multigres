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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// fakeIdentity is a test Identity for the backup engine. It exposes a fixed
// pooler ID/shard.
type fakeIdentity struct {
	id       *clustermetadatapb.ID
	shardRef *clustermetadatapb.ShardRef
}

func (f *fakeIdentity) Id() *clustermetadatapb.ID             { return f.id }
func (f *fakeIdentity) ShardRef() *clustermetadatapb.ShardRef { return f.shardRef }

// newTestEngine builds a backup engine with a fake identity, optionally
// resolving a repo config from the given backup location.
func newTestEngine(t *testing.T, poolerDir, tableGroup, shard, backupLocation string) (*Engine, *fakeIdentity) {
	t.Helper()

	database := "test-database"
	if tableGroup == "" {
		tableGroup = constants.DefaultTableGroup
	}
	if shard == "" {
		shard = constants.DefaultShard
	}

	id := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-multipooler",
	}
	fid := &fakeIdentity{
		id: id,
		shardRef: &clustermetadatapb.ShardRef{
			TableGroup: tableGroup,
			Shard:      shard,
			Database:   database,
		},
	}

	var backupConfig *commonbackup.Config
	if backupLocation != "" {
		backupConfig, _ = commonbackup.NewConfig(utils.FilesystemBackupLocation(backupLocation))
	}

	logger := slog.Default()
	e := NewEngine(logger, fid, Settings{
		PgDataDir:      poolerDir,
		Sidecar:        execSidecar{},
		ArchiveCommand: func() string { return "archive-command" },
		RestoreCommand: func(pidFile string) string { return "restore-command " + pidFile },
	})
	if backupConfig != nil {
		e.SetBackupConfig(backupConfig)
	}
	return e, fid
}

// execSidecar is a SidecarRunner that runs the `pgbackrest` stub on PATH with
// the argv the real sidecar renders (the stanza first, the subcommand last), so
// engine tests can script pgBackRest's output and exit code with a shell
// script.
type execSidecar struct{}

func (execSidecar) Run(ctx context.Context, cmd pb.Command, args []string, stdout, stderr io.Writer) (int, error) {
	var sub string
	for name, c := range sidecarCommands {
		if c == cmd {
			sub = name
		}
	}
	argv := append([]string{"--stanza=" + stanzaName}, args...)
	argv = append(argv, sub)
	c := executil.Command(ctx, "pgbackrest", argv...)
	c.Stdout, c.Stderr = stdout, stderr
	if err := c.Start(); err != nil {
		return 0, err
	}
	if err := c.Wait(); err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			return ee.ExitCode(), nil
		}
		return 0, err
	}
	return 0, nil
}

func (execSidecar) Status(context.Context) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{}, nil
}

// stubPgbackrest installs a fake `pgbackrest` executable on PATH for the test,
// so engine methods that shell out can be exercised without real pgbackrest.
func stubPgbackrest(t *testing.T, script string) {
	t.Helper()
	binDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(script), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
}

// pgbackrestInfoStub returns a stub script that emits the given JSON for the
// `info` subcommand and exits 0 for any other subcommand.
func pgbackrestInfoStub(jsonOutput string) string {
	return "#!/bin/bash\nif [[ \"$*\" == *\"info\"* ]]; then\ncat <<'JSONEOF'\n" + jsonOutput + "\nJSONEOF\nexit 0\nfi\nexit 0\n"
}

func TestFindByJobID(t *testing.T) {
	tests := []struct {
		name          string
		jobID         string
		jsonOutput    string
		wantBackupID  string
		wantError     bool
		errorContains string
	}{
		{
			name:  "Single matching backup",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-100000F",
			wantError:    false,
		},
		{
			name:  "Multiple backups, one match",
			jobID: "20250104-120000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-120000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-120000.000000_mp-zone1"
					}
				}]
			}]`,
			wantBackupID: "20250104-120000F",
			wantError:    false,
		},
		{
			name:  "No matching backup",
			jobID: "20250104-180000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
		{
			name:          "No backups at all",
			jobID:         "20250104-100000.000000_mp-zone1",
			jsonOutput:    `[{"backup": []}]`,
			wantError:     true,
			errorContains: "no backups found",
		},
		{
			name:  "Duplicate matching backups",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}, {
					"label": "20250104-100000F_20250104-110000I",
					"annotation": {
						"multipooler_id": "zone1-multipooler1",
						"job_id": "20250104-100000.000000_mp-zone1"
					}
				}]
			}]`,
			wantError:     true,
			errorContains: "found 2 backups",
		},
		{
			name:  "Backup without annotations",
			jobID: "20250104-100000.000000_mp-zone1",
			jsonOutput: `[{
				"backup": [{
					"label": "20250104-100000F"
				}]
			}]`,
			wantError:     true,
			errorContains: "no backup found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory for mock pgbackrest binary
			binDir := t.TempDir()

			// Create mock pgbackrest binary that returns the test JSON
			mockScript := `#!/bin/bash
if [[ "$*" == *"info"* ]]; then
    cat << 'JSONEOF'
` + tt.jsonOutput + `
JSONEOF
    exit 0
fi
exit 1
`
			pgbackrestPath := binDir + "/pgbackrest"
			err := exec.Command("sh", "-c", "cat > "+pgbackrestPath+" << 'EOF'\n"+mockScript+"\nEOF").Run()
			require.NoError(t, err)
			err = exec.Command("chmod", "+x", pgbackrestPath).Run()
			require.NoError(t, err)

			// Prepend bin dir to PATH so our mock pgbackrest is found first
			t.Setenv("PATH", binDir+":/usr/bin:/bin")

			// Use separate directory for pooler data
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "test-tg", "0", poolerDir)

			backupID, err := e.FindByJobID(context.Background(), tt.jobID)

			if tt.wantError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBackupID, backupID)
			}
		})
	}
}

func TestRequireBackupConfig(t *testing.T) {
	t.Run("Error when backup config is not set", func(t *testing.T) {
		e, _ := newTestEngine(t, t.TempDir(), "test-tg", "0", "") // no backup location
		cfg, err := e.requireBackupConfig()
		require.Error(t, err)
		assert.Nil(t, cfg)
		assert.Contains(t, err.Error(), "backup config not set")
	})

	t.Run("Returns config when set", func(t *testing.T) {
		e, _ := newTestEngine(t, t.TempDir(), "test-tg", "0", "/tmp/backups")
		cfg, err := e.requireBackupConfig()
		require.NoError(t, err)
		assert.NotNil(t, cfg)
	})
}

func TestEngineAccessors(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "test-tg", "myshard", "/tmp/backups")
	assert.Equal(t, "myshard", e.shardID())
	assert.NotNil(t, e.Metrics())
}

// sidecarCall is one Run the fake sidecar saw.
type sidecarCall struct {
	cmd  pb.Command
	args []string
}

// fakeSidecar is a SidecarRunner that records calls and lets a test script the
// reply for each.
type fakeSidecar struct {
	mu     sync.Mutex
	err    error
	calls  []sidecarCall
	reply  func(cmd pb.Command, args []string, stdout, stderr io.Writer) int
	status func() *pb.StatusResponse
	// statusErr, when set and returning non-nil, fails Status.
	statusErr func() error
}

func (f *fakeSidecar) Run(_ context.Context, cmd pb.Command, args []string, stdout, stderr io.Writer) (int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, sidecarCall{cmd: cmd, args: slices.Clone(args)})
	f.mu.Unlock()
	exit := 0
	if f.reply != nil {
		exit = f.reply(cmd, args, stdout, stderr)
	}
	if f.err != nil {
		return exit, f.err
	}
	return exit, nil
}

func (f *fakeSidecar) Status(context.Context) (*pb.StatusResponse, error) {
	if f.statusErr != nil {
		if err := f.statusErr(); err != nil {
			return nil, err
		}
	}
	if f.status == nil {
		return &pb.StatusResponse{}, nil
	}
	return f.status(), nil
}

func (f *fakeSidecar) lastCall(t *testing.T) sidecarCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.NotEmpty(t, f.calls)
	return f.calls[len(f.calls)-1]
}

func newSidecarTestEngine(t *testing.T, fake *fakeSidecar) *Engine {
	t.Helper()
	e, _ := newTestEngine(t, t.TempDir(), "", "", t.TempDir())
	e.settings.Sidecar = fake
	return e
}

// sidecarAllowlist mirrors the rules in go/services/pgbackrestd/allowlist.go.
// It is a copy because checkArgs is unexported and a service may not import
// another service's internals; a change there must be made here too, and the
// test below is what notices an engine argv that the real sidecar would reject.
var sidecarAllowlist = map[pb.Command]map[string]func(string) bool{
	pb.Command_COMMAND_BACKUP: {
		"type":          func(v string) bool { return slices.Contains([]string{"full", "diff", "incr"}, v) },
		"annotation":    func(v string) bool { return strings.Contains(v, "=") && !strings.ContainsAny(v, "\x00\n\r") },
		"repo":          regexp.MustCompile(`^[0-9]{1,3}$`).MatchString,
		"pg2-host":      regexp.MustCompile(`^[0-9A-Za-z._:-]{1,253}$`).MatchString,
		"pg2-path":      func(v string) bool { return strings.HasPrefix(v, "/") && filepath.Clean(v) == v },
		"pg2-host-port": regexp.MustCompile(`^[0-9]{1,5}$`).MatchString,
	},
	pb.Command_COMMAND_VERIFY:        {"set": regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`).MatchString, "repo": regexp.MustCompile(`^[0-9]{1,3}$`).MatchString},
	pb.Command_COMMAND_CHECK:         {"repo": regexp.MustCompile(`^[0-9]{1,3}$`).MatchString},
	pb.Command_COMMAND_STANZA_CREATE: {},
	pb.Command_COMMAND_INFO: {
		"set":    regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`).MatchString,
		"repo":   regexp.MustCompile(`^[0-9]{1,3}$`).MatchString,
		"output": func(v string) bool { return v == "json" },
		"log-level-console": func(v string) bool {
			return slices.Contains([]string{"off", "error", "warn", "info", "detail", "debug", "trace"}, v)
		},
	},
	pb.Command_COMMAND_RESTORE: {
		"set":  regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`).MatchString,
		"repo": regexp.MustCompile(`^[0-9]{1,3}$`).MatchString,
		"type": func(v string) bool { return v == "standby" },
	},
	pb.Command_COMMAND_EXPIRE: {
		"set":  regexp.MustCompile(`^[0-9A-Za-z_-]{1,128}$`).MatchString,
		"repo": regexp.MustCompile(`^[0-9]{1,3}$`).MatchString,
	},
}

var sidecarRetentionRe = regexp.MustCompile(`^repo[0-9]{1,3}-retention-(full|full-type|diff|history|archive|archive-type)$`)

func sidecarAllows(cmd pb.Command, args []string) error {
	rules := sidecarAllowlist[cmd]
	seen := map[string]bool{}
	for _, a := range args {
		rest, ok := strings.CutPrefix(a, "--")
		if !ok {
			return fmt.Errorf("%q: not an option", a)
		}
		name, value, ok := strings.Cut(rest, "=")
		if !ok {
			return fmt.Errorf("%q: not --name=value", a)
		}
		valid, ok := rules[name]
		switch {
		case ok:
		case cmd == pb.Command_COMMAND_EXPIRE && sidecarRetentionRe.MatchString(name):
			valid = regexp.MustCompile(`^[0-9a-z]+$`).MatchString
		default:
			return fmt.Errorf("%q: option not allowed", a)
		}
		if seen[name] && name != "annotation" {
			return fmt.Errorf("%q: repeated", a)
		}
		seen[name] = true
		if !valid(value) {
			return fmt.Errorf("%q: bad value", a)
		}
	}
	return nil
}

func TestEngine_SidecarModeStripsStanzaAndConfig(t *testing.T) {
	const infoJSON = `[{"backup":[{"label":"20250104-100000F","annotation":{"job_id":"j1","table_group":"default","shard":"0-inf"}}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`
	fake := &fakeSidecar{}
	fake.reply = func(cmd pb.Command, _ []string, stdout, stderr io.Writer) int {
		// log-level-stderr=info makes the sidecar's stderr carry log lines.
		_, _ = io.WriteString(stderr, "P00   INFO: info command begin\n")
		if cmd == pb.Command_COMMAND_INFO {
			_, _ = io.WriteString(stdout, infoJSON)
		}
		return 0
	}
	e := newSidecarTestEngine(t, fake)
	ctx := actionlockedCtx(t)

	t.Run("info parses stdout only", func(t *testing.T) {
		backups, err := e.ListBackups(ctx)
		require.NoError(t, err)
		require.Len(t, backups, 1)
		assert.Equal(t, "20250104-100000F", backups[0].BackupId)
		call := fake.lastCall(t)
		assert.Equal(t, pb.Command_COMMAND_INFO, call.cmd)
		assert.Equal(t, []string{"--output=json", "--log-level-console=off"}, call.args)
	})

	t.Run("check", func(t *testing.T) {
		require.NoError(t, e.Check(ctx))
		call := fake.lastCall(t)
		assert.Equal(t, pb.Command_COMMAND_CHECK, call.cmd)
		assert.Empty(t, call.args)
	})

	t.Run("verify", func(t *testing.T) {
		_, err := e.Verify(ctx)
		require.NoError(t, err)
		assert.Equal(t, pb.Command_COMMAND_VERIFY, fake.lastCall(t).cmd)
	})

	t.Run("stanza-create", func(t *testing.T) {
		require.NoError(t, e.StanzaCreate(ctx))
		call := fake.lastCall(t)
		assert.Equal(t, pb.Command_COMMAND_STANZA_CREATE, call.cmd)
		assert.Empty(t, call.args)
	})

	t.Run("nonzero exit is an error carrying the output", func(t *testing.T) {
		failing := &fakeSidecar{reply: func(_ pb.Command, _ []string, _, stderr io.Writer) int {
			_, _ = io.WriteString(stderr, "ERROR: [103]: repo unreachable")
			return 103
		}}
		err := newSidecarTestEngine(t, failing).Check(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exit status 103")
		assert.Contains(t, err.Error(), "repo unreachable")
	})

	for _, c := range fake.calls {
		for _, a := range c.args {
			assert.False(t, strings.HasPrefix(a, "--stanza=") || strings.HasPrefix(a, "--config="), "sidecar must not be sent %q", a)
		}
	}
}

func TestEngine_SidecarRestoreArgsPassAllowlist(t *testing.T) {
	fake := &fakeSidecar{}
	e := newSidecarTestEngine(t, fake)

	for _, backupID := range []string{"", "20250104-100000F", "20250104-100000F_20250105-010101I"} {
		require.NoError(t, e.Restore(context.Background(), backupID, t.TempDir()))
		call := fake.lastCall(t)
		assert.Equal(t, pb.Command_COMMAND_RESTORE, call.cmd)
		for _, a := range call.args {
			assert.False(t, strings.HasPrefix(a, "--recovery-option"), "the sidecar renders recovery options itself: %q", a)
			assert.NotContains(t, a, "restore-wrapper", "the local wrapper must not reach the sidecar")
		}
		require.NoError(t, sidecarAllows(call.cmd, call.args), "args %v", call.args)
	}
}

// TestEngine_SidecarRestoreWaitsForPendingStart covers a restore that exits 0
// while the sidecar is still retrying the start that clears its stop file.
func TestEngine_SidecarRestoreWaitsForPendingStart(t *testing.T) {
	oldWait, oldPoll := restoreStartWait, restoreStartPoll
	restoreStartWait, restoreStartPoll = 2*time.Second, 5*time.Millisecond
	t.Cleanup(func() { restoreStartWait, restoreStartPoll = oldWait, oldPoll })

	var polls int
	fake := &fakeSidecar{status: func() *pb.StatusResponse {
		polls++
		return &pb.StatusResponse{RestoreStartPending: polls < 3}
	}}
	e := newSidecarTestEngine(t, fake)
	require.NoError(t, e.Restore(context.Background(), "", t.TempDir()))
	assert.GreaterOrEqual(t, polls, 3, "restore must wait for the pending start to clear")

	// A start that never clears must not hang the restore.
	restoreStartWait = 50 * time.Millisecond
	stuck := &fakeSidecar{status: func() *pb.StatusResponse { return &pb.StatusResponse{RestoreStartPending: true} }}
	done := make(chan error, 1)
	go func() { done <- newSidecarTestEngine(t, stuck).Restore(context.Background(), "", t.TempDir()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("restore waited forever on a start that never clears")
	}
}

// TestEngine_SidecarArgsPassAllowlist runs every argv the engine builds for
// the sidecar through the allowlist.
func TestEngine_SidecarArgsPassAllowlist(t *testing.T) {
	fake := &fakeSidecar{}
	fake.reply = func(cmd pb.Command, _ []string, stdout, _ io.Writer) int {
		if cmd == pb.Command_COMMAND_INFO {
			_, _ = io.WriteString(stdout, `[{"backup":[],"repo":[]}]`)
		}
		return 0
	}
	e := newSidecarTestEngine(t, fake)
	ctx := actionlockedCtx(t)

	_, _ = e.ListBackups(ctx)
	require.NoError(t, e.Check(ctx))
	_, err := e.Verify(ctx)
	require.NoError(t, err)
	require.NoError(t, e.StanzaCreate(ctx))
	_, err = e.Expire(ctx, map[string]string{"repo1_retention_full": "7", "repo1_retention_diff": "3"})
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.NotEmpty(t, fake.calls)
	for _, c := range fake.calls {
		assert.NoError(t, sidecarAllows(c.cmd, c.args), "%v %v", c.cmd, c.args)
	}
}

func actionlockedCtx(t *testing.T) context.Context {
	t.Helper()
	lock := actionlock.NewActionLock()
	ctx, err := lock.Acquire(context.Background(), "engine-test")
	require.NoError(t, err)
	t.Cleanup(func() { lock.Release(ctx) })
	return ctx
}

// TestEngine_SidecarBackupWithOverridesPassesAllowlist is the round trip for
// ApplyPgBackRestOverrides: the pg2 options a replica backup sends, after
// overrides are applied, must still be accepted by the sidecar, and their order
// must not depend on map iteration.
func TestEngine_SidecarBackupWithOverridesPassesAllowlist(t *testing.T) {
	const jobID = "job-1"
	fake := &fakeSidecar{}
	fake.reply = func(cmd pb.Command, _ []string, stdout, _ io.Writer) int {
		if cmd == pb.Command_COMMAND_INFO {
			_, _ = fmt.Fprintf(stdout, `[{"backup":[{"label":"20250104-100000F","annotation":{"job_id":%q,"table_group":%q,"shard":%q}}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`,
				jobID, constants.DefaultTableGroup, constants.DefaultShard)
		}
		return 0
	}
	e := newSidecarTestEngine(t, fake)

	ts := memorytopo.NewServer(t.Context(), "zone1")
	t.Cleanup(func() { ts.Close() })
	ctx := actionlockedCtx(t)
	ctx, unlock, err := ts.TryLockBackup(ctx, e.id.ShardRef(), "engine-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		var uerr error
		unlock(&uerr)
	})

	base := []string{"--pg2-host=primary.example.com", "--pg2-path=/data/pg_data", "--pg2-host-port=8432"}
	overrides := map[string]string{"pg2_path": "/other/pgdata", "pg2_host_port": "9000", "repo": "1"}
	var first []string
	for i := range 20 {
		pg2 := commonbackup.ApplyPgBackRestOverrides(slices.Clone(base), overrides)
		_, err := e.Backup(ctx, PgBackRestFull, jobID, pg2, clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)
		require.NoError(t, err)

		fake.mu.Lock()
		var backupArgs []string
		for _, c := range fake.calls {
			if c.cmd == pb.Command_COMMAND_BACKUP {
				backupArgs = c.args
			}
		}
		fake.mu.Unlock()
		require.NoError(t, sidecarAllows(pb.Command_COMMAND_BACKUP, backupArgs), "args %v", backupArgs)
		if i == 0 {
			first = backupArgs
			assert.Contains(t, backupArgs, "--pg2-path=/other/pgdata")
			assert.Contains(t, backupArgs, "--pg2-host-port=9000")
			continue
		}
		assert.Equal(t, first, backupArgs, "argv order must not vary between runs")
	}

	// An override the sidecar refuses is refused up front by the test replica
	// the same way the real allowlist would: it names a location.
	bad := commonbackup.ApplyPgBackRestOverrides(slices.Clone(base), map[string]string{"repo1_path": "/elsewhere"})
	assert.Error(t, sidecarAllows(pb.Command_COMMAND_BACKUP, bad))
}

// TestEngine_SidecarFailureIsNotAnEmptyRepository: a sidecar that cannot be
// reached, or that fails with no output, must never read as "no stanza yet",
// which is what lets bootstrap create a stanza over a repository it could not
// read.
func TestEngine_SidecarFailureIsNotAnEmptyRepository(t *testing.T) {
	ctx := actionlockedCtx(t)

	down := newSidecarTestEngine(t, &fakeSidecar{err: errors.New("dial unix control.sock: connection refused")})
	backups, err := down.ListBackups(ctx)
	require.Error(t, err)
	assert.Nil(t, backups)

	silent := newSidecarTestEngine(t, &fakeSidecar{reply: func(pb.Command, []string, io.Writer, io.Writer) int { return 103 }})
	_, err = silent.ListBackups(ctx)
	require.Error(t, err)

	missing := newSidecarTestEngine(t, &fakeSidecar{reply: func(_ pb.Command, _ []string, _, stderr io.Writer) int {
		_, _ = io.WriteString(stderr, "ERROR: [055]: unable to open missing file '/repo/backup.info'; stanza does not exist")
		return 55
	}})
	backups, err = missing.ListBackups(ctx)
	require.NoError(t, err, "a real missing stanza is still an empty list")
	assert.Empty(t, backups)
}

// argvGoldenFile is the contract between this engine and the sidecar's
// allowlist: the exact argv the engine sends for each command.
// go/services/pgbackrestd reads the same file and runs every entry through its
// real checkArgs, and neither package imports the other, so a change on either
// side that breaks the other fails a test here or there. Regenerate with
// UPDATE_GOLDEN=1 after an intended change.
const argvGoldenFile = "testdata/sidecar_argv.golden.json"

type argvGoldenEntry struct {
	Name    string   `json:"name"`
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func TestEngine_SidecarArgvGolden(t *testing.T) {
	const jobID = "job-1"
	var got []argvGoldenEntry
	record := func(name string, fake *fakeSidecar) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		for _, c := range fake.calls {
			args := c.args
			if args == nil {
				args = []string{}
			}
			got = append(got, argvGoldenEntry{Name: name, Command: c.cmd.String(), Args: args})
		}
	}
	newFake := func() *fakeSidecar {
		f := &fakeSidecar{}
		f.reply = func(cmd pb.Command, _ []string, stdout, _ io.Writer) int {
			if cmd == pb.Command_COMMAND_INFO {
				_, _ = fmt.Fprintf(stdout, `[{"backup":[{"label":"20250104-100000F","annotation":{"job_id":%q,"table_group":%q,"shard":%q}}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`,
					jobID, constants.DefaultTableGroup, constants.DefaultShard)
			}
			return 0
		}
		return f
	}

	backupRun := func(name string, pg2 []string, role clustermetadatapb.RoutingRole) {
		fake := newFake()
		e := newSidecarTestEngine(t, fake)
		ts := memorytopo.NewServer(t.Context(), "zone1")
		t.Cleanup(func() { ts.Close() })
		ctx, unlock, err := ts.TryLockBackup(actionlockedCtx(t), e.id.ShardRef(), "golden")
		require.NoError(t, err)
		_, err = e.Backup(ctx, PgBackRestFull, jobID, pg2, role)
		require.NoError(t, err)
		var uerr error
		unlock(&uerr)
		record(name, fake)
	}
	backupRun("backup-primary", nil, clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY)
	backupRun("backup-replica-overrides",
		commonbackup.ApplyPgBackRestOverrides(
			[]string{"--pg2-host=primary.example.com", "--pg2-path=/data/pg_data", "--pg2-host-port=8432"},
			map[string]string{"pg2_path": "/other/pgdata", "repo": "1"}),
		clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA)

	scenario := func(name string, run func(e *Engine, ctx context.Context) error) {
		fake := newFake()
		require.NoError(t, run(newSidecarTestEngine(t, fake), actionlockedCtx(t)), name)
		record(name, fake)
	}
	scenario("restore-latest", func(e *Engine, ctx context.Context) error { return e.Restore(ctx, "", t.TempDir()) })
	scenario("restore-set", func(e *Engine, ctx context.Context) error {
		return e.Restore(ctx, "20250104-100000F_20250105-010101I", t.TempDir())
	})
	scenario("expire-retention", func(e *Engine, ctx context.Context) error {
		_, err := e.Expire(ctx, map[string]string{"repo1_retention_full": "7", "repo1_retention_diff": "3"})
		return err
	})
	scenario("info", func(e *Engine, ctx context.Context) error { _, err := e.ListBackups(ctx); return err })
	scenario("check", func(e *Engine, ctx context.Context) error { return e.Check(ctx) })
	scenario("stanza-create", func(e *Engine, ctx context.Context) error { return e.StanzaCreate(ctx) })
	scenario("verify", func(e *Engine, ctx context.Context) error { _, err := e.Verify(ctx); return err })

	for _, g := range got {
		require.NoError(t, sidecarAllows(pb.Command(pb.Command_value[g.Command]), g.Args), "%s %v", g.Name, g.Args)
	}

	enc, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	enc = append(enc, '\n')
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(argvGoldenFile), 0o755))
		require.NoError(t, os.WriteFile(argvGoldenFile, enc, 0o644))
	}
	want, err := os.ReadFile(argvGoldenFile)
	require.NoError(t, err, "run with UPDATE_GOLDEN=1 to create it")
	assert.JSONEq(t, string(want), string(enc), "the argv the engine sends changed: update the golden and make sure pgbackrestd still accepts it")
}

// TestEngine_SidecarRestoreWaitStatusErrorEndsIt: a Status that keeps failing
// must end the post-restore wait instead of spinning.
func TestEngine_SidecarRestoreWaitStatusErrorEndsIt(t *testing.T) {
	oldWait, oldPoll := restoreStartWait, restoreStartPoll
	restoreStartWait, restoreStartPoll = time.Hour, time.Millisecond
	t.Cleanup(func() { restoreStartWait, restoreStartPoll = oldWait, oldPoll })

	var calls int
	fake := &fakeSidecar{statusErr: func() error { calls++; return errors.New("status unavailable") }}
	e := newSidecarTestEngine(t, fake)

	done := make(chan error, 1)
	go func() { done <- e.Restore(context.Background(), "", t.TempDir()) }()
	select {
	case err := <-done:
		require.NoError(t, err, "the restore itself succeeded")
	case <-time.After(5 * time.Second):
		t.Fatal("a failing Status must end the wait, not loop until the deadline")
	}
	assert.Equal(t, 1, calls)
}

func pgVersionAt(t *testing.T, e *Engine) {
	t.Helper()
	require.NoError(t, os.MkdirAll(e.settings.PgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(e.settings.PgDataDir, "PG_VERSION"), []byte("16"), 0o644))
}

func TestEngine_SidecarRestoreFailureSettlesBeforeReporting(t *testing.T) {
	undo := OverrideSidecarRestoreTimingForTest(5*time.Second, time.Millisecond)
	t.Cleanup(undo)

	t.Run("a lost call whose restore then succeeds is a success", func(t *testing.T) {
		var polls int
		fake := &fakeSidecar{err: errors.New("stream reset"), status: func() *pb.StatusResponse {
			polls++
			return &pb.StatusResponse{RestoreInProgress: polls < 4, LastRestoreSucceeded: true}
		}}
		e := newSidecarTestEngine(t, fake)
		pgVersionAt(t, e)
		require.NoError(t, e.Restore(context.Background(), "", t.TempDir()))
		assert.GreaterOrEqual(t, polls, 4, "it waited for the restore to end")
	})

	t.Run("success without PG_VERSION is not trusted", func(t *testing.T) {
		fake := &fakeSidecar{err: errors.New("stream reset"), status: func() *pb.StatusResponse {
			return &pb.StatusResponse{LastRestoreSucceeded: true}
		}}
		err := newSidecarTestEngine(t, fake).Restore(context.Background(), "", t.TempDir())
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrRestoreUnsettled, "the restore ended, so cleanup is safe")
	})

	t.Run("an ended failed restore is a plain failure even with PG_VERSION on disk", func(t *testing.T) {
		fake := &fakeSidecar{err: errors.New("deadline"), status: func() *pb.StatusResponse {
			return &pb.StatusResponse{LastRestoreSucceeded: false}
		}}
		e := newSidecarTestEngine(t, fake)
		pgVersionAt(t, e)
		err := e.Restore(context.Background(), "", t.TempDir())
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrRestoreUnsettled)
	})

	t.Run("a restore that never ends is unsettled, not failed", func(t *testing.T) {
		undo := OverrideSidecarRestoreTimingForTest(150*time.Millisecond, time.Millisecond)
		defer undo()
		fake := &fakeSidecar{err: errors.New("deadline"), status: func() *pb.StatusResponse {
			return &pb.StatusResponse{RestoreInProgress: true}
		}}
		err := newSidecarTestEngine(t, fake).Restore(context.Background(), "", t.TempDir())
		require.ErrorIs(t, err, ErrRestoreUnsettled)
	})

	t.Run("an unreachable status is unsettled", func(t *testing.T) {
		undo := OverrideSidecarRestoreTimingForTest(150*time.Millisecond, time.Millisecond)
		defer undo()
		fake := &fakeSidecar{err: errors.New("deadline"), statusErr: func() error { return errors.New("down") }}
		err := newSidecarTestEngine(t, fake).Restore(context.Background(), "", t.TempDir())
		require.ErrorIs(t, err, ErrRestoreUnsettled)
	})

	t.Run("the settle wait survives the caller's context ending", func(t *testing.T) {
		var polls int
		fake := &fakeSidecar{err: context.Canceled, status: func() *pb.StatusResponse {
			polls++
			return &pb.StatusResponse{RestoreInProgress: polls < 3}
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := newSidecarTestEngine(t, fake).Restore(ctx, "", t.TempDir())
		require.Error(t, err)
		assert.GreaterOrEqual(t, polls, 3)
		assert.NotErrorIs(t, err, ErrRestoreUnsettled)
	})
}

func TestEngine_SidecarOutputTruncation(t *testing.T) {
	ctx := actionlockedCtx(t)
	trunc := &fakeSidecar{err: ErrOutputTruncated}

	t.Run("verify, info and check fail", func(t *testing.T) {
		e := newSidecarTestEngine(t, trunc)
		_, err := e.Verify(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dropped part of the command output")
		err = e.Check(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dropped part of the command output")
		_, err = e.ListBackups(ctx)
		require.Error(t, err, "a truncated info must not read as an empty repository")
	})

	t.Run("restore only logs", func(t *testing.T) {
		e := newSidecarTestEngine(t, trunc)
		require.NoError(t, e.Restore(context.Background(), "", t.TempDir()))
	})
}

// A truncated info whose partial output holds the words that mean "no stanza"
// must still fail: only a command that ran and exited non-zero can say that.
func TestEngine_SidecarTruncatedInfoIsNotAnEmptyRepository(t *testing.T) {
	ctx := actionlockedCtx(t)
	for _, partial := range []string{
		`[{"backup":[],"repo":[{"status":{"code":99,"message":"unable to open missing file '/repo/backup.info'`,
		`ERROR: stanza does not exist`,
	} {
		fake := &fakeSidecar{err: ErrOutputTruncated, reply: func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
			_, _ = io.WriteString(stdout, partial)
			return 0
		}}
		backups, err := newSidecarTestEngine(t, fake).ListBackups(ctx)
		require.Error(t, err, partial)
		assert.Nil(t, backups)
	}

	// The same partial text from a command that did exit non-zero still means what it says.
	exited := &fakeSidecar{reply: func(_ pb.Command, _ []string, _, stderr io.Writer) int {
		_, _ = io.WriteString(stderr, "ERROR: [055]: unable to open missing file '/repo/backup.info'")
		return 55
	}}
	backups, err := newSidecarTestEngine(t, exited).ListBackups(ctx)
	require.NoError(t, err)
	assert.Empty(t, backups)

	// And a call that ended with its context carries no repository evidence either.
	cancelled := &fakeSidecar{err: context.Canceled, reply: func(_ pb.Command, _ []string, stdout, _ io.Writer) int {
		_, _ = io.WriteString(stdout, "stanza does not exist")
		return 0
	}}
	_, err = newSidecarTestEngine(t, cancelled).ListBackups(ctx)
	require.Error(t, err)
}

func TestEngine_SidecarRestoreSettleHonoursShutdown(t *testing.T) {
	undo := OverrideSidecarRestoreTimingForTest(time.Hour, time.Millisecond)
	t.Cleanup(undo)
	shutdown, stop := context.WithCancel(context.Background())
	fake := &fakeSidecar{err: errors.New("deadline"), status: func() *pb.StatusResponse {
		return &pb.StatusResponse{RestoreInProgress: true}
	}}
	e := newSidecarTestEngine(t, fake)
	e.settings.Shutdown = shutdown

	done := make(chan error, 1)
	go func() { done <- e.Restore(context.Background(), "", t.TempDir()) }()
	time.Sleep(50 * time.Millisecond)
	stop()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrRestoreUnsettled)
	case <-time.After(5 * time.Second):
		t.Fatal("settle held the action lock through shutdown")
	}
}

func TestEngine_SidecarRestoreNotDeliveredDoesNotWait(t *testing.T) {
	undo := OverrideSidecarRestoreTimingForTest(time.Hour, time.Millisecond)
	t.Cleanup(undo)
	fake := &fakeSidecar{err: fmt.Errorf("%w: dial unix x: connection refused", ErrRunNotDelivered)}
	start := time.Now()
	err := newSidecarTestEngine(t, fake).Restore(context.Background(), "", t.TempDir())
	require.ErrorIs(t, err, ErrRunNotDelivered)
	assert.Less(t, time.Since(start), 2*time.Second)
}
