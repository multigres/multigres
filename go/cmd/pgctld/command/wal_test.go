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

package command

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
)

func TestWALHelper(t *testing.T) {
	sub := os.Getenv("TEST_WAL_SUBCOMMAND")
	if sub == "" {
		t.Skip("subprocess helper")
	}
	var args []string
	if sub == "pgbackrest-restore-wal" {
		args = append(args, "00000001.history")
	}
	args = append(args, "pg_wal/path with space")
	require.NoError(t, RunWALCommand(sub, args))
}

func TestWALCommands(t *testing.T) {
	for _, sub := range []string{"pgbackrest-archive-wal", "pgbackrest-restore-wal"} {
		t.Run(sub, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv(backup.WALPoolerDirEnv, dir)
			t.Setenv("TEST_WAL_SUBCOMMAND", sub)
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			log := filepath.Join(dir, "args")
			t.Setenv("TEST_WAL_ARGS", log)
			stub := filepath.Join(dir, "pgbackrest")
			require.NoError(t, os.WriteFile(stub, []byte(`#!/bin/sh
printf '%s\n' "$@" > "$TEST_WAL_ARGS"
exit "$TEST_WAL_EXIT"
`), 0o700))
			helper := func() *executil.Cmd { return executil.Command(t.Context(), os.Args[0], "-test.run=^TestWALHelper$") }
			for _, code := range []int{0, 37} {
				t.Setenv("TEST_WAL_EXIT", strconv.Itoa(code))
				err := helper().Run()
				if code == 0 {
					require.NoError(t, err)
				} else {
					var e *exec.ExitError
					require.ErrorAs(t, err, &e)
					require.NotZero(t, e.ExitCode())
				}
			}
			got, err := os.ReadFile(log)
			require.NoError(t, err)
			want := []string{"--stanza=multigres", "--config=" + filepath.Join(dir, "pgbackrest", "pgbackrest.conf")}
			if sub == "pgbackrest-restore-wal" {
				want = append(want, "archive-get", "00000001.history")
			} else {
				want = append(want, "--reset-archive-push-queue-max", "archive-push")
			}
			want = append(want, "pg_wal/path with space")
			require.Equal(t, want, strings.Split(strings.TrimSuffix(string(got), "\n"), "\n"))
			require.Error(t, RunWALCommand(sub, nil))
			require.Error(t, RunWALCommand(sub, []string{"one", "two", "three"}))
			// The child announces readiness only after installing its TERM handler.
			ready := filepath.Join(dir, "ready")
			t.Setenv("TEST_WAL_READY", ready)
			require.NoError(t, os.WriteFile(stub, []byte(`#!/bin/sh
trap 'exit 19' TERM
echo $$ > "$TEST_WAL_READY"
while :; do :; done
`), 0o700))
			c := helper()
			require.NoError(t, c.Start())
			t.Cleanup(func() { _, _ = c.Stop(context.Background()) })
			var pid int
			require.Eventually(t, func() bool {
				data, _ := os.ReadFile(ready)
				pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
				return pid > 0
			}, 5*time.Second, 10*time.Millisecond)

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			err, exited := c.Terminate(ctx)
			require.True(t, exited)
			require.Error(t, err)
			require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
			require.NoFileExists(t, filepath.Join(dir, constants.RestoreCommandPIDFile))
		})
	}
}

func TestUnknownSubcommandFails(t *testing.T) {
	root, _ := GetRootCommand()
	root.SetArgs([]string{"unknown-wal-command", "segment"})
	require.Error(t, root.Execute())
}

// Interpose on the test pg_ctl and capture the environment it actually received.
func captureWALLaunchEnv(t *testing.T, bin, dir string) {
	t.Helper()
	ctl := filepath.Join(bin, "pg_ctl")
	require.NoError(t, os.Rename(ctl, ctl+".real"))
	output := filepath.Join(t.TempDir(), "wal-env")
	t.Setenv("WAL_ENV_OUTPUT", output)
	t.Setenv(backup.WALBinDirEnv, "/inherited-decoy")
	require.NoError(t, os.WriteFile(ctl, []byte(`#!/bin/sh
if [ "$1" = start ]; then
  printf '%s\n' "$MULTIGRES_WAL_POOLER_DIR" "${MULTIGRES_WAL_BIN_DIR-unset}" > "$WAL_ENV_OUTPUT"
fi
exec "$0.real" "$@"
`), 0o700))
	t.Cleanup(func() {
		got, err := os.ReadFile(output)
		require.NoError(t, err)
		// pgctld here is the test binary, without a sibling installation.
		require.Equal(t, dir+"\n\n", string(got))
	})
}

// An older image cannot authorize a wrapper directory. Never run an
// executable by its bare name in this case.
func TestWALCommandsFailClosedOnOldImage(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgctld"), []byte("#!/bin/sh\nexit 0\n"), 0o700))
	t.Setenv("PATH", dir)
	for _, cmd := range []string{backup.ArchiveWALCommand, backup.RestoreWALCommand} {
		err := executil.Command(t.Context(), "/bin/sh", "-c", cmd).AddEnv(backup.WALBinDirEnv + "=").Run()
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.NotZero(t, exit.ExitCode())
	}
}

func TestInvalidWALPoolerDir(t *testing.T) {
	t.Setenv("PGPASSFILE", "/valid/pgbackrest/pgbackrest.pgpass")
	for _, dir := range []string{"relative", ""} {
		t.Setenv(backup.WALPoolerDirEnv, dir)
		require.ErrorContains(t, RunWALCommand(backup.ArchiveWALExecutable, []string{"--probe", "wal-wrapper", "version"}), "absolute path")
	}
	for _, v := range walChildEnv("") {
		require.False(t, strings.HasPrefix(v, backup.WALPoolerDirEnv+"="))
	}
}

func TestWALProbeRejectsWrongAbsolutePooler(t *testing.T) {
	dir := t.TempDir()
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte(dir)))
	probe := []string{"--probe", "pooler-root", expected}
	for _, name := range []string{backup.ArchiveWALExecutable, backup.RestoreWALExecutable} {
		t.Setenv(backup.WALPoolerDirEnv, t.TempDir())
		require.ErrorContains(t, RunWALCommand(name, probe), "directory mismatch")
		t.Setenv(backup.WALPoolerDirEnv, dir)
		require.NoError(t, RunWALCommand(name, probe))
	}
}

func TestWALInstallReprobe(t *testing.T) {
	dir := t.TempDir()
	cache := walInstallCache{}
	archive := filepath.Join(dir, backup.ArchiveWALExecutable)
	restore := filepath.Join(dir, backup.RestoreWALExecutable)
	write := func(p, body string) { require.NoError(t, os.WriteFile(p, []byte(body), 0o700)) }
	success := "#!/bin/sh\necho wal-wrapper-v1\n"
	write(archive, success)
	write(restore, success)
	check := func(ctx context.Context, force bool, v uint32, unknown bool) {
		t.Helper()
		got, u := cache.check(ctx, dir, force)
		require.Equal(t, v, got)
		require.Equal(t, unknown, u)
	}
	check(t.Context(), false, 1, false)
	info, err := os.Stat(archive)
	require.NoError(t, err)
	write(archive, "#!/bin/sh\nexit 127\n"+strings.Repeat("#", len(success)-len("#!/bin/sh\nexit 127\n")))
	require.NoError(t, os.Chtimes(archive, info.ModTime(), info.ModTime()))
	check(t.Context(), true, 0, false)
	check(t.Context(), false, 0, false)
	write(archive, "not an executable format")
	check(t.Context(), false, 0, false)
	write(archive, "#!/bin/sh\nexec sleep 60\n")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	check(ctx, true, 0, true)
	require.NoError(t, os.Remove(restore))
	check(ctx, true, 0, false)
}

func TestWALInstallCachesIncompatibleOutput(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "probes")
	t.Setenv("TEST_WAL_PROBES", log)
	archive := filepath.Join(dir, backup.ArchiveWALExecutable)
	restore := filepath.Join(dir, backup.RestoreWALExecutable)
	require.NoError(t, os.WriteFile(archive, []byte(`#!/bin/sh
printf 'probe\n' >> "$TEST_WAL_PROBES"
# A valid prefix followed by excess output must not be accepted.
printf 'wal-wrapper-v1\n'
while :; do printf 'excess output\n'; done
`), 0o700))
	require.NoError(t, os.WriteFile(restore, []byte("#!/bin/sh\necho wal-wrapper-v1\n"), 0o700))
	var cache walInstallCache
	for _, force := range []bool{false, false, true, false} {
		version, unknown := cache.check(t.Context(), dir, force)
		require.Zero(t, version)
		require.False(t, unknown)
	}
	got, err := os.ReadFile(log)
	require.NoError(t, err)
	require.Equal(t, "probe\nprobe\n", string(got))
}

func TestWALProbeBoundsDescendantPipeWait(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "descendant.pid")
	t.Setenv("TEST_WAL_DESCENDANT_PID", pidFile)
	path := filepath.Join(dir, backup.ArchiveWALExecutable)
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
/bin/sleep 10 &
printf '%s\n' "$!" > "$TEST_WAL_DESCENDANT_PID"
echo wal-wrapper-v1
`), 0o700))
	t.Cleanup(func() {
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ok, unknown := probeWALExecutable(ctx, path)
	require.False(t, ok)
	require.True(t, unknown)
	require.NoError(t, ctx.Err(), "an inherited stdout pipe must not wait for the context deadline")
}

func TestWALInstallOrdinaryCheckDoesNotWaitForForcedProbe(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "probing")
	t.Setenv("TEST_WAL_PROBE_STARTED", ready)
	archive := filepath.Join(dir, backup.ArchiveWALExecutable)
	restore := filepath.Join(dir, backup.RestoreWALExecutable)
	for _, path := range []string{archive, restore} {
		require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho wal-wrapper-v1\n"), 0o700))
	}
	var cache walInstallCache
	version, unknown := cache.check(t.Context(), dir, false)
	require.EqualValues(t, 1, version)
	require.False(t, unknown)
	require.NoError(t, os.WriteFile(archive, []byte(`#!/bin/sh
: > "$TEST_WAL_PROBE_STARTED"
exec /bin/sleep 10
`), 0o700))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.check(ctx, dir, true)
	}()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	status, cancelStatus := context.WithTimeout(t.Context(), time.Second)
	defer cancelStatus()
	version, unknown = cache.check(status, dir, false)
	require.Zero(t, version)
	require.True(t, unknown)
	require.NoError(t, status.Err())
}

func TestWALChildEnvDoesNotUsePATHFallback(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "approved bin with spaces")
	require.NoError(t, os.Mkdir(bin, 0o700))
	fallback := t.TempDir()
	t.Setenv("PATH", fallback+":"+os.Getenv("PATH"))
	t.Setenv(backup.WALBinDirEnv, fallback)
	for _, name := range walExecutableNames {
		require.NoError(t, os.WriteFile(filepath.Join(fallback, name), []byte("#!/bin/sh\nexit 0\n"), 0o700))
	}
	var cache walInstallCache
	assertResolution := func(binDir string, wantOK bool) {
		t.Helper()
		env := walChildEnvForInstall(t.Context(), &cache, binDir, "/pooler")
		for _, command := range []string{backup.ArchiveWALCommand, backup.RestoreWALCommand} {
			cmd := executil.Command(t.Context(), "/bin/sh", "-c", command).AddEnv(env...)
			if wantOK {
				out, err := cmd.Output()
				require.NoError(t, err)
				require.Equal(t, "approved sibling\n", string(out))
			} else {
				var exit *exec.ExitError
				require.ErrorAs(t, cmd.Run(), &exit)
				require.NotZero(t, exit.ExitCode())
			}
		}
	}
	assertResolution(bin, false)
	assertResolution("relative", false)
	assertResolution("", false)
	archive := filepath.Join(bin, backup.ArchiveWALExecutable)
	restore := filepath.Join(bin, backup.RestoreWALExecutable)
	for _, path := range []string{archive, restore} {
		require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
if [ "$1" = --probe ]; then
  echo wal-wrapper-v1
else
  echo 'approved sibling'
fi
`), 0o700))
	}
	assertResolution(bin, true)

	// A sibling disappearing after launch must not send execution to PATH.
	launchedEnv := walChildEnvForInstall(t.Context(), &cache, bin, "/pooler")
	require.NoError(t, os.Remove(archive))
	err := executil.Command(t.Context(), "/bin/sh", "-c", backup.ArchiveWALCommand).AddEnv(launchedEnv...).Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 127, exit.ExitCode())

	// Neither kind of link to a mutable pgctld file is an independent wrapper.
	ctl := filepath.Join(bin, "pgctld")
	require.NoError(t, os.WriteFile(ctl, []byte("#!/bin/sh\necho wal-wrapper-v1\n"), 0o700))
	require.NoError(t, os.Link(ctl, archive))
	assertResolution(bin, false)
	require.NoError(t, os.Remove(archive))
	require.NoError(t, os.Symlink(ctl, archive))
	assertResolution(bin, false)
}
