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

package command

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/pgctld"
)

// TestRestoreWrapperSkipsTelemetry guards the coupling between
// restore_wrapper.go (which sets skipTelemetryAnnotation) and root.go's
// PersistentPreRunE/PersistentPostRunE (which check it) — a typo or rename
// on either side would silently re-enable telemetry init for every
// restore_command invocation instead of failing loudly.
func TestRestoreWrapperSkipsTelemetry(t *testing.T) {
	root, _ := GetRootCommand()

	cmd, _, err := root.Find([]string{"restore-wrapper"})
	require.NoError(t, err)
	assert.Equal(t, "true", cmd.Annotations[skipTelemetryAnnotation],
		"restore-wrapper must carry the annotation root's hooks check")

	// Running it with no args fails in RunE (missing pidfile/command), but
	// PersistentPreRunE must still have run first — proving the shared
	// logging/SilenceUsage setup isn't skipped, only telemetry init is.
	root.SetArgs([]string{"restore-wrapper"})
	err = root.Execute()
	require.Error(t, err)
	assert.True(t, root.SilenceUsage, "PersistentPreRunE should have run despite skipping telemetry")
}

const restoreWrapperHelperEnv = "MULTIGRES_RESTORE_WRAPPER_TEST_HELPER"

// TestRestoreWrapperHelperProcess is not a test. The tests below re-exec the
// test binary into it so a real wrapper runs as its own process, the way
// postgres starts it. The wrapped command survives SIGTERM, SIGINT and SIGQUIT,
// which exercises the wrapper's SIGKILL escalation, and records each one it
// receives so a test can tell which signal the wrapper forwarded.
func TestRestoreWrapperHelperProcess(t *testing.T) {
	if os.Getenv(restoreWrapperHelperEnv) != "1" {
		t.Skip("helper process for the restore-wrapper tests")
	}
	err := runRestoreWrapper(nil, []string{
		os.Getenv("MULTIGRES_RESTORE_WRAPPER_PIDFILE"), "--", "sh", "-c",
		`for s in TERM INT QUIT; do trap "echo $s >> '$2'" $s; done; echo $$ > "$1"; while :; do sleep 1; done`,
		"sh", os.Getenv("MULTIGRES_RESTORE_WRAPPER_CHILD_PIDFILE"), os.Getenv("MULTIGRES_RESTORE_WRAPPER_CHILD_SIGNALS"),
	})
	require.NoError(t, err)
}

// wrapperHelper is a running restore-wrapper process started through
// TestRestoreWrapperHelperProcess.
type wrapperHelper struct {
	cmd         *exec.Cmd
	pid         int
	childPID    int
	signalsFile string
	exited      chan error
}

// startWrapperHelper starts a wrapper that publishes pidfile and waits until
// both it and its wrapped command are running. The wrapper is reaped as soon
// as it exits, so a liveness probe on its PID never sees a zombie.
func startWrapperHelper(t *testing.T, pidfile string) *wrapperHelper {
	t.Helper()
	dir := t.TempDir()
	childPIDFile := filepath.Join(dir, "child.pid")
	h := &wrapperHelper{signalsFile: filepath.Join(dir, "child.signals"), exited: make(chan error, 1)}
	h.cmd = exec.Command(os.Args[0], "-test.run=^TestRestoreWrapperHelperProcess$")
	h.cmd.Env = append(os.Environ(),
		restoreWrapperHelperEnv+"=1",
		// A -race binary sleeps for a second at exit by default, which would
		// push the wrapper past the deadline StopRestoreCommand gives it.
		"GORACE=atexit_sleep_ms=0",
		"MULTIGRES_RESTORE_WRAPPER_PIDFILE="+pidfile,
		"MULTIGRES_RESTORE_WRAPPER_CHILD_PIDFILE="+childPIDFile,
		"MULTIGRES_RESTORE_WRAPPER_CHILD_SIGNALS="+h.signalsFile,
	)
	h.cmd.Stdout = os.Stdout
	h.cmd.Stderr = os.Stderr
	require.NoError(t, h.cmd.Start())
	h.pid = h.cmd.Process.Pid
	go func() { h.exited <- h.cmd.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(h.pid, syscall.SIGKILL)
		if h.childPID != 0 {
			_ = syscall.Kill(-h.childPID, syscall.SIGKILL)
		}
	})

	readPID := func(path string) (int, bool) {
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		return pid, err == nil
	}
	require.Eventually(t, func() bool {
		wrapperPID, wrapperOK := readPID(pidfile)
		var childOK bool
		h.childPID, childOK = readPID(childPIDFile)
		return wrapperOK && childOK && wrapperPID == h.pid
	}, 5*time.Second, 10*time.Millisecond, "wrapper and child did not publish their PIDs")
	return h
}

// requireCleanExit checks the wrapper exited with status 0, removed its
// pidfile, and left no wrapped process behind.
func (h *wrapperHelper) requireCleanExit(t *testing.T, pidfile string) {
	t.Helper()
	select {
	case err := <-h.exited:
		require.NoError(t, err, "wrapper did not exit cleanly")
	case <-time.After(3 * time.Second):
		t.Fatal("wrapper did not exit")
	}
	_, err := os.Stat(pidfile)
	require.True(t, os.IsNotExist(err), "wrapper pidfile was not removed")
	require.Eventually(t, func() bool {
		return syscall.Kill(h.childPID, 0) != nil
	}, time.Second, 10*time.Millisecond, "wrapped process %d is still running", h.childPID)
}

// TestRestoreWrapperSignalExitsCleanly covers the wrapper being signalled while
// its command runs: SIGTERM from StopRestoreCommand or postgres's fast
// shutdown, and SIGQUIT from postgres's immediate shutdown. Either way the
// wrapped command's process group must be gone and the pidfile removed when the
// wrapper exits, and the wrapped command must see SIGTERM: pgbackrest has no
// SIGQUIT handler and would dump core.
func TestRestoreWrapperSignalExitsCleanly(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGQUIT} {
		t.Run(sig.String(), func(t *testing.T) {
			pidfile := filepath.Join(t.TempDir(), "wrapper.pid")
			h := startWrapperHelper(t, pidfile)
			assert.True(t, pidfileLocked(t, pidfile), "a running wrapper must hold its pidfile lock")
			require.NoError(t, syscall.Kill(h.pid, sig))

			h.requireCleanExit(t, pidfile)
			got, err := os.ReadFile(h.signalsFile)
			require.NoError(t, err, "wrapped command saw no signal")
			assert.Equal(t, "TERM", strings.TrimSpace(string(got)))
		})
	}
}

// TestStopRestoreCommandStopsRealWrapper is the path recruit takes:
// StopRestoreCommand signalling a live wrapper whose command ignores SIGTERM.
// pgctld must wait long enough for the wrapper to escalate to SIGKILL and exit
// on its own; killing the wrapper first would orphan the wrapped command.
func TestStopRestoreCommandStopsRealWrapper(t *testing.T) {
	poolerDir := t.TempDir()
	pidfile := pgctld.RestoreCommandPIDFile(poolerDir)
	h := startWrapperHelper(t, pidfile)

	svc := &PgCtldService{
		logger:   testLogger(),
		pgConfig: &pgctld.PostgresCtlConfig{PoolerDir: poolerDir},
	}
	resp, err := svc.StopRestoreCommand(context.Background(), &pb.StopRestoreCommandRequest{})
	require.NoError(t, err)
	assert.True(t, resp.Found)
	assert.True(t, resp.Killed)

	h.requireCleanExit(t, pidfile)
}

// pidfileLocked reports whether some open file still holds the exclusive lock
// a live wrapper keeps on its pidfile, the way StopRestoreCommand checks.
func pidfileLocked(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	if err == nil {
		return false
	}
	require.ErrorIs(t, err, syscall.EWOULDBLOCK)
	return true
}

// TestPublishPidfileIsAtomic polls the pidfile while it is republished
// repeatedly, the way StopRestoreCommand reads it while a standby runs
// restore_command on every archive fetch. A truncate-then-write publish shows
// readers an empty file.
func TestPublishPidfileIsAtomic(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "restore_command.pid")
	const pid = 4242
	want := strconv.Itoa(pid)

	type observation struct {
		count int
		first string
	}
	stop := make(chan struct{})
	done := make(chan observation, 1)
	go func() {
		var seen observation
		for {
			select {
			case <-stop:
				done <- seen
				return
			default:
			}
			data, err := os.ReadFile(pidfile)
			if err != nil {
				continue // not published yet
			}
			if string(data) != want {
				if seen.count == 0 {
					seen.first = string(data)
				}
				seen.count++
			}
		}
	}()
	// Stop the reader even if the publish loop fails the test early.
	var once sync.Once
	stopReader := func() observation {
		once.Do(func() { close(stop) })
		return <-done
	}
	t.Cleanup(func() { once.Do(func() { close(stop) }) })

	for range 2000 {
		f, err := publishPidfile(pidfile, pid)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	seen := stopReader()
	assert.Zerof(t, seen.count, "reader observed %d partially written pidfiles, first %q", seen.count, seen.first)

	// Permissions must match what a plain write with the same mode produces
	// under the current umask, as the pidfile always had.
	reference := filepath.Join(dir, "reference")
	require.NoError(t, os.WriteFile(reference, nil, 0o644))
	refInfo, err := os.Stat(reference)
	require.NoError(t, err)
	info, err := os.Stat(pidfile)
	require.NoError(t, err)
	assert.Equal(t, refInfo.Mode().Perm(), info.Mode().Perm())
	require.NoError(t, os.Remove(reference))

	assert.ElementsMatch(t, []string{"restore_command.pid", "restore_command.pid.lock"}, dirNames(t, dir), "temp files must not be left behind")
}

// breakGuard makes the guard file for pidfile unopenable by pointing it at a
// path that cannot be created.
func breakGuard(t *testing.T, pidfile string) {
	t.Helper()
	guard := pidfileGuardPath(pidfile)
	if err := os.Remove(guard); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, os.Symlink(filepath.Join(filepath.Dir(pidfile), "missing", "guard"), guard))
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestPublishPidfileFailureLeavesNothing(t *testing.T) {
	dir := t.TempDir()
	// Rename onto a directory fails after the temp file is fully written.
	target := filepath.Join(dir, "restore_command.pid")
	require.NoError(t, os.Mkdir(target, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(target, "keep"), nil, 0o644))

	f, err := publishPidfile(target, 1)
	require.Error(t, err)
	assert.Nil(t, f)
	assert.ElementsMatch(t, []string{"restore_command.pid", "restore_command.pid.lock"}, dirNames(t, dir), "failed publish must remove its temp file")
}

// TestRestoreWrapperFailsWhenPidfileCannotBePublished covers a pooler
// directory that is missing or unwritable: the wrapper must fail before it
// starts the wrapped command, since StopRestoreCommand could never find it.
func TestRestoreWrapperFailsWhenPidfileCannotBePublished(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	pidfile := filepath.Join(t.TempDir(), "missing", "restore_command.pid")

	err := runRestoreWrapper(nil, []string{pidfile, "--", "touch", marker})
	require.ErrorContains(t, err, "failed to write pidfile")
	assert.NoFileExists(t, marker, "wrapped command must not run without a published pidfile")
}

// TestReleasePidfileLeavesNewerWrapper covers two wrappers overlapping, as when
// one outlives its postgres: the older one exiting must not delete the newer
// one's pidfile, or StopRestoreCommand could no longer find the newer one.
func TestReleasePidfileLeavesNewerWrapper(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")

	older, err := publishPidfile(pidfile, 100)
	require.NoError(t, err)
	newer, err := publishPidfile(pidfile, 200)
	require.NoError(t, err)

	releasePidfile(pidfile, older)
	data, err := os.ReadFile(pidfile)
	require.NoError(t, err, "older wrapper removed the newer wrapper's pidfile")
	assert.Equal(t, "200", string(data))
	assert.True(t, pidfileLocked(t, pidfile), "newer wrapper's lock must survive the older one exiting")

	releasePidfile(pidfile, newer)
	assert.NoFileExists(t, pidfile)
}

// TestReleasePidfileRacingNewerPublish places a newer wrapper's publish in the
// window between the older wrapper confirming it owns the pidfile and removing
// it. Without the path guard the rename lands first and the remove deletes the
// newer wrapper's pidfile, hiding a live fetch from StopRestoreCommand.
func TestReleasePidfileRacingNewerPublish(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")

	older, err := publishPidfile(pidfile, 100)
	require.NoError(t, err)

	type result struct {
		f   *os.File
		err error
	}
	published := make(chan result, 1)
	contended := make(chan struct{}, 1)
	renamed := make(chan struct{})
	pidfileGuardContended = func() { contended <- struct{}{} }
	pidfileReleaseChecked = func() {
		go func() {
			f, err := publishPidfile(pidfile, 200)
			close(renamed)
			published <- result{f, err}
		}()
		// Either the newer wrapper renamed straight away (no guard) or it is
		// about to block on the guard held by this release.
		select {
		case <-renamed:
			t.Error("newer wrapper renamed into place while the older one was releasing")
		case <-contended:
		}
	}
	t.Cleanup(func() {
		pidfileGuardContended = nil
		pidfileReleaseChecked = nil
	})

	releasePidfile(pidfile, older)

	res := <-published
	require.NoError(t, res.err)
	t.Cleanup(func() { _ = res.f.Close() })
	data, err := os.ReadFile(pidfile)
	require.NoError(t, err, "older wrapper removed the newer wrapper's pidfile")
	assert.Equal(t, "200", string(data))
	assert.True(t, pidfileLocked(t, pidfile))
}

func TestReleasePidfileAlreadyGone(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")
	f, err := publishPidfile(pidfile, 100)
	require.NoError(t, err)
	require.NoError(t, os.Remove(pidfile))

	releasePidfile(pidfile, f)
	assert.NoFileExists(t, pidfile)
}

// A release that cannot take the guard must not remove anything: it cannot
// tell whether a newer wrapper is mid-publish.
func TestReleasePidfileKeepsPidfileWithoutGuard(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")
	f, err := publishPidfile(pidfile, 100)
	require.NoError(t, err)
	breakGuard(t, pidfile)

	releasePidfile(pidfile, f)
	assert.FileExists(t, pidfile)
	assert.False(t, pidfileLocked(t, pidfile), "the lock goes with the exiting wrapper")
}

func TestReleasePidfileWithClosedFile(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")
	f, err := publishPidfile(pidfile, 100)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	releasePidfile(pidfile, f)
	assert.FileExists(t, pidfile, "a wrapper that cannot identify its file removes nothing")
}

func TestPublishPidfileFailsWithoutGuard(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "restore_command.pid")
	breakGuard(t, pidfile)

	f, err := publishPidfile(pidfile, 100)
	require.Error(t, err)
	assert.Nil(t, f)
	assert.NoFileExists(t, pidfile)
	assert.Equal(t, []string{"restore_command.pid.lock"}, dirNames(t, dir), "failed publish must remove its temp file")
}

// TestRunRestoreWrapperPublishesWhileRunning runs the wrapper in-process: the
// pidfile must name this process and be locked while the wrapped command runs,
// and be gone, with the command's exit status returned, once it finishes.
func TestRunRestoreWrapperPublishesWhileRunning(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "restore_command.pid")
	snapshot := filepath.Join(t.TempDir(), "snapshot")

	script := `cat "$1" > "$2"; exit 3`
	err := runRestoreWrapper(nil, []string{pidfile, "--", "sh", "-c", script, "sh", pidfile, snapshot})
	require.Error(t, err)

	seen, readErr := os.ReadFile(snapshot)
	require.NoError(t, readErr)
	assert.Equal(t, strconv.Itoa(os.Getpid()), string(seen))
	assert.NoFileExists(t, pidfile)
}

func TestRunRestoreWrapperUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"pidfile"}, {"pidfile", "--"}, {"", "--", "true"}} {
		require.ErrorContains(t, runRestoreWrapper(nil, args), "usage:", "args %q", args)
	}
}

func TestRestoreWrapper_RenameOnSuccess(t *testing.T) {
	dir := t.TempDir()
	tmp, dest := filepath.Join(dir, "seg.fetch"), filepath.Join(dir, "seg")

	err := runRestoreWrapper(nil, []string{
		filepath.Join(dir, "w.pid"), "--rename", tmp, dest, "--",
		"sh", "-c", `printf wal > "$1"`, "sh", tmp,
	})
	require.NoError(t, err)

	got, err := os.ReadFile(dest)
	require.NoError(t, err)
	assert.Equal(t, "wal", string(got))
	assert.NoFileExists(t, tmp)
}

func TestRestoreWrapper_RemoveTmpOnFailure(t *testing.T) {
	dir := t.TempDir()
	tmp, dest := filepath.Join(dir, "seg.fetch"), filepath.Join(dir, "seg")

	err := runRestoreWrapper(nil, []string{
		filepath.Join(dir, "w.pid"), "--rename", tmp, dest, "--",
		"sh", "-c", `printf partial > "$1"; exit 22`, "sh", tmp,
	})
	require.Error(t, err)

	assert.NoFileExists(t, tmp)
	assert.NoFileExists(t, dest, "a failed fetch must not publish the destination")
}

func TestRestoreWrapper_RemoveTmpOnSignalAndExit0(t *testing.T) {
	const helperEnv = "MULTIGRES_RESTORE_WRAPPER_RENAME_TEST_HELPER"
	if os.Getenv(helperEnv) == "1" {
		err := runRestoreWrapper(nil, []string{
			os.Getenv("MULTIGRES_RESTORE_WRAPPER_PIDFILE"),
			"--rename", os.Getenv("MULTIGRES_RESTORE_WRAPPER_TMP"), os.Getenv("MULTIGRES_RESTORE_WRAPPER_DEST"), "--",
			"sh", "-c", `printf partial > "$1"; while :; do sleep 1; done`, "sh", os.Getenv("MULTIGRES_RESTORE_WRAPPER_TMP"),
		})
		require.NoError(t, err)
		return
	}

	dir := t.TempDir()
	pidfile := filepath.Join(dir, "wrapper.pid")
	tmp, dest := filepath.Join(dir, "seg.fetch"), filepath.Join(dir, "seg")
	helper := exec.Command(os.Args[0], "-test.run=^TestRestoreWrapper_RemoveTmpOnSignalAndExit0$")
	helper.Env = append(os.Environ(),
		helperEnv+"=1",
		"MULTIGRES_RESTORE_WRAPPER_PIDFILE="+pidfile,
		"MULTIGRES_RESTORE_WRAPPER_TMP="+tmp,
		"MULTIGRES_RESTORE_WRAPPER_DEST="+dest,
	)
	helper.Stdout = os.Stdout
	helper.Stderr = os.Stderr
	require.NoError(t, helper.Start())
	t.Cleanup(func() { _ = syscall.Kill(helper.Process.Pid, syscall.SIGKILL) })

	// The partial file exists only once the child is running, so waiting for it
	// proves the signal arrives mid-fetch.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(tmp)
		return err == nil && string(data) == "partial"
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, syscall.Kill(helper.Process.Pid, syscall.SIGTERM))

	waitCh := make(chan error, 1)
	go func() { waitCh <- helper.Wait() }()
	select {
	case err := <-waitCh:
		require.NoError(t, err, "wrapper must exit 0 after a forwarded signal")
	case <-time.After(5 * time.Second):
		t.Fatal("wrapper did not exit after SIGTERM")
	}

	assert.NoFileExists(t, tmp)
	assert.NoFileExists(t, dest)
}

func TestRestoreWrapper_RenameUsage(t *testing.T) {
	err := runRestoreWrapper(nil, []string{"pid", "--rename", "only-one"})
	require.ErrorIs(t, err, errUsage)
}
