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

// TestRestoreWrapperSignalExitsCleanly covers the wrapper being signalled while
// its command runs: SIGTERM from StopRestoreCommand, and SIGQUIT from
// postgres's immediate shutdown. Either way the wrapped command's process group
// must be gone and the pidfile removed when the wrapper exits. The wrapped
// command ignores the signal, so this also exercises the SIGKILL escalation.
func TestRestoreWrapperSignalExitsCleanly(t *testing.T) {
	const helperEnv = "MULTIGRES_RESTORE_WRAPPER_TEST_HELPER"
	if os.Getenv(helperEnv) == "1" {
		err := runRestoreWrapper(nil, []string{
			os.Getenv("MULTIGRES_RESTORE_WRAPPER_PIDFILE"), "--", "sh", "-c",
			`trap '' TERM INT QUIT; echo $$ > "$1"; while :; do sleep 1; done`,
			"sh", os.Getenv("MULTIGRES_RESTORE_WRAPPER_CHILD_PIDFILE"),
		})
		require.NoError(t, err)
		return
	}

	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGQUIT} {
		t.Run(sig.String(), func(t *testing.T) {
			dir := t.TempDir()
			pidfile := filepath.Join(dir, "wrapper.pid")
			childPIDFile := filepath.Join(dir, "child.pid")
			helper := exec.Command(os.Args[0], "-test.run=^TestRestoreWrapperSignalExitsCleanly$")
			helper.Env = append(os.Environ(),
				helperEnv+"=1",
				"MULTIGRES_RESTORE_WRAPPER_PIDFILE="+pidfile,
				"MULTIGRES_RESTORE_WRAPPER_CHILD_PIDFILE="+childPIDFile,
			)
			helper.Stdout = os.Stdout
			helper.Stderr = os.Stderr
			require.NoError(t, helper.Start())

			childPID := 0
			t.Cleanup(func() {
				_ = syscall.Kill(helper.Process.Pid, syscall.SIGKILL)
				if childPID != 0 {
					_ = syscall.Kill(-childPID, syscall.SIGKILL)
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

			wrapperPID := 0
			require.Eventually(t, func() bool {
				var wrapperOK, childOK bool
				wrapperPID, wrapperOK = readPID(pidfile)
				childPID, childOK = readPID(childPIDFile)
				return wrapperOK && childOK
			}, 5*time.Second, 10*time.Millisecond, "wrapper and child did not publish their PIDs")
			require.Equal(t, helper.Process.Pid, wrapperPID)
			assert.True(t, pidfileLocked(t, pidfile), "a running wrapper must hold its pidfile lock")
			require.NoError(t, syscall.Kill(wrapperPID, sig))

			waitCh := make(chan error, 1)
			go func() { waitCh <- helper.Wait() }()
			select {
			case err := <-waitCh:
				require.NoError(t, err, "wrapper did not exit cleanly")
			case <-time.After(3 * time.Second):
				t.Fatalf("wrapper did not exit after %v", sig)
			}

			require.Eventually(t, func() bool {
				_, err := os.Stat(pidfile)
				return os.IsNotExist(err)
			}, time.Second, 10*time.Millisecond, "wrapper pidfile was not removed")
			require.Eventually(t, func() bool {
				process, err := os.FindProcess(childPID)
				return err != nil || process.Signal(syscall.Signal(0)) != nil
			}, time.Second, 10*time.Millisecond, "wrapped process %d is still running", childPID)
		})
	}
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

	for range 20000 {
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

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "temp files must not be left behind")
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
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "failed publish must remove its temp file")
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
