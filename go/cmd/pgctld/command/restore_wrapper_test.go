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

func TestRestoreWrapperSIGTERMExitsCleanly(t *testing.T) {
	const helperEnv = "MULTIGRES_RESTORE_WRAPPER_TEST_HELPER"
	if os.Getenv(helperEnv) == "1" {
		err := runRestoreWrapper(nil, []string{
			os.Getenv("MULTIGRES_RESTORE_WRAPPER_PIDFILE"), "--", "sh", "-c",
			`trap '' TERM INT; echo $$ > "$1"; while :; do sleep 1; done`,
			"sh", os.Getenv("MULTIGRES_RESTORE_WRAPPER_CHILD_PIDFILE"),
		})
		require.NoError(t, err)
		return
	}

	dir := t.TempDir()
	pidfile := filepath.Join(dir, "wrapper.pid")
	childPIDFile := filepath.Join(dir, "child.pid")
	helper := exec.Command(os.Args[0], "-test.run=^TestRestoreWrapperSIGTERMExitsCleanly$")
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
	require.NoError(t, syscall.Kill(wrapperPID, syscall.SIGTERM))

	waitCh := make(chan error, 1)
	go func() { waitCh <- helper.Wait() }()
	select {
	case err := <-waitCh:
		require.NoError(t, err, "wrapper did not exit cleanly")
	case <-time.After(3 * time.Second):
		t.Fatal("wrapper did not exit after SIGTERM")
	}

	require.Eventually(t, func() bool {
		_, err := os.Stat(pidfile)
		return os.IsNotExist(err)
	}, time.Second, 10*time.Millisecond, "wrapper pidfile was not removed")
	require.Eventually(t, func() bool {
		process, err := os.FindProcess(childPID)
		return err != nil || process.Signal(syscall.Signal(0)) != nil
	}, time.Second, 10*time.Millisecond, "wrapped process %d is still running", childPID)
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
