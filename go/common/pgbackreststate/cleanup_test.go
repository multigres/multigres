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

package pgbackreststate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func stampedOwner(t *testing.T) (string, *os.File) {
	t.Helper()
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, InstanceLockName), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	require.NoError(t, unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	require.NoError(t, RecordSupervision(dir, f))
	return dir, f
}

func TestCleanupGuardMissingOrUnverifiedState(t *testing.T) {
	t.Run("missing directory creates nothing", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "missing")
		guard, err := AcquireCleanupGuard(dir)
		require.Nil(t, guard)
		require.ErrorIs(t, err, ErrUnverifiedState)
		require.False(t, errors.Is(err, os.ErrNotExist), "unsafe missing state must not look like already-clean success")
		require.NoDirExists(t, dir)
	})
	for _, content := range []string{"", "not a supervision record", "multigres-pgbackrestd-owned-workers-v2\n"} {
		t.Run("unrecognized "+content, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, InstanceLockName)
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			guard, err := AcquireCleanupGuard(dir)
			require.Nil(t, guard)
			require.ErrorIs(t, err, ErrUnverifiedState)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, string(after))
		})
	}
	t.Run("missing lock is not synthesized", func(t *testing.T) {
		dir := t.TempDir()
		guard, err := AcquireCleanupGuard(dir)
		require.Nil(t, guard)
		require.ErrorIs(t, err, ErrUnverifiedState)
		require.False(t, errors.Is(err, os.ErrNotExist), "missing lock evidence is not already-clean success")
		require.NoFileExists(t, filepath.Join(dir, InstanceLockName))
	})
}

func TestCleanupGuardLiveOwnerAndLease(t *testing.T) {
	dir, owner := stampedOwner(t)
	guard, err := AcquireCleanupGuard(dir)
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrStateInUse)
	require.NoError(t, owner.Close())
	before, err := os.ReadFile(filepath.Join(dir, InstanceLockName))
	require.NoError(t, err)
	guard, err = AcquireCleanupGuard(dir)
	require.NoError(t, err)
	// A guard is not a check-then-release: another owner cannot acquire this
	// inode until the cleanup caller explicitly finishes and closes the guard.
	other, err := os.OpenFile(filepath.Join(dir, InstanceLockName), os.O_RDWR, 0)
	require.NoError(t, err)
	defer other.Close()
	require.Error(t, unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	after, err := os.ReadFile(filepath.Join(dir, InstanceLockName))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, guard.Close())
	require.NoError(t, guard.Close())
	require.NoError(t, unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB))
}

func TestCleanupGuardVetoesAllActiveMarkers(t *testing.T) {
	for _, kind := range []string{"opaque", "directory", "symlink", "scoped prior namespace"} {
		t.Run(kind, func(t *testing.T) {
			dir, owner := stampedOwner(t)
			require.NoError(t, owner.Close())
			path := filepath.Join(dir, "worker-unknown.pending")
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o700))
			case "symlink":
				require.NoError(t, os.Symlink(filepath.Join(dir, "missing"), path))
			case "scoped prior namespace":
				require.NoError(t, os.WriteFile(path, []byte(`{"version":1,"owner":{"platform":"linux","pid":1,"pid_namespace_inode":123}}`), 0o600))
			default:
				require.NoError(t, os.WriteFile(path, []byte("unsettled\n"), 0o600))
			}
			guard, err := AcquireCleanupGuard(dir)
			require.Nil(t, guard)
			require.ErrorIs(t, err, ErrWorkersUnsettled)
			_, err = os.Lstat(path)
			require.NoError(t, err, "inspection must not retire or remove evidence")
			// Refusal must release its lock, not wedge subsequent diagnostic attempts.
			owner, err = os.OpenFile(filepath.Join(dir, InstanceLockName), os.O_RDWR, 0)
			require.NoError(t, err)
			defer owner.Close()
			require.NoError(t, unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		})
	}
}

func TestCleanupGuardIgnoresRetiredAuditAndDataValidity(t *testing.T) {
	dir, owner := stampedOwner(t)
	require.NoError(t, owner.Close())
	require.NoError(t, os.Mkdir(filepath.Join(dir, "recovered-workers"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "recovered-workers", "worker-old.pending"), []byte("audit"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "state"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state", "restore.pb"), []byte("interrupted journal"), 0o600))
	guard, err := AcquireCleanupGuard(dir)
	require.NoError(t, err)
	// No other launchers exist in this isolated fixture. Real callers must hold
	// a lifecycle lock outside this tree while deleting the lock pathname.
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, guard.Close())
}

func TestCleanupGuardRejectsSymlinksAndFIFO(t *testing.T) {
	dir, owner := stampedOwner(t)
	require.NoError(t, owner.Close())
	link := filepath.Join(t.TempDir(), "state-link")
	require.NoError(t, os.Symlink(dir, link))
	guard, err := AcquireCleanupGuard(link)
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrUnverifiedState)
	other := t.TempDir()
	require.NoError(t, os.Symlink(filepath.Join(dir, InstanceLockName), filepath.Join(other, InstanceLockName)))
	guard, err = AcquireCleanupGuard(other)
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrUnverifiedState)
	fifo := t.TempDir()
	require.NoError(t, unix.Mkfifo(filepath.Join(fifo, InstanceLockName), 0o600))
	guard, err = AcquireCleanupGuard(fifo)
	require.Nil(t, guard)
	require.ErrorIs(t, err, ErrUnverifiedState)
}

func TestServingProtocolPreservesUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, InstanceLockName)
	original := []byte("future-version\n")
	require.NoError(t, os.WriteFile(path, original, 0o600))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	require.ErrorIs(t, ValidateServingProtocol(f), ErrUnverifiedState)
	require.ErrorIs(t, RecordSupervision(dir, f), ErrUnverifiedState)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, after)
}
