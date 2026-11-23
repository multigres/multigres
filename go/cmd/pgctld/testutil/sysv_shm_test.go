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

package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// createSegment creates a private segment owned by this process and removes it
// at the end of the test if it is still present.
func createSegment(t *testing.T) int {
	t.Helper()
	shmid, err := unix.SysvShmGet(unix.IPC_PRIVATE, 64, unix.IPC_CREAT|0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = unix.SysvShmCtl(shmid, unix.IPC_RMID, nil) })
	return shmid
}

func writePostmasterPID(t *testing.T, dir string, pid, shmid int) {
	t.Helper()
	dataDir := filepath.Join(dir, "pg_data")
	require.NoError(t, os.MkdirAll(dataDir, 0o700))
	content := fmt.Sprintf("%d\n%s\n1700000000\n5432\n/tmp\n*\n%9d %9d\nready   \n", pid, dataDir, 1234, shmid)
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "postmaster.pid"), []byte(content), 0o600))
}

func segmentExists(shmid int) bool {
	var desc unix.SysvShmDesc
	_, err := unix.SysvShmCtl(shmid, unix.IPC_STAT, &desc)
	return err == nil
}

func TestReleaseOrphanedSharedMemory(t *testing.T) {
	t.Run("removes unattached segment of recorded postmaster", func(t *testing.T) {
		dir := t.TempDir()
		shmid := createSegment(t)
		writePostmasterPID(t, dir, os.Getpid(), shmid)

		ReleaseOrphanedSharedMemory(t.Logf, dir)

		require.False(t, segmentExists(shmid))
	})

	t.Run("keeps attached segment", func(t *testing.T) {
		dir := t.TempDir()
		shmid := createSegment(t)
		mem, err := unix.SysvShmAttach(shmid, 0, 0)
		require.NoError(t, err)
		defer func() { _ = unix.SysvShmDetach(mem) }()
		writePostmasterPID(t, dir, os.Getpid(), shmid)

		ReleaseOrphanedSharedMemory(t.Logf, dir)

		require.True(t, segmentExists(shmid))
	})

	t.Run("keeps segment created by another process", func(t *testing.T) {
		dir := t.TempDir()
		shmid := createSegment(t)
		writePostmasterPID(t, dir, DeadPID, shmid)

		ReleaseOrphanedSharedMemory(t.Logf, dir)

		require.True(t, segmentExists(shmid))
	})

	t.Run("ignores postmaster.pid without shared memory line", func(t *testing.T) {
		dir := t.TempDir()
		CreateDeadPIDFile(t, dir, DeadPID)

		ReleaseOrphanedSharedMemory(t.Logf, dir)
	})
}
