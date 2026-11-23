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
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// ReleaseOrphanedSharedMemory removes the System V shared memory segment of
// every dead postmaster whose postmaster.pid lives under dir.
//
// A postmaster that exits uncleanly (SIGKILL, test timeout) leaves its segment
// behind. PostgreSQL reclaims it only when a new postmaster starts in the same
// data directory, so once a test deletes the directory the segment leaks for
// good. macOS allows just 32 segments system-wide (kern.sysv.shmmni), after
// which every postgres start fails with "could not create shared memory
// segment: No space left on device". Call this before removing data
// directories.
//
// A segment is removed only if it was created by the postmaster recorded in
// postmaster.pid and nothing is attached to it, so live servers and reused
// segment IDs are left alone.
func ReleaseOrphanedSharedMemory(logf func(string, ...any), dir string) {
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.Name() != "postmaster.pid" || !d.Type().IsRegular() {
			return nil //nolint:nilerr // Best effort: keep walking past unreadable entries
		}
		pid, shmid, ok := parsePostmasterShm(path)
		if !ok {
			return nil
		}
		var desc unix.SysvShmDesc
		if _, err := unix.SysvShmCtl(shmid, unix.IPC_STAT, &desc); err != nil {
			return nil //nolint:nilerr // Segment already gone
		}
		if int(desc.Cpid) != pid || desc.Nattch != 0 {
			return nil
		}
		if _, err := unix.SysvShmCtl(shmid, unix.IPC_RMID, nil); err != nil {
			logf("Warning: failed to remove shared memory segment %d from %s: %v", shmid, path, err)
			return nil
		}
		logf("Removed orphaned shared memory segment %d from %s", shmid, path)
		return nil
	})
}

// parsePostmasterShm returns the postmaster PID (line 1) and shared memory ID
// (second field of line 7, "key shmid") from a postmaster.pid file.
func parsePostmasterShm(path string) (pid, shmid int, ok bool) {
	// #nosec G304 -- path comes from walking the test's own temp dir.
	content, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, false
	}
	lines := strings.Split(string(content), "\n")
	if len(lines) < 7 {
		return 0, 0, false
	}
	pid, err = strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return 0, 0, false
	}
	fields := strings.Fields(lines[6])
	if len(fields) != 2 {
		return 0, 0, false
	}
	shmid, err = strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, false
	}
	return pid, shmid, true
}
