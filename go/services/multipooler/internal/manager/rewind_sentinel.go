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
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/common/constants"
)

// rewind_sentinel.go implements a durable, on-disk marker for an in-progress
// pg_rewind, mirroring the bootstrap sentinel (see rpc_first_backup.go). pg_rewind
// mutates the target data directory in place and is not transactional: if it is
// interrupted (typically the pod is killed mid-rewind, exceeding the shutdown
// grace period), the directory is left partially rewound — unstartable and, per
// PostgreSQL guidance, generally unrecoverable.
//
// restartAsStandbyLocked writes the sentinel just before the mutating pg_rewind
// runs and removes it only after postgres is verified back up as a standby. Its
// presence on a later monitor tick is therefore the authoritative signal that a
// prior rewind did not complete — the one durable signal that survives a process
// restart (the in-memory suspectedDivergence flag does not). The monitor uses it
// to re-arm the rewind-repair path instead of starting postgres on the
// half-rewound directory, and to let the unrecoverable-recovery classifier
// quarantine the node if repair keeps failing.

// rewindSentinelPath is the on-disk location of the rewind sentinel. It lives in
// pooler_dir (not PGDATA) so it is not captured by pgBackRest backups, and on the
// pooler's local volume so it survives a pod restart on the same PVC.
func (pm *MultipoolerManager) rewindSentinelPath() string {
	return filepath.Join(pm.record.PoolerDir(), constants.RewindSentinelFile)
}

// hasRewindSentinel reports whether the sentinel file exists. A non-existent file
// is (false, nil); any other stat failure (e.g. permissions) is surfaced as an
// error so callers don't silently treat it as "not present".
func (pm *MultipoolerManager) hasRewindSentinel() (bool, error) {
	_, err := os.Stat(pm.rewindSentinelPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// writeRewindSentinel creates the sentinel and fsyncs both the file and its
// parent directory so the marker is durable across an OS crash — the sentinel is
// only useful if it reliably outlives the very interruption it guards against.
func (pm *MultipoolerManager) writeRewindSentinel() error {
	path := pm.rewindSentinelPath()
	if err := os.WriteFile(path, []byte("pg_rewind in progress\n"), 0o644); err != nil {
		return err
	}
	if err := fsyncPath(path); err != nil {
		return err
	}
	// fsync the directory so the new directory entry itself is durable.
	return fsyncPath(filepath.Dir(path))
}

// removeRewindSentinel deletes the sentinel; a missing file is not an error.
func (pm *MultipoolerManager) removeRewindSentinel() error {
	if err := os.Remove(pm.rewindSentinelPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// fsyncPath opens path (a file or directory) and fsyncs it.
func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
