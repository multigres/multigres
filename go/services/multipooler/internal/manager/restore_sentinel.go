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

// restore_sentinel.go implements a durable, on-disk marker for an in-progress
// pgBackRest restore, mirroring the bootstrap and rewind sentinels (see
// rpc_first_backup.go and rewind_sentinel.go). pgbackrest restore writes PGDATA
// file by file, and PG_VERSION can land well before the restore finishes, so an
// interrupted restore (typically the pod is killed mid-restore) leaves a
// partial data directory that hasDataDirectory() reads as initialized. The
// cleanup in restoreFromBackupLocked's error path cannot run when the process
// itself dies.
//
// restoreFromBackupLocked writes the sentinel just before pgbackrest restore
// runs and removes it as soon as the restore completes, or after the partial
// data directory has been removed on failure. Its presence on a later monitor
// tick is therefore the authoritative signal that a prior restore did not
// complete. The monitor uses it to route to the restore path, which removes the
// partial data directory and restores again instead of starting postgres on it.

// restoreSentinelPath is the on-disk location of the restore sentinel. It lives
// in pooler_dir (not PGDATA) so it is not captured by pgBackRest backups or
// removed along with a partial data directory, and on the pooler's local volume
// so it survives a pod restart on the same PVC.
func (pm *MultipoolerManager) restoreSentinelPath() string {
	return filepath.Join(pm.record.PoolerDir(), constants.RestoreSentinelFile)
}

// hasRestoreSentinel reports whether the sentinel file exists. A non-existent
// file is (false, nil); any other stat failure (e.g. permissions) is surfaced as
// an error so callers don't silently treat it as "not present".
func (pm *MultipoolerManager) hasRestoreSentinel() (bool, error) {
	_, err := os.Stat(pm.restoreSentinelPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// writeRestoreSentinel creates the sentinel and fsyncs both the file and its
// parent directory so the marker is durable across an OS crash — the sentinel
// is only useful if it reliably outlives the very interruption it guards
// against.
func (pm *MultipoolerManager) writeRestoreSentinel() error {
	path := pm.restoreSentinelPath()
	if err := os.WriteFile(path, []byte("pgbackrest restore in progress\n"), 0o644); err != nil {
		return err
	}
	if err := fsyncPath(path); err != nil {
		return err
	}
	// fsync the directory so the new directory entry itself is durable.
	return fsyncPath(filepath.Dir(path))
}

// removeRestoreSentinel deletes the sentinel; a missing file is not an error.
func (pm *MultipoolerManager) removeRestoreSentinel() error {
	if err := os.Remove(pm.restoreSentinelPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
