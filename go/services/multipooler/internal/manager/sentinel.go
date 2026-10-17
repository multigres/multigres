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
)

// sentinel is a durable, on-disk marker for an operation that changes PGDATA in
// several steps and must not be mistaken for complete if the process dies
// partway through. There is one per operation: the first-backup bootstrap (see
// rpc_first_backup.go), the rewind recovery (rewind_sentinel.go) and the
// pgBackRest restore (restore_sentinel.go). Each is written before its
// operation starts and removed once it completes, so finding one on a later
// monitor tick means the operation was interrupted.
//
// Sentinels live in pooler_dir (not PGDATA), so they are not captured by
// pgBackRest backups or removed along with a data directory, and on the
// pooler's local volume, so they survive a pod restart on the same PVC.
type sentinel struct {
	path     string
	contents string
}

// newSentinel returns the sentinel with the given file name in this pooler's
// pooler_dir.
func (pm *MultipoolerManager) newSentinel(name, contents string) sentinel {
	return sentinel{
		path:     filepath.Join(pm.record.PoolerDir(), name),
		contents: contents,
	}
}

// present reports whether the sentinel file exists. A non-existent file is
// (false, nil); any other stat failure (e.g. permissions) is surfaced as an
// error so callers don't silently treat it as "not present".
func (s sentinel) present() (bool, error) {
	_, err := os.Stat(s.path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// write creates the sentinel and fsyncs both the file and its parent directory
// so the marker is durable across an OS crash — the sentinel is only useful if
// it reliably outlives the very interruption it guards against.
func (s sentinel) write() error {
	if err := os.WriteFile(s.path, []byte(s.contents), 0o644); err != nil {
		return err
	}
	if err := fsyncPath(s.path); err != nil {
		return err
	}
	// fsync the directory so the new directory entry itself is durable.
	return fsyncPath(filepath.Dir(s.path))
}

// remove deletes the sentinel; a missing file is not an error.
func (s sentinel) remove() error {
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
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
