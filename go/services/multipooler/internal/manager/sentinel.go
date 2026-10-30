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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/safefs"
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

	// root, when set, routes every operation through a safefs root on pooler_dir
	// instead of plain os calls, so a symlink planted by postgres on the shared
	// data volume is replaced or reported rather than followed. Only the restore
	// sentinel sets it; the others keep plain os access.
	root func() (*safefs.Root, error)
	name string
}

// newSentinel returns the sentinel with the given file name in this pooler's
// pooler_dir.
func (pm *MultipoolerManager) newSentinel(name, contents string) sentinel {
	return sentinel{
		path:     filepath.Join(pm.record.PoolerDir(), name),
		contents: contents,
		name:     name,
	}
}

// present reports whether the sentinel file exists. A non-existent file is
// (false, nil); any other stat failure (e.g. permissions) is surfaced as an
// error so callers don't silently treat it as "not present".
func (s sentinel) present() (bool, error) {
	ok, _, err := s.info()
	return ok, err
}

// info is present plus the sentinel's modification time, which is when the
// operation it stands for was started.
func (s sentinel) info() (bool, time.Time, error) {
	if s.root != nil {
		return s.infoRoot()
	}
	fi, err := os.Stat(s.path)
	if err == nil {
		return true, fi.ModTime(), nil
	}
	if os.IsNotExist(err) {
		return false, time.Time{}, nil
	}
	return false, time.Time{}, err
}

func (s sentinel) infoRoot() (bool, time.Time, error) {
	root, err := s.root()
	if err != nil {
		return false, time.Time{}, err
	}
	defer root.Close()
	fi, err := root.Stat(s.name)
	if err == nil {
		return true, fi.ModTime(), nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, time.Time{}, nil
	}
	return false, time.Time{}, err
}

// write creates the sentinel and fsyncs both the file and its parent directory
// so the marker is durable across an OS crash — the sentinel is only useful if
// it reliably outlives the very interruption it guards against.
func (s sentinel) write() error {
	if s.root != nil {
		root, err := s.root()
		if err != nil {
			return err
		}
		defer root.Close()
		// safefs writes a temp file and renames it, fsyncing both file and directory.
		return root.WriteFile(s.name, []byte(s.contents), 0o600)
	}
	if err := os.WriteFile(s.path, []byte(s.contents), 0o644); err != nil {
		return err
	}
	if err := fsyncPath(s.path); err != nil {
		return err
	}
	// fsync the directory so the new directory entry itself is durable.
	return fsyncPath(filepath.Dir(s.path))
}

// remove deletes the sentinel; a missing file is not an error. Like write, it
// fsyncs the parent directory so the removal is durable: an unlink lost to a
// power failure would bring the sentinel back after its operation completed.
// The fsync also runs when the file is already gone, since an earlier remove
// may have unlinked it and then failed before its own fsync.
func (s sentinel) remove() error {
	if s.root != nil {
		root, err := s.root()
		if err != nil {
			return err
		}
		defer root.Close()
		if err := root.Remove(s.name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		// safefs does not fsync after an unlink. pooler_dir is the anchored
		// directory the root was opened on, so syncing it by path reaches the same
		// directory.
		return fsyncPath(filepath.Dir(s.path))
	}
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return fsyncPath(filepath.Dir(s.path))
}

// testHookFsyncPath, when set, sees every path fsyncPath is asked to sync.
var testHookFsyncPath func(path string)

// fsyncPath opens path (a file or directory) and fsyncs it.
func fsyncPath(path string) error {
	if testHookFsyncPath != nil {
		testHookFsyncPath(path)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
