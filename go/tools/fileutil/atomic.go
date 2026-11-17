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

package fileutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// AtomicWriteFile writes data to a file atomically by writing to a temporary
// file first and then renaming it to the target path. This ensures that the
// target file is never in a partially written state.
//
// The function creates a temporary file in the same directory as the target,
// writes the data, syncs it to disk, atomically renames it to the target path,
// and then syncs the directory so the rename itself survives a power loss.
// This provides durability guarantees and prevents readers from seeing partial
// writes. If the directory sync fails the new content is already in place but
// not known to be durable, and an error is returned.
func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	if err := tmpFile.Chmod(perm); err != nil {
		return fmt.Errorf("failed to set permissions: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	// The rename is a directory update; without a directory fsync a power loss can revert it.
	return syncDir(dir)
}

// syncDir fsyncs a directory so a rename or create inside it is durable.
// fsync on a file does not persist its directory entry; see fsync(2)
// (https://man7.org/linux/man-pages/man2/fsync.2.html) and the same
// rename-then-fsync-directory sequence in PostgreSQL's durable_rename()
// (src/backend/storage/file/fd.c) and etcd's WAL (server/storage/wal/wal.go).
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory for sync: %w", err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}
	return nil
}
