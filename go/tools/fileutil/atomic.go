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
// writes the data, syncs it to disk, and then atomically renames it to the
// target path. This provides durability guarantees and prevents readers from
// seeing partial writes.
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

	return nil
}
