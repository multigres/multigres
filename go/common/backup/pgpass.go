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

package backup

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/tools/fileutil"
)

// WritePgpassFile writes the postgres credentials used by pgbackrest and
// returns the resulting file path.
func WritePgpassFile(poolerDir, user, password string) (string, error) {
	pgpassDir := filepath.Join(poolerDir, "pgbackrest")
	if err := os.MkdirAll(pgpassDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create pgbackrest directory: %w", err)
	}

	pgpassPath := filepath.Join(pgpassDir, "pgbackrest.pgpass")
	pgpassContent := fmt.Sprintf("*:*:*:%s:%s\n", user, password)
	if err := fileutil.AtomicWriteFile(pgpassPath, []byte(pgpassContent), 0o600); err != nil {
		return "", fmt.Errorf("failed to write pgbackrest pgpass file: %w", err)
	}

	return pgpassPath, nil
}

// RestorePgpassMode resets the pgpass file at path to 0600 if anything has
// widened its permissions, and reports whether it did. libpq silently ignores a
// password file with any group or world access, so a widened file makes every
// replication and pgbackrest connection fail with "no password supplied"
// rather than with an error that points at the file. Kubelet widens it when it
// re-applies a pod's fsGroup to the volume. A missing file is not an error:
// nothing is using it.
func RestorePgpassMode(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to stat pgbackrest pgpass file: %w", err)
	}
	if info.Mode().Perm()&0o077 == 0 {
		return false, nil
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return false, fmt.Errorf("failed to restore pgbackrest pgpass file mode: %w", err)
	}
	return true, nil
}
