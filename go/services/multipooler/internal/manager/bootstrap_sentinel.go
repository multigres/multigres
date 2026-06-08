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

func (pm *MultiPoolerManager) bootstrapSentinelPath() string {
	return filepath.Join(pm.record.PoolerDir(), constants.BootstrapSentinelFile)
}

// hasBootstrapSentinel reports whether the sentinel file exists. A non-existent
// file is (false, nil); any other stat failure (e.g. permissions) is surfaced
// as an error so callers don't silently treat it as "not present".
func (pm *MultiPoolerManager) hasBootstrapSentinel() (bool, error) {
	_, err := os.Stat(pm.bootstrapSentinelPath())
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (pm *MultiPoolerManager) writeBootstrapSentinel() error {
	return os.WriteFile(pm.bootstrapSentinelPath(), []byte("first-backup bootstrap in progress\n"), 0o644)
}

// removeBootstrapSentinel deletes the sentinel; a missing file is not an error.
func (pm *MultiPoolerManager) removeBootstrapSentinel() error {
	if err := os.Remove(pm.bootstrapSentinelPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
