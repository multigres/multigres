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

package manager

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/tools/fileutil"
)

// backupScopeFile is the file, in the pooler directory's pgbackrest
// directory, that records which shard repository this pooler's backups go to
// (backup.Config.RepositoryScope).
const backupScopeFile = "repository_scope"

// checkBackupRepositoryScope pins a pooler directory to one shard repository.
// The first start records scope; a later start with a different scope, such
// as the same directory restarted with another --tablegroup-id, is refused.
// The repository it would switch to holds none of this shard's backups or WAL
// and has no stanza, so archiving would fail and WAL would build up until the
// disk fills. Removing the file accepts the new repository on the next start.
func (pm *MultipoolerManager) checkBackupRepositoryScope(scope string) error {
	dir := filepath.Join(pm.record.PoolerDir(), "pgbackrest")
	path := filepath.Join(dir, backupScopeFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("recording the backup repository scope: %w", err)
		}
		if err := fileutil.AtomicWriteFile(path, []byte(scope), 0o600); err != nil {
			return fmt.Errorf("recording the backup repository scope: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading the backup repository scope: %w", err)
	}
	if recorded := string(data); recorded != scope {
		return fmt.Errorf("this pooler directory backs up to the repository at %s, but this start would use %s; "+
			"a changed --table-group, --tablegroup-id or --shard would leave this shard's backups and WAL behind. "+
			"If the change is intended, remove %s and restart",
			describeRepositoryScope(recorded), describeRepositoryScope(scope), path)
	}
	return nil
}

// describeRepositoryScope names a scope for an error message.
func describeRepositoryScope(scope string) string {
	if scope == "" {
		return "the backup location's base path (the catalog shard)"
	}
	return fmt.Sprintf("%q below the backup location", scope)
}
