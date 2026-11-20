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
	"syscall"

	"github.com/multigres/multigres/go/common/safefs"
)

// legacyBackupScopeFile is where a pooler that ran pgBackRest in-process
// recorded the repository its backups went to, inside the pgbackrest directory
// that the sidecar cutover removes.
const legacyBackupScopeFile = "pgbackrest/repository_scope"

// describeRepositoryScope names a scope for an error message.
func describeRepositoryScope(scope string) string {
	if scope == "" {
		return "the backup location's base path (the catalog shard)"
	}
	return fmt.Sprintf("%q below the backup location", scope)
}

// checkSidecarScope fails when the sidecar serves another shard repository
// than the one this pooler's --table-group, --tablegroup-id and --shard
// select. The sidecar owns the repository and pins it, so this is a
// comparison of two configurations, never a second pin: a mismatch means the
// pooler and its sidecar were started for different shards.
func checkSidecarScope(sidecarScope, poolerScope string) error {
	if sidecarScope == poolerScope {
		return nil
	}
	return fmt.Errorf("the pgbackrest sidecar uses the repository at %s, but this pooler's --table-group, --tablegroup-id and --shard select %s",
		describeRepositoryScope(sidecarScope), describeRepositoryScope(poolerScope))
}

// checkLegacyScopePin carries the repository a pooler directory was pinned to
// before the sidecar existed across the cutover. The sidecar pins its own
// state directory only once it first configures a repository, so a directory
// that last backed up to another shard repository must be refused here, before
// the file that records it is removed. An absent file leaves nothing to carry.
func checkLegacyScopePin(root *safefs.Root, sidecarScope string) error {
	data, err := root.ReadFile(legacyBackupScopeFile)
	// A link or a plain file where the directory should be is not something
	// the in-process pgBackRest wrote, so it holds no pin. It is not followed:
	// the removal that comes next deletes it as it is.
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.ENOTDIR) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the legacy backup repository scope: %w", err)
	}
	if recorded := string(data); recorded != sidecarScope {
		return fmt.Errorf("this pooler directory backs up to the repository at %s, but the pgbackrest sidecar uses %s; "+
			"a changed --table-group, --tablegroup-id or --shard would leave this shard's backups and WAL behind. "+
			"If the change is intended, remove %s from the pooler directory and restart",
			describeRepositoryScope(recorded), describeRepositoryScope(sidecarScope), legacyBackupScopeFile)
	}
	return nil
}
