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

// restoreSentinel returns the restore sentinel. Like every sentinel it lives in
// pooler_dir, so it is not removed along with a partial data directory; see
// sentinel.go.
func (pm *MultipoolerManager) restoreSentinel() sentinel {
	return pm.newSentinel(constants.RestoreSentinelFile, "pgbackrest restore in progress\n")
}
