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
// partial data directory that must not be trusted as initialized.
//
// restoreFromBackupLocked writes the operation identity into the durable marker
// before sending Run. It removes it only after that operation has succeeded and
// archive configuration has been normalized. Failure never deletes PGDATA here.
// On the next tick the manager selects a complete backup, then the sidecar
// fences the prior request and cleans interrupted data under its restore gate,
// after positive worker quiescence. Marker presence or idle Status alone never
// authorizes cleanup.
//
// Because the restore path removes the data directory whenever the sentinel is
// present, nothing may start postgres while it is: postgres serving that data
// would make the sentinel stale, and a later restart would delete the data.
// pgctld therefore refuses every start while the sentinel is on disk, and the
// first-backup bootstrap, which starts postgres itself, clears it first.

// restoreSentinel returns the restore sentinel. Like every sentinel it lives in
// pooler_dir, so it is not removed along with a partial data directory; see
// sentinel.go.
func (pm *MultipoolerManager) restoreSentinel() sentinel {
	s := pm.newSentinel(constants.RestoreSentinelFile, "pgbackrest restore in progress\n")
	// The sidecar shares the data volume with postgres, so the marker is
	// reached through a mount-anchored safefs root.
	s.root = pm.openGenerationsRoot
	return s
}

// The sentinel is also the restore marker that gates every decision to start
// postgres, treat PGDATA as initialized or restore (see restoreSettled):
//   - the restore runs in the sidecar and can outlive this process, so the
//     sentinel is removed only after the sidecar confirmed the restore ended
//     and the steps after it ran, not when pgbackrest returns;
//   - a failed restore removes nothing, because the sidecar may still be
//     writing PGDATA: the next attempt must fence it under the sidecar gate;
//   - a sentinel older than restoreMarkerStaleAfter counts toward quarantine.
