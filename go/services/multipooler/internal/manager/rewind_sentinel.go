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

// rewind_sentinel.go implements a durable, on-disk marker for an in-progress
// rewind recovery, mirroring the bootstrap sentinel (see rpc_first_backup.go).
// The recovery temporarily enables archive restore and pg_rewind mutates the
// target data directory in place, so an interruption can leave either unsafe
// archive recovery enabled or a partially rewound directory.
//
// runPgRewind writes the sentinel before enabling archive restore and
// restartAsStandbyLocked removes it only after postgres is verified back up as a
// standby. Its
// presence on a later monitor tick is therefore the authoritative signal that a
// prior rewind did not complete — the one durable signal that survives a process
// restart (the in-memory suspectedDivergence flag does not). The monitor uses it
// to re-arm the rewind-repair path instead of starting postgres on the
// half-rewound directory, and to let the unrecoverable-recovery classifier
// quarantine the node if repair keeps failing.

// rewindSentinel returns the rewind sentinel. Like every sentinel it lives in
// pooler_dir; see sentinel.go.
func (pm *MultipoolerManager) rewindSentinel() sentinel {
	return pm.newSentinel(constants.RewindSentinelFile, "pg_rewind in progress\n")
}
