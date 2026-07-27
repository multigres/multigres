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

package shardsetup

import (
	"bufio"
	"context"
	"os/user"
	"runtime"
	"strings"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// reclaimOrphanedSharedMemory removes leaked System V shared-memory segments
// owned by the current user that have no attached process (NATTCH == 0).
//
// PostgreSQL allocates a small, keyed SysV segment per instance (a
// postmaster-liveness interlock) and only releases it on a clean shutdown.
// Tests that SIGKILL postgres (KillPostgres) or crash it leave that segment
// orphaned. On a long-lived developer workstation these accumulate across runs
// because each run uses fresh ports (fresh keys), so old orphans are never
// reused. macOS caps SysV segments at 32 (kern.sysv.shmmni), so exhaustion is
// quick and breaks later postgres starts with shmget "No space left on device";
// Linux's default cap is far higher (4096) but the machine is equally
// long-lived, so we sweep there too. Other platforms (and any host without
// ipcs) are a no-op. CI is unaffected regardless: its runners are ephemeral, so
// nothing accumulates, and reaping already-dead segments there is harmless.
//
// Only segments with NATTCH == 0 are removed, so a live postgres (the
// developer's own server or a concurrently running test's poolers) is never
// affected: an attached postmaster always keeps NATTCH > 0. IPC_PRIVATE
// segments (key 0x00000000, which other software uses and postgres never does)
// are skipped as an extra guard.
func reclaimOrphanedSharedMemory(logf func(string, ...any)) {
	if !shmSweepSupported() {
		return
	}

	me, err := user.Current()
	if err != nil {
		return // best effort
	}

	ids := orphanedShmSegments(me.Username)
	if len(ids) == 0 {
		return
	}

	removed := 0
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := executil.Command(ctx, "ipcrm", "-m", id).Run()
		cancel()
		if err == nil {
			removed++
		}
	}
	if removed > 0 {
		logf("reclaimed %d orphaned SysV shared-memory segment(s)", removed)
	}
}

// shmSweepSupported reports whether we know how to parse this host's ipcs output.
func shmSweepSupported() bool {
	return runtime.GOOS == "darwin" || runtime.GOOS == "linux"
}

// orphanedShmSegments returns the shmids of SysV shared-memory segments owned by
// owner that currently have no attached process (NATTCH == 0) and are not
// IPC_PRIVATE (key 0x00000000).
//
// The `ipcs` column layout differs by platform, so parse each explicitly.
//
// macOS (`ipcs -mo`) — a leading type column ("m"), NATTCH is the last field:
//
//	T     ID     KEY        MODE       OWNER    GROUP NATTCH
//	m 262144 0x00145a28 --rw-------     mats    staff      0
//
// Linux (`ipcs -m`) — no type column, NATTCH is the 6th field, an optional
// status column may follow; data rows start with a hex key:
//
//	------ Shared Memory Segments --------
//	key        shmid   owner  perms   bytes   nattch  status
//	0x0052e2c1 32768   mats   600     524288  0
func orphanedShmSegments(owner string) []string {
	var ipcsArgs []string
	switch runtime.GOOS {
	case "darwin":
		ipcsArgs = []string{"-mo"}
	default:
		ipcsArgs = []string{"-m"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	out, err := executil.Command(ctx, "ipcs", ipcsArgs...).Output()
	if err != nil {
		return nil
	}
	return parseOrphanedShmIDs(string(out), runtime.GOOS, owner)
}

// parseOrphanedShmIDs extracts the shmids of segments owned by owner with
// nattch == 0 (and not IPC_PRIVATE) from `ipcs` output. Split out from the exec
// so both column layouts can be unit-tested without a live host. darwin selects
// the macOS `ipcs -mo` layout; otherwise the Linux `ipcs -m` layout is used.
func parseOrphanedShmIDs(out string, operatingSystem string, owner string) []string {
	var ids []string
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())

		var key, id, segOwner, nattch string
		switch operatingSystem {
		case "darwin":
			// m ID KEY MODE OWNER GROUP NATTCH
			if len(fields) < 7 || fields[0] != "m" {
				continue
			}
			key, id, segOwner, nattch = fields[2], fields[1], fields[4], fields[6]

		default:
			// KEY SHMID OWNER PERMS BYTES NATTCH [STATUS]. Data rows start with a
			// hex key, so this skips the "------" separator and column header.
			if len(fields) < 6 || !strings.HasPrefix(fields[0], "0x") {
				continue
			}
			key, id, segOwner, nattch = fields[0], fields[1], fields[2], fields[5]
		}

		if segOwner == owner && nattch == "0" && key != "0x00000000" {
			ids = append(ids, id)
		}
	}
	return ids
}
