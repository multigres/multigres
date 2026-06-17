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

package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
)

// vpid tracking: multigres.backend_vpid maps a real PostgreSQL backend pid to
// the multigateway virtual pid (ClientConnectionId) it is currently serving.
// The isolation test harness shim (multigres_test_session_is_blocked) joins
// this table against pg_stat_activity to translate the virtual pids that
// isolationtester sees into real backend pids for pg_blocking_pids probes.
//
// The mapping row is written on the session's own backend connection, but
// only at hand-off points where the connection is guaranteed to be in
// autocommit (a fresh regular-conn checkout, or a new reservation before its
// BEGIN runs). That guarantee matters: a row written inside an open
// transaction would be invisible to the probing session until commit, and
// would entangle the metadata write with the client transaction (extra
// predicate locks under SERIALIZABLE, rollback on abort). Mid-transaction
// re-stamping is unnecessary — a reserved backend stays bound to one gateway
// session for the life of the reservation, and unlike the old
// application_name stamp the table row cannot be wiped by RESET ALL /
// DISCARD ALL, so the row written at reservation time stays valid.
//
// Rows are deleted at the release/recycle boundary so the table reflects only
// backends currently associated with a gateway connection. That keeps metadata
// consumers from having to distinguish live assignments from idle pooled
// backends. The table also stores pg_stat_activity.backend_start and readers
// join on (pid, backend_start), so an orphaned row left behind by abrupt
// backend loss cannot match a future backend that reuses the same pid.
//
// Cost on the query path: when tracking is enabled (the default), each regular
// pooled query records an active association before user SQL and clears it
// before recycling the backend. The --backend-vpid-tracking-enabled flag is an
// emergency opt-out, not the default operating mode.

const vpidCleanupTimeout = 250 * time.Millisecond

// trackVpidOnRegular upserts the (backend_pid/backend_start → vpid) mapping
// row for the given connection's backend. Must only be called while the
// connection is in autocommit (fresh checkout / pre-BEGIN) — see the package
// comment above.
// Best-effort: a failure does not block the actual query; only lock detection
// through the proxy depends on the mapping. No-op when tracking is disabled,
// the caller has no client connection id, or this connection already recorded
// the same active association.
func (e *Executor) trackVpidOnRegular(ctx context.Context, conn *regular.Conn, options *query.ExecuteOptions) {
	if e.backendVpidTrackingDisabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	state := conn.State()
	if state.TrackedVpid() == options.ClientConnectionId {
		return
	}
	if !conn.IsIdle() {
		e.logger.DebugContext(ctx, "skipping vpid mapping upsert outside autocommit")
		return
	}

	upsert := fmt.Sprintf(
		"INSERT INTO multigres.backend_vpid (backend_pid, backend_start, vpid) "+
			"SELECT pg_backend_pid(), backend_start, %d FROM pg_stat_activity WHERE pid = pg_backend_pid() "+
			"ON CONFLICT (backend_pid) DO UPDATE SET backend_start = EXCLUDED.backend_start, vpid = EXCLUDED.vpid, updated_at = now()",
		options.ClientConnectionId)
	if _, err := conn.Query(ctx, upsert); err != nil {
		e.logger.DebugContext(ctx, "vpid mapping upsert failed", "error", err)
		return
	}
	state.SetTrackedVpid(options.ClientConnectionId)
}

// trackVpidOnReserved records the mapping for a freshly reserved backend.
// Must be called before the reservation's BEGIN so the row commits in
// autocommit and is visible to other sessions for the whole transaction.
func (e *Executor) trackVpidOnReserved(ctx context.Context, conn *reserved.Conn, options *query.ExecuteOptions) {
	if e.backendVpidTrackingDisabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	e.trackVpidOnRegular(ctx, conn.Conn(), options)
}

// clearVpidOnRegular removes this backend's active vpid mapping before the
// connection stops being associated with the gateway session that borrowed it.
// It must run in autocommit: if the backend is not idle, or the cleanup query
// fails/times out, the caller must not recycle the backend as clean.
func (e *Executor) clearVpidOnRegular(ctx context.Context, conn *regular.Conn) bool {
	if e.backendVpidTrackingDisabled {
		return true
	}
	state := conn.State()
	if state.TrackedVpid() == 0 {
		return true
	}
	defer state.SetTrackedVpid(0)

	if !conn.IsIdle() {
		e.logger.DebugContext(ctx, "skipping vpid mapping cleanup outside autocommit")
		return false
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, vpidCleanupTimeout)
	defer cancel()

	if _, err := conn.Query(cleanupCtx, "DELETE FROM multigres.backend_vpid WHERE backend_pid = pg_backend_pid()"); err != nil {
		e.logger.DebugContext(cleanupCtx, "vpid mapping cleanup failed", "error", err)
		return false
	}
	return true
}

func (e *Executor) recycleTrackedRegularConn(ctx context.Context, conn regular.PooledConn) {
	if !e.clearVpidOnRegular(ctx, conn.Conn) {
		conn.Conn.Close()
	}
	conn.Recycle()
}
