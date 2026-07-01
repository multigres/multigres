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
// session for the life of the reservation, and the mapping lives outside
// PostgreSQL session GUC state, so RESET ALL / DISCARD ALL do not affect it.
//
// Rows are deleted at the release/recycle boundary so the table reflects only
// backends currently associated with a gateway connection. That keeps metadata
// consumers from having to distinguish live assignments from idle pooled
// backends. Readers join the table against pg_stat_activity on backend_pid, so
// a row orphaned by abrupt backend loss is ignored once the pid is gone and is
// overwritten (ON CONFLICT) the next time the pool hands that pid to a session.

const vpidCleanupTimeout = 250 * time.Millisecond

// trackVpidOnRegular upserts the (backend_pid → vpid) mapping row for the given
// connection's backend. Must only be called while the connection is in
// autocommit (fresh checkout / pre-BEGIN) — see the package comment above.
//
// The write deliberately uses only pg_backend_pid() and a literal vpid: it
// reads no catalog/stats views, so it cannot be perturbed by the session's
// current role (e.g. a client SET ROLE to an unprivileged role).
//
// If the upsert fails, the query still runs; the affected behavior is lock-wait
// translation for this gateway/backend association. No-op when the caller has
// no client connection id, or this connection already recorded the same active
// association.
func (e *Executor) trackVpidOnRegular(ctx context.Context, conn *regular.Conn, options *query.ExecuteOptions) {
	if options == nil || options.ClientConnectionId == 0 {
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
		"INSERT INTO multigres.backend_vpid (backend_pid, vpid) "+
			"VALUES (pg_backend_pid(), %d) "+
			"ON CONFLICT (backend_pid) DO UPDATE SET vpid = EXCLUDED.vpid, updated_at = now()",
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
	if options == nil || options.ClientConnectionId == 0 {
		return
	}
	e.trackVpidOnRegular(ctx, conn.Conn(), options)
}

// clearVpidOnRegular removes this backend's active vpid mapping before the
// connection stops being associated with the gateway session that borrowed it.
// Returns true on the normal path (mapping cleared, or nothing to clear) so the
// caller recycles the backend.
//
// It returns false only when the backend cannot be confidently returned clean:
// the conn is not idle (a leaked open transaction would hand a dirty backend to
// the next session), or the cleanup DELETE failed/timed out. The caller then
// closes the backend, which also makes any surviving mapping row disappear from
// pg_stat_activity joins instead of remaining visible for a reused pid.
func (e *Executor) clearVpidOnRegular(ctx context.Context, conn *regular.Conn) bool {
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

func (e *Executor) recycleTrackedRegularConn(conn regular.PooledConn) {
	// Release cleanup uses its own bounded context so caller cancellation does not
	// leave active-association metadata behind or force unnecessary reconnects.
	if !e.clearVpidOnRegular(context.TODO(), conn.Conn) {
		conn.Conn.Close()
	}
	conn.Recycle()
}
