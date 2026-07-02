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
// The mapping row is written through the multipooler's admin pool, using the
// backend pid learned from PostgreSQL's BackendKeyData during startup. Client
// roles only need SELECT on the table for lock-wait probes; they never receive
// INSERT/UPDATE/DELETE privileges and cannot corrupt the mapping. Admin-pool
// writes are independent of the borrowed backend's current role, transaction,
// and GUC state, so metadata is immediately visible to probing sessions and is
// not rolled back with client work. We still stamp only at hand-off points: a
// fresh regular-conn checkout, or a new reservation before its BEGIN runs.
// Mid-transaction re-stamping is unnecessary — a reserved backend stays bound
// to one gateway session for the life of the reservation, and the mapping
// lives outside PostgreSQL session GUC state, so RESET ALL / DISCARD ALL do not
// affect it.
//
// Rows are deleted at the release/recycle boundary so the table reflects only
// backends currently associated with a gateway connection. That keeps metadata
// consumers from having to distinguish live assignments from idle pooled
// backends. Readers join the table against pg_stat_activity on backend_pid, so
// a row orphaned by abrupt backend loss is ignored once the pid is gone and is
// overwritten (ON CONFLICT) the next time the pool hands that pid to a session.

const vpidCleanupTimeout = 250 * time.Millisecond

// trackVpidOnRegular upserts the (backend_pid → vpid) mapping row for the given
// connection's backend. Must only be called at a hand-off point (fresh checkout
// / pre-BEGIN) — see the package comment above.
//
// The write uses the admin pool and the backend pid already known from the
// PostgreSQL startup handshake, so client roles do not need DML privileges on
// the sidecar table.
//
// If the upsert fails, the query still runs; the affected behavior is lock-wait
// translation for this gateway/backend association. No-op when tracking is
// disabled, the caller has no client connection id, or this connection already
// recorded the same active association.
func (e *Executor) trackVpidOnRegular(ctx context.Context, conn *regular.Conn, options *query.ExecuteOptions) {
	if !e.backendVpidTrackingEnabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	state := conn.State()
	if state.TrackedVpid() == options.ClientConnectionId {
		return
	}
	if !conn.IsIdle() {
		e.logger.DebugContext(ctx, "skipping vpid mapping upsert because backend is not idle at handoff")
		return
	}

	if err := e.execBackendVpidMutation(ctx,
		"INSERT INTO multigres.backend_vpid (backend_pid, vpid) "+
			"VALUES ($1::int4, $2::int8) "+
			"ON CONFLICT (backend_pid) DO UPDATE SET vpid = EXCLUDED.vpid, updated_at = now()",
		conn.ProcessID(), options.ClientConnectionId); err != nil {
		e.logger.DebugContext(ctx, "vpid mapping upsert failed", "error", err)
		return
	}
	state.SetTrackedVpid(options.ClientConnectionId)
}

// trackVpidOnReserved records the mapping for a freshly reserved backend.
// Must be called before the reservation can be handed to another gateway
// session flow; the admin-pool write is immediately visible to probes and the
// row stays valid for the whole transaction.
func (e *Executor) trackVpidOnReserved(ctx context.Context, conn *reserved.Conn, options *query.ExecuteOptions) {
	if !e.backendVpidTrackingEnabled || options == nil || options.ClientConnectionId == 0 {
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
	if !e.backendVpidTrackingEnabled {
		return true
	}
	state := conn.State()
	if state.TrackedVpid() == 0 {
		return true
	}
	defer state.SetTrackedVpid(0)

	cleanupCtx, cancel := context.WithTimeout(ctx, vpidCleanupTimeout)
	defer cancel()

	if err := e.execBackendVpidMutation(cleanupCtx,
		"DELETE FROM multigres.backend_vpid WHERE backend_pid = $1::int4",
		conn.ProcessID()); err != nil {
		e.logger.DebugContext(cleanupCtx, "vpid mapping cleanup failed", "error", err)
		return false
	}
	if !conn.IsIdle() {
		e.logger.DebugContext(ctx, "vpid mapping cleanup succeeded, but backend is not idle")
		return false
	}
	return true
}

func (e *Executor) execBackendVpidMutation(ctx context.Context, sql string, args ...any) error {
	adminConn, err := e.poolManager.GetAdminConn(ctx)
	if err != nil {
		return err
	}
	defer adminConn.Recycle()

	_, err = adminConn.Conn.QueryArgsWithRetry(ctx, sql, args...)
	return err
}

func (e *Executor) recycleTrackedRegularConn(conn regular.PooledConn) {
	// Release cleanup uses its own bounded context so caller cancellation does not
	// leave active-association metadata behind or force unnecessary reconnects.
	if !e.clearVpidOnRegular(context.TODO(), conn.Conn) {
		conn.Conn.Close()
	}
	conn.Recycle()
}
