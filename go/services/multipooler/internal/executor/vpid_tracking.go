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

	"github.com/multigres/multigres/go/common/multigresschema"
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
// Rows are deliberately not deleted when a connection returns to the pool:
// the next checkout overwrites the row for that backend pid before any query
// runs, and a leftover row pointing at an idle pooled backend contributes no
// blockers to the aggregated probe. Dead backends are filtered out by the
// shim's join against pg_stat_activity.

// ensureVpidTable creates the multigres schema and backend_vpid table once
// per executor, using the supplied connection. Best-effort: on a standby the
// DDL fails (read-only) and the error is only logged — standbys receive no
// vpid-stamped traffic, and a primary that was reinitialized underneath a
// running pooler is healed by the retry inside trackVpidOnRegular.
func (e *Executor) ensureVpidTable(ctx context.Context, conn *regular.Conn) {
	e.vpidTableEnsure.Do(func() {
		if _, err := conn.Query(ctx, multigresschema.BackendVpidDDL); err != nil {
			e.logger.WarnContext(ctx, "failed to create multigres.backend_vpid; vpid tracking degraded", "error", err)
		}
	})
}

// trackVpidOnRegular upserts the (backend_pid → vpid) mapping row for the
// given connection's backend. Must only be called while the connection is in
// autocommit (fresh checkout / pre-BEGIN) — see the package comment above.
// Best-effort: a failure does not block the actual query; only lock
// detection through the proxy depends on the mapping. No-op when vpid
// tracking is disabled or the caller has no client connection id.
func (e *Executor) trackVpidOnRegular(ctx context.Context, conn *regular.Conn, options *query.ExecuteOptions) {
	if !e.vpidStampEnabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	e.ensureVpidTable(ctx, conn)
	upsert := fmt.Sprintf(
		"INSERT INTO multigres.backend_vpid (backend_pid, vpid) VALUES (pg_backend_pid(), %d) "+
			"ON CONFLICT (backend_pid) DO UPDATE SET vpid = EXCLUDED.vpid, updated_at = now()",
		options.ClientConnectionId)
	if _, err := conn.Query(ctx, upsert); err == nil {
		return
	}
	// The table can vanish underneath a long-lived pooler (e.g. the test
	// harness drops and recreates the data directory between suites without
	// restarting the pooler). Re-run the DDL outside the sync.Once and retry
	// the upsert once; ignore the DDL error so a concurrent re-creation race
	// still lets the retry succeed.
	if _, err := conn.Query(ctx, multigresschema.BackendVpidDDL); err != nil {
		e.logger.DebugContext(ctx, "vpid table re-create failed", "error", err)
	}
	if _, err := conn.Query(ctx, upsert); err != nil {
		e.logger.DebugContext(ctx, "vpid mapping upsert failed", "error", err)
	}
}

// trackVpidOnReserved records the mapping for a freshly reserved backend.
// Must be called before the reservation's BEGIN so the row commits in
// autocommit and is visible to other sessions for the whole transaction.
func (e *Executor) trackVpidOnReserved(ctx context.Context, conn *reserved.Conn, options *query.ExecuteOptions) {
	if !e.vpidStampEnabled || options == nil || options.ClientConnectionId == 0 {
		return
	}
	e.trackVpidOnRegular(ctx, conn.Conn(), options)
}
