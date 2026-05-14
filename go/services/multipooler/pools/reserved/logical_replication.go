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

package reserved

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/services/multipooler/pools/regular"
)

// logicalReplicationClientConfig returns a copy of base with the `replication`
// startup parameter set to `database`, requesting a logical-replication-
// capable backend. The base config is not modified; the Parameters map is
// copied so callers can keep using base for non-replication connections.
func logicalReplicationClientConfig(base client.Config) client.Config {
	cfg := base
	cfg.Parameters = make(map[string]string, len(base.Parameters)+1)
	maps.Copy(cfg.Parameters, base.Parameters)
	cfg.Parameters["replication"] = "database"
	return cfg
}

// NewLogicalReplicationConn opens a Postgres connection with `replication=database`
// in its startup parameters and tags it with ReasonLogicalReplication so the
// reservation bitmask treats it as a session-pinned connection.
//
// The connection consumes one slot in the underlying regular pool, sharing the
// per-user cap (and block-on-full, waiter metrics, and demand tracking) with
// transactional reserved conns. Because `replication=database` is a startup-
// time parameter that cannot be flipped on an existing backend, we acquire a
// slot, discard the regular socket it gave us, and swap in a fresh replication-
// mode socket into the same Pooled wrapper. The session-pinned semantics still
// hold: the wrapper is tainted at release time so Recycle closes the socket
// instead of returning it to the idle list.
//
// The caller must call Release when the session ends. Every Release reason
// taints the wrapper (see Pool.release) before Recycle so the underlying
// socket is closed and the cap slot freed.
//
// Asymmetry with NewConn: this factory does not take a *connstate.Settings.
// The intended callers (replication-protocol streaming, slot management) issue
// walsender-protocol commands that ignore session settings. A
// `replication=database` walsender is still a real backend session, so plain
// SQL run over it would observe search_path / work_mem / etc.; callers needing
// those should apply them on the returned conn via Conn().Query(ctx, "SET ...")
// before the settings-sensitive SQL. If a caller materializes a real need for
// atomic acquire-with-settings, add an overload — don't bake it in
// speculatively.
func (p *Pool) NewLogicalReplicationConn(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("reserved pool is closed")
	}
	p.mu.Unlock()

	// Acquire a slot in the underlying connpool. This is the same path
	// transactional reserved conns take, so we share the per-user cap,
	// block-and-wait behavior, and demand/wait metrics.
	pooled, err := p.conns.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire reserved slot for replication conn: %w", err)
	}

	// Discard the pooled socket. Replication mode is set in the startup
	// packet and cannot be turned on for an existing backend, so we swap a
	// fresh replication-mode socket into the same Pooled wrapper to keep
	// the slot accounted for.
	pooled.Conn.Close()

	cfg := logicalReplicationClientConfig(*p.config.RegularPoolConfig.ClientConfig)
	clientConn, err := client.Connect(ctx, p.ctx, &cfg)
	if err != nil {
		pooled.Taint()
		pooled.Recycle()
		return nil, fmt.Errorf("dial replication connection: %w", err)
	}
	pooled.Conn = regular.NewConn(clientConn, p.config.RegularPoolConfig.AdminPool)

	// IMPORTANT: do NOT call pooled.Taint() here. Taint immediately frees
	// the slot via p.pool.put(nil) — see connpool/pooled.go. The slot must
	// stay held for the session's lifetime, so we leave pool intact and
	// let reserved.Pool.release() Taint right before Recycle.

	connID := p.lastID.Add(1)
	c := newConn(pooled, connID, p)
	c.AddReservationReason(protoutil.ReasonLogicalReplication)

	// Deliberately leave inactivityTimeout = 0 so the reserved pool's idleKiller
	// never evicts this connection. Idle teardown for replication-mode sessions
	// is Postgres' job via wal_sender_timeout (default 60s) — the walsender
	// backend kills the stream itself, and the resulting socket error tears the
	// connection down here. A second multipooler-side timer would only race.

	p.mu.Lock()
	if p.closed {
		// Pool closed between our earlier check and registration. Tear down
		// the freshly opened socket rather than orphaning it in a closed pool.
		p.mu.Unlock()
		pooled.Taint()
		pooled.Recycle()
		return nil, errors.New("reserved pool is closed")
	}
	p.active[connID] = c
	p.mu.Unlock()

	p.reserveCount.Add(1)
	if p.config.OnReserve != nil {
		p.config.OnReserve()
	}
	p.logger.DebugContext(ctx, "logical replication connection created",
		"conn_id", connID,
		"process_id", c.ProcessID())
	return c, nil
}
