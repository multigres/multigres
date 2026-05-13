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
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
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
// The connection bypasses the regular pool entirely. A `replication=database`
// walsender is a real backend that accepts SQL as well as replication-protocol
// commands, so it *could* technically be returned to a pool — but it carries
// long-lived session state (an in-flight replication stream, an owned slot)
// that must stay pinned to this specific backend for the session's
// lifetime. Handing it to another caller mid-flight would lose that affinity.
// Per-session direct dial sidesteps the question. The returned *Conn is
// registered in p.active so Get(connID) still works.
//
// The caller must call Release when the session ends. Because the Pooled
// wrapper holds pool == nil, every Release reason closes the underlying
// socket — there is no recycle path for a replication-mode conn.
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

	cfg := logicalReplicationClientConfig(*p.config.RegularPoolConfig.ClientConfig)
	clientConn, err := client.Connect(ctx, p.ctx, &cfg)
	if err != nil {
		return nil, fmt.Errorf("dial replication connection: %w", err)
	}
	rc := regular.NewConn(clientConn, p.config.RegularPoolConfig.AdminPool)

	// pool == nil makes Recycle close the conn directly (see
	// connpool/pooled.go), which is what we want for a session-pinned conn
	// that cannot re-enter a pool.
	pooled := &connpool.Pooled[*regular.Conn]{Conn: rc}

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
		_ = clientConn.Close()
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
