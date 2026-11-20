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

package multipooler

import (
	"context"
	"fmt"
	"slices"

	"github.com/multigres/multigres/go/common/backingconn"
)

// checkBackingDatabase refuses a backing connection that serves another database than the
// pooler's own: the pooler advertises --database to the cluster and dials the connection's.
func checkBackingDatabase(conn *backingconn.Conn, database string) error {
	if conn.Database != database {
		return fmt.Errorf("backing connection %q serves database %q, which does not match --database %q", conn.Name, conn.Database, database)
	}
	return nil
}

// connectionSource says how a shard (tablegroup id, shard name) is served: by an external
// PostgreSQL's connection, or managed.
type connectionSource interface {
	// ShardBinding returns the connection of the shard, or managed when the shard has none. A
	// source that says neither has said nothing: it is never taken for managed.
	ShardBinding(ctx context.Context, tablegroupID int64, shard string) (conn *backingconn.Conn, managed bool, err error)
}

// fetchBinding asks a source how a shard is served and refuses an answer that says nothing, both,
// or a connection a pooler cannot front (Conn.ForPooler).
func fetchBinding(ctx context.Context, source connectionSource, tablegroupID int64, shard string) (conn *backingconn.Conn, managed bool, err error) {
	conn, managed, err = source.ShardBinding(ctx, tablegroupID, shard)
	switch {
	case err != nil:
		return nil, false, err
	case managed && conn != nil:
		return nil, false, fmt.Errorf("invalid backing connection: shard %s of tablegroup %d: the source returned a connection for a managed shard", shard, tablegroupID)
	case managed:
		return nil, true, nil
	case conn == nil:
		return nil, false, fmt.Errorf("invalid backing connection: shard %s of tablegroup %d: the source said neither that it is managed nor which connection serves it", shard, tablegroupID)
	}
	if err := conn.ForPooler(); err != nil {
		return nil, false, fmt.Errorf("invalid backing connection %q: %w", conn.Name, err)
	}
	return conn, false, nil
}

// The flags of a postgres managed through pgctld, all refused by an unmanaged pooler
// (validateBackendFlags). A managed pooler adopts the first two groups from pgctld's Status
// unless they are set (adoptPgctldValues).
var (
	// pgctldRequiredFlags are the identity of the postgres pgctld runs: Init fails when pgctld
	// is unreachable and these are not set.
	pgctldRequiredFlags = []string{"pg-port", "pooler-dir"}
	// pgctldOptionalFlags have workable defaults: Init starts degraded when pgctld is
	// unreachable and these are not set.
	pgctldOptionalFlags = []string{"pgbackrest-port", "pgbackrest-cert-file", "pgbackrest-key-file", "pgbackrest-ca-file"}
	// managedOnlyFlags are never adopted from pgctld and still mean nothing without it.
	managedOnlyFlags = []string{"socket-file", "pgctld-addr", "pgbackrest-cipher-key-file"}
)

// managedPostgresFlags returns every flag that belongs to pgctld or to a postgres it manages.
func managedPostgresFlags() []string {
	return slices.Concat(pgctldRequiredFlags, pgctldOptionalFlags, managedOnlyFlags)
}

// validateBackendFlags refuses the flags of a pgctld-managed postgres on an unmanaged pooler
// rather than ignoring them: its endpoint comes only from the catalog.
func validateBackendFlags(unmanaged bool, explicit func(name string) bool) error {
	if !unmanaged {
		return nil
	}
	for _, name := range managedPostgresFlags() {
		if explicit(name) {
			return fmt.Errorf("--%s configures a pgctld-managed postgres, which an unmanaged pooler does not have", name)
		}
	}
	return nil
}
