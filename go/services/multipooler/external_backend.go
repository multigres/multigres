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

// connectionSource tells a pooler how its tablegroup is served, by the tablegroup's catalog id:
// by the external PostgreSQL whose connection it returns, or by managed shards. The pooler's own
// source asks the default primary of its database.
type connectionSource interface {
	// TableGroupBinding returns the connection of the tablegroup, or managed when the tablegroup
	// has none. A source that says neither has said nothing: it is never taken for managed.
	TableGroupBinding(ctx context.Context, tablegroupID int64) (conn *backingconn.Conn, managed bool, err error)
}

// fetchBinding asks a source how a tablegroup is served and returns the answer only if it is
// usable: whoever the source is, an empty answer, or a connection a pooler cannot front
// (Conn.ForPooler), is refused.
func fetchBinding(ctx context.Context, source connectionSource, tablegroupID int64) (conn *backingconn.Conn, managed bool, err error) {
	conn, managed, err = source.TableGroupBinding(ctx, tablegroupID)
	switch {
	case err != nil:
		return nil, false, err
	case managed && conn != nil:
		return nil, false, fmt.Errorf("invalid backing connection: tablegroup %d: the source returned a connection for a managed tablegroup", tablegroupID)
	case managed:
		return nil, true, nil
	case conn == nil:
		return nil, false, fmt.Errorf("invalid backing connection: tablegroup %d: the source said neither that it is managed nor which connection serves it", tablegroupID)
	}
	if err := conn.ForPooler(); err != nil {
		return nil, false, fmt.Errorf("invalid backing connection: backing connection %q: %w", conn.Name, err)
	}
	return conn, false, nil
}

// The flags of a postgres managed through pgctld. A managed pooler adopts the first two groups
// from pgctld's Status unless they are set (adoptPgctldValues); an unmanaged pooler has no pgctld,
// pgBackRest or data directory and refuses all of them (validateBackendFlags).
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
