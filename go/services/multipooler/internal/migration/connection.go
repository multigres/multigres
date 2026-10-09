// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import "time"

// Connection is a stored, named source conninfo. It is scoped to the shard
// whose Migrator created it — its row lives only in that shard's own
// multigres.migration_connection, so which shard it belongs to is implied by
// which Migrator you asked, not a field here. A migration created with a named
// connection (CreateParams.ConnectionName) references it live by id — not a
// one-time copy — so altering a Connection's DSN takes effect on every
// migration that references it on their very next action (see
// Coordinator.resolveSourceDSN).
type Connection struct {
	ID   int64
	Name string

	// DSN is the full libpq conninfo (may include TLS options), password
	// included. Every migration resolves its source DSN from here (see
	// Migration.ConnectionID), never storing one of its own. Stored in the
	// superuser-only sidecar schema, returned as-is on reads.
	DSN string

	CreatedAt time.Time
}

// CreateMigrationConnectionSQL is the DDL for the sidecar connection table. It
// is idempotent (IF NOT EXISTS) and must run before CreateMigrationSQL: the
// migration table's connection_id column carries a foreign key into this one.
// The table is created through the admin pool and gets no PUBLIC grant, same
// protection class as multigres.migration (connection rows carry a DSN).
const CreateMigrationConnectionSQL = `CREATE TABLE IF NOT EXISTS multigres.migration_connection (
	connection_id BIGINT PRIMARY KEY,
	name TEXT NOT NULL,
	dsn TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// MigrationConnectionNameUniqueIndexSQL enforces uniqueness of the connection
// name. Unlike MigrationNameUniqueIndexSQL, this is a plain (non-partial) unique
// index: a connection's name is always required, never NULL.
const MigrationConnectionNameUniqueIndexSQL = `CREATE UNIQUE INDEX IF NOT EXISTS migration_connection_name_key
	ON multigres.migration_connection (name)`
