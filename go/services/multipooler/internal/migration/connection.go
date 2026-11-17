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

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/backingconn"
)

// Connection is a stored, named source conninfo: a row of
// multigres.connections, the table the migrator shares with the other users of
// named connections. The table lives on the catalog shard only (see
// constants.IsCatalogShard), so migrations, which resolve their source from
// it, run on the catalog shard; elsewhere a connection query reports
// ErrNoConnectionsTable. A migration references one live by id, not a
// one-time copy, so replacing a Connection takes effect on every migration
// that references it on its very next action.
type Connection struct {
	ID   int64
	Name string

	// DSN is the connection's canonical libpq rendering, password included, for display. Nothing
	// dials it.
	DSN string

	// ConnInfo is the connection, validated and made self-contained when it was created, and the
	// source of truth: every consumer reads it and renders what its client is dialed with from it.
	// It is nil only when it could not be decoded (see Unreadable).
	ConnInfo *backingconn.Conn

	// OwnerRole is the PostgreSQL role the gateway's SCRAM-authenticated SQL
	// client was connected as when this connection was created. Empty when
	// created through the CLI/multiadmin path, which has no SQL-authenticated
	// identity to stamp it from — CreateMigration's ownership check is skipped
	// in that case, the same empty-callerRole convention CheckDropPrivilege
	// already uses. See CreateMigration's doc comment: the gateway's
	// CanCreateMigration gate only confirms a caller could perform
	// logical-replication setup in principle, not that it may use this
	// specific connection's credentials.
	OwnerRole string

	CreatedAt time.Time

	// connInfoErr is why the stored conninfo could not be decoded (ErrConnectionUnreadable). A
	// read does not fail on it: a caller that may not see the connection must get the same answer
	// as for one that does not exist, so whoever uses its content asks Unreadable once the
	// caller's right to it is settled.
	connInfoErr error
}

// Unreadable is nil for a connection whose stored conninfo was read, and otherwise an
// ErrConnectionUnreadable naming the connection.
func (c *Connection) Unreadable() error { return c.connInfoErr }

// Conn returns the connection, valid and self-contained (backingconn.Conn.Validate), as the
// caller's own copy with the connection's name set. One that is not valid is refused with
// ErrInvalidConnection, naming the connection; an unreadable one is an error (see Unreadable).
func (c *Connection) Conn() (*backingconn.Conn, error) {
	if c.connInfoErr != nil {
		return nil, c.connInfoErr
	}
	return validConn(c.Name, c.ConnInfo)
}

// validConn is a copy of conninfo, validated, with the name of its connection.
func validConn(name string, conninfo *backingconn.Conn) (*backingconn.Conn, error) {
	conn := conninfo.Clone()
	if err := conn.Validate(); err != nil {
		return nil, fmt.Errorf("%w: connection %q: %w", ErrInvalidConnection, name, err)
	}
	conn.Name = name
	return conn, nil
}

// Rendering is the connection's canonical libpq rendering, read from its conninfo and not from
// the dsn column: it is what a caller is shown. It is empty for a connection that cannot be read.
func (c *Connection) Rendering() string {
	conn, err := c.Conn()
	if err != nil {
		return ""
	}
	return conn.DSN()
}

// decodeConnInfo reads the text of the conninfo column, which is empty for a NULL. It does not
// validate the connection: a caller does that with the rules it applies (Conn.Validate,
// Conn.ForPooler). One this version cannot read (a field of a later version, say) is refused with
// ErrConnectionUnreadable, not read as less than it says; so is a connection with no conninfo,
// which setup could not fill from its dsn (the catalog's fillConnInfo).
func decodeConnInfo(name, conninfo string) (*backingconn.Conn, error) {
	if conninfo == "" {
		return nil, fmt.Errorf("connection %q: %w: it has no conninfo; drop it and create it again", name, ErrConnectionUnreadable)
	}
	conn := &backingconn.Conn{}
	if err := json.Unmarshal([]byte(conninfo), conn); err != nil {
		return nil, fmt.Errorf("connection %q: %w: %w", name, ErrConnectionUnreadable, err)
	}
	return conn, nil
}
