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

package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// connectionStore reads the connection directory from the sharding catalog on
// the default primary, through the admin pool: the named connections of
// multigres.connections, read by the same store the migrator uses.
type connectionStore struct {
	queryService func() executor.InternalQueryService
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{queryService: pm.internalQueryService}
}

// BackingConnection reads the named connection and returns it as the structured connection the
// contract carries. The catalog stores it structured (the conninfo column, validated when the
// connection was created), and that is served as it is; the dsn column is never read. A connection
// with no conninfo cannot be read, and one that a pooler cannot front, one that does not name
// exactly one server for example, is refused.
//
// A failure to read the row is UNAVAILABLE when the query service is missing or the connection
// to the database is lost, which the caller retries; a catalog without the connections table is
// FAILED_PRECONDITION, a fact about where the call was made; so is a stored connection that
// cannot be read; anything else is passed on.
func (s *connectionStore) BackingConnection(ctx context.Context, name string) (*multipoolerservicepb.BackingConnection, bool, error) {
	qs := s.queryService()
	if qs == nil {
		return nil, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service not available")
	}
	stored, err := migration.NewStore(qs).GetConnectionByRef(ctx, migration.Ref{Name: name})
	switch {
	case errors.Is(err, migration.ErrConnectionNotFound):
		return nil, false, nil
	case mterrors.IsConnectionError(err):
		return nil, false, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "read connection %q: %v", name, err)
	case errors.Is(err, migration.ErrNoConnectionsTable):
		// Not the catalog shard, or the catalog's table is not created yet: a fact about the
		// state this call expects, as for the migrator's own connection RPCs.
		return nil, false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, migration.NoConnectionsTableMessage)
	case errors.Is(err, migration.ErrConnectionUnreadable):
		return nil, false, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	case err != nil:
		return nil, false, err
	}
	reply, err := backingFromStored(stored)
	if err != nil {
		return nil, false, mterrors.Wrap(err, fmt.Sprintf("connection %q", name))
	}
	return reply, true, nil
}

// errUnusableConnection is a stored connection that cannot be a backing connection. It is a fact
// about the persisted connection, not a transient failure, so it is FAILED_PRECONDITION. The
// message never carries a value of the connection, which holds the password: the errors of
// backingconn name at most a libpq parameter, and the rest are fixed text.
func errUnusableConnection(format string, args ...any) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "backing connection is not usable: "+format, args...)
}

// backingFromStored returns a stored connection as the reply, refused unless a pooler can front it
// (Conn.ForPooler), so that the default primary never serves a connection that the pooler refuses.
func backingFromStored(c *migration.Connection) (*multipoolerservicepb.BackingConnection, error) {
	conn, err := c.Conn()
	if err != nil {
		return nil, errUnusableConnection("%s", err)
	}
	return backingReply(conn)
}

// backingReply returns a connection as the reply, refused unless a pooler can front it.
func backingReply(conn *backingconn.Conn) (*multipoolerservicepb.BackingConnection, error) {
	if err := conn.ForPooler(); err != nil {
		return nil, errUnusableConnection("%s", err)
	}
	return conn.ToProto(), nil
}

// backingFromColumns returns the connection of a row of multigres.connections, read by its
// columns, as the reply; a refusal names the connection.
func backingFromColumns(name, conninfo string) (*multipoolerservicepb.BackingConnection, error) {
	conn, err := migration.ConnFromColumns(name, conninfo)
	switch {
	case errors.Is(err, migration.ErrConnectionUnreadable):
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	case err != nil:
		return nil, mterrors.Wrap(errUnusableConnection("%s", err), fmt.Sprintf("connection %q", name))
	}
	reply, err := backingReply(conn)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("connection %q", name))
	}
	return reply, nil
}
