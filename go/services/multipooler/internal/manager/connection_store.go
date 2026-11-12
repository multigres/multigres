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

// BackingConnection reads the named connection and decodes its stored form, a
// libpq connection string in the dsn column (a postgres:// URI or keyword=value
// pairs, the form the migrator stores), into the structured connection the
// contract carries. A dsn that does not name exactly one server is refused.
//
// A failure to read the row is UNAVAILABLE when the query service is missing or the connection
// to the database is lost, which the caller retries; a catalog without the connections table is
// FAILED_PRECONDITION, a fact about where the call was made; anything else is passed on.
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
	case err != nil:
		return nil, false, err
	}
	conn, err := parseBackingDSN(stored.DSN)
	if err != nil {
		return nil, false, mterrors.Wrap(err, fmt.Sprintf("connection %q", name))
	}
	conn.Name = name
	return conn, true, nil
}

// errInvalidBackingDSN is a stored dsn that cannot name the one server a backing connection
// must. It is a fact about the persisted connection, not a transient failure, so it is
// FAILED_PRECONDITION. The message never carries a value of the dsn, which holds the password:
// the errors of backingconn.ParseDSN name at most a libpq parameter, and the rest are fixed text.
func errInvalidBackingDSN(format string, args ...any) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "backing connection dsn is not usable: "+format, args...)
}

// parseBackingDSN decodes a stored libpq connection string, in either form, into the connection
// a pooler fronts: the strict parser of backingconn (see backingconn.ParseDSN for what it refuses
// and why), the defaults of Conn.Complete, and the rules a pooler applies to the connection it
// receives (Conn.Validate, Conn.ForPooler), so that the default primary never serves a connection
// that the pooler refuses.
func parseBackingDSN(raw string) (*multipoolerservicepb.BackingConnection, error) {
	conn, err := backingconn.FromDSN(raw)
	if err == nil {
		err = conn.ForPooler()
	}
	if err != nil {
		return nil, errInvalidBackingDSN("%s", err)
	}
	return conn.ToProto(), nil
}
