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
	"fmt"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerservicepb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// connectionStore reads how a tablegroup is served from the sharding catalog on the default
// primary, through the admin pool: the tablegroup row, and the named connection of
// multigres.connections it is bound to, the table the migrator shares.
type connectionStore struct {
	queryService func() executor.InternalQueryService
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{queryService: pm.internalQueryService}
}

// TableGroupBinding reads the tablegroup's row and the connection it is bound to, and returns that
// connection as the structured connection the contract carries (backingFromColumns). A managed
// tablegroup has no connection, and one being dropped is served to no one.
//
// A failure to read is UNAVAILABLE when the query service is missing or the connection to the
// database is lost, which the caller retries; a stored connection that cannot be read is
// FAILED_PRECONDITION; anything else is passed on.
func (s *connectionStore) TableGroupBinding(ctx context.Context, tablegroupID int64) (*multipoolerservicepb.BackingConnection, bool, error) {
	qs := s.queryService()
	if qs == nil {
		return nil, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service not available")
	}
	res, err := qs.QueryAdminArgs(ctx,
		"SELECT t.state, coalesce(c.name, ''), coalesce(c.conninfo::text, '') FROM multigres.tablegroup t "+
			"LEFT JOIN multigres.connections c ON c.connection_id = t.backing_connection_id WHERE t.tablegroup_id = $1", tablegroupID)
	switch {
	case mterrors.IsConnectionError(err):
		return nil, false, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "read tablegroup %d: %v", tablegroupID, err)
	case err != nil:
		return nil, false, err
	}
	if len(res.Rows) == 0 {
		return nil, false, nil
	}
	var state, name, conninfo string
	if err := executor.ScanRow(res.Rows[0], &state, &name, &conninfo); err != nil {
		return nil, false, mterrors.Wrap(err, "read tablegroup row")
	}
	if state == tableGroupStateDropping {
		return nil, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %d is being dropped", tablegroupID)
	}
	if name == "" { // the left join found no connection: a connection always has a name
		return nil, true, nil
	}
	reply, err := backingFromColumns(name, conninfo)
	if err != nil {
		return nil, false, err
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

// backingFromColumns returns the connection of a row of multigres.connections as the reply, or an
// error naming it unless a pooler can front it (Conn.ForPooler, which validates it): the default
// primary never serves a connection that the pooler refuses.
func backingFromColumns(name, conninfo string) (*multipoolerservicepb.BackingConnection, error) {
	conn, err := migration.DecodeConnInfo(name, conninfo)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, err.Error())
	}
	conn.Name = name
	if err := conn.ForPooler(); err != nil {
		return nil, mterrors.Wrap(errUnusableConnection("%s", err), fmt.Sprintf("connection %q", name))
	}
	return conn.ToProto(), nil
}
