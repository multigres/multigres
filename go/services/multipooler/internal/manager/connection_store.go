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

// connectionStore reads how a shard is served from the sharding catalog on the default primary,
// through the admin pool.
type connectionStore struct {
	queryService func() executor.InternalQueryService
}

func newConnectionStore(pm *MultipoolerManager) *connectionStore {
	return &connectionStore{queryService: pm.internalQueryService}
}

// ShardBinding returns the connection the shard is bound to, as the reply. A managed shard has
// none; a shard of a tablegroup being dropped, and a retired shard, are served to no one. A read
// that fails for want of a query service or a database connection is UNAVAILABLE, which the
// caller retries; a stored connection that cannot be read or used is FAILED_PRECONDITION.
func (s *connectionStore) ShardBinding(ctx context.Context, tablegroupID int64, shard string) (*multipoolerservicepb.BackingConnection, bool, error) {
	qs := s.queryService()
	if qs == nil {
		return nil, false, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "internal query service not available")
	}
	res, err := qs.QueryAdminArgs(ctx,
		"SELECT t.state, s.state, coalesce(c.name, ''), coalesce(c.conninfo::text, '') FROM multigres.shard s "+
			"JOIN multigres.tablegroup t ON t.tablegroup_id = s.tablegroup_id "+
			"LEFT JOIN multigres.connections c ON c.connection_id = s.backing_connection_id "+
			"WHERE s.tablegroup_id = $1 AND s.shard_name = $2", tablegroupID, shard)
	switch {
	case mterrors.IsConnectionError(err):
		return nil, false, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "read shard %s of tablegroup %d: %v", shard, tablegroupID, err)
	case err != nil:
		return nil, false, err
	}
	if len(res.Rows) == 0 {
		return nil, false, nil
	}
	var tableGroupState, shardState, name, conninfo string
	if err := executor.ScanRow(res.Rows[0], &tableGroupState, &shardState, &name, &conninfo); err != nil {
		return nil, false, mterrors.Wrap(err, "read shard row")
	}
	if tableGroupState == tableGroupStateDropping {
		return nil, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "tablegroup %d is being dropped", tablegroupID)
	}
	if shardState == shardStateRetired {
		return nil, false, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "shard %s of tablegroup %d is retired", shard, tablegroupID)
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

// errUnusableConnection is a stored connection that cannot be a backing connection, which is not
// transient, so FAILED_PRECONDITION. The message never carries a value of the connection, which
// holds the password: backingconn's errors name at most a libpq parameter.
func errUnusableConnection(format string, args ...any) error {
	return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION, "backing connection is not usable: "+format, args...)
}

// backingFromColumns returns the connection of a multigres.connections row as the reply, refused
// unless a pooler can front it (Conn.ForPooler), so the default primary never serves one the
// pooler refuses.
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
