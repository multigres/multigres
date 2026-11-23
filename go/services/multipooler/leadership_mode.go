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
	"errors"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/backingconn"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// leadership is how a pooler is managed, and, for an unmanaged pooler, the external PostgreSQL
// it fronts.
type leadership struct {
	mode clustermetadatapb.LeadershipMode
	// backing is set exactly when the pooler is unmanaged.
	backing *backingconn.Conn
}

// resolveLeadership decides how this pooler is managed, in the order documented in
// docs/sharding/tablegroup-operations.md. A catalog read that fails is retried until ctx ends:
// the pooler never starts managed for want of an answer.
func (mp *Multipooler) resolveLeadership(ctx context.Context, logger *slog.Logger, id *clustermetadatapb.ID) (leadership, error) {
	managed := leadership{mode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS}
	if mp.staticLeader {
		managed.mode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	}
	// Only a shard of a non-default tablegroup with a catalog id can be bound.
	cataloged := !mp.staticLeader && mp.tableGroup.Get() != constants.DefaultTableGroup && mp.tableGroupID.Get() > 0

	recorded, hasRecord := readRecordedLeadershipMode(ctx, mp.ts, id, logger)
	switch {
	case hasRecord && topoclient.IsManagedLeadershipMode(recorded):
		return managed, nil
	case !hasRecord && !cataloged:
		return managed, nil
	case hasRecord && !cataloged:
		return leadership{}, fmt.Errorf("pooler %s is registered in the topology as %s, which it can only start as with a shard in the catalog: "+
			"start it with --tablegroup-id of a non-default tablegroup, or under a new name, or delete its topology record first",
			topoclient.ComponentIDString(id), recorded)
	}

	backing, isManaged, err := mp.awaitBinding(ctx, logger)
	switch {
	case err != nil:
		return leadership{}, err
	case isManaged && hasRecord:
		return leadership{}, fmt.Errorf("pooler %s is registered in the topology as %s and cannot start as %s, which the catalog says shard %s of tablegroup %d is: "+
			"start it under a new name, or delete its topology record first",
			topoclient.ComponentIDString(id), recorded, managed.mode, mp.shard.Get(), mp.tableGroupID.Get())
	case isManaged:
		return managed, nil
	}
	return leadership{mode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, backing: backing}, nil
}

// readRecordedLeadershipMode returns the leadership mode of this pooler's topology record, if any.
// A failed read counts as no record: RegisterMultipooler refuses to change whether a record is
// managed (topoclient.ErrManagementChanged), so nothing rests on this read.
func readRecordedLeadershipMode(ctx context.Context, ts topoclient.Store, id *clustermetadatapb.ID, logger *slog.Logger) (clustermetadatapb.LeadershipMode, bool) {
	info, err := ts.GetMultipooler(ctx, id)
	switch {
	case err == nil:
		return info.GetLeadershipMode(), true
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}):
		return 0, false
	}
	logger.WarnContext(ctx, "could not read this pooler's topology record to learn whether it is unmanaged; going by its shard",
		"pooler_id", topoclient.ComponentIDString(id), "error", err)
	return 0, false
}

// bindingSource is the default primary of the pooler's database, dialed with the
// --multipooler-grpc-* client flags like every dial to a pooler, so a cluster whose poolers serve
// TLS sets them on an unmanaged pooler too.
func (mp *Multipooler) bindingSource(logger *slog.Logger) (connectionSource, error) {
	if mp.connections != nil {
		return mp.connections, nil
	}
	transport, err := mp.defaultPrimaryConn.TransportCredentials(logger)
	if err != nil {
		return nil, fmt.Errorf("default primary transport: %w", err)
	}
	return &defaultPrimarySource{ts: mp.ts, database: mp.database.Get(), transport: transport}, nil
}

// awaitBinding asks how the pooler's shard is served until it is answered.
func (mp *Multipooler) awaitBinding(ctx context.Context, logger *slog.Logger) (*backingconn.Conn, bool, error) {
	tablegroupID, shard := mp.tableGroupID.Get(), mp.shard.Get()
	source, err := mp.bindingSource(logger)
	if err != nil {
		return nil, false, err
	}
	var (
		conn    *backingconn.Conn
		managed bool
	)
	err = retryBootstrap(ctx, func(ctx context.Context) error {
		var err error
		conn, managed, err = fetchBinding(ctx, source, tablegroupID, shard)
		return err
	}, func(attempt int, err error) {
		level := slog.LevelDebug
		if attempt <= 3 || attempt%10 == 0 {
			level = slog.LevelWarn
		}
		logger.Log(ctx, level, "waiting for the catalog to say how this shard is served",
			"tablegroup_id", tablegroupID, "shard", shard, "attempt", attempt, "error", err)
	})
	if err != nil {
		return nil, false, fmt.Errorf("the catalog did not say how shard %s of tablegroup %d is served: %w", shard, tablegroupID, err)
	}
	return conn, managed, nil
}
