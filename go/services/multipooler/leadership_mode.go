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

// resolveLeadership decides how this pooler is managed: from its own topology record, else from
// its tablegroup's catalog row, else (a static leader, the default tablegroup, no catalog id) as
// managed without a lookup; the order and its reasons are in docs/sharding/tablegroup-operations.md.
// A catalog read that fails is retried until ctx ends: the pooler never starts as a managed one
// for want of an answer.
func (mp *Multipooler) resolveLeadership(ctx context.Context, logger *slog.Logger, id *clustermetadatapb.ID, tableGroup string) (leadership, error) {
	managed := leadership{mode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_CONSENSUS}
	if mp.staticLeader {
		managed.mode = clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_STATIC
	}
	tablegroupID := mp.tableGroupID.Get()
	// Only a cataloged tablegroup can be bound, and the default one never is.
	cataloged := !mp.staticLeader && tableGroup != constants.DefaultTableGroup && tablegroupID > 0

	recorded, hasRecord := readRecordedLeadershipMode(ctx, mp.ts, id, logger)
	switch {
	case hasRecord && topoclient.IsManagedLeadershipMode(recorded):
		return managed, nil
	case !hasRecord && !cataloged:
		return managed, nil
	case hasRecord && !cataloged:
		return leadership{}, fmt.Errorf("pooler %s is registered in the topology as %s, which it can only start as with a tablegroup in the catalog: "+
			"start it with --tablegroup-id of a non-default tablegroup, or under a new name, or delete its topology record first",
			topoclient.ComponentIDString(id), recorded)
	}

	backing, isManaged, err := mp.awaitBinding(ctx, logger, tablegroupID)
	switch {
	case err != nil:
		return leadership{}, err
	case isManaged && hasRecord:
		return leadership{}, fmt.Errorf("pooler %s is registered in the topology as %s and cannot start as %s, which the catalog says tablegroup %d is: "+
			"start it under a new name, or delete its topology record first",
			topoclient.ComponentIDString(id), recorded, managed.mode, tablegroupID)
	case isManaged:
		return managed, nil
	}
	return leadership{mode: clustermetadatapb.LeadershipMode_LEADERSHIP_MODE_UNMANAGED, backing: backing}, nil
}

// readRecordedLeadershipMode returns the leadership mode of this pooler's topology record, and
// whether there is one: a missing record is a first start. Any other read failure counts as no
// record, so a pooler that has no catalog row still starts while topology is unreachable; nothing
// rests on this read, because RegisterMultipooler refuses to change whether a record is managed
// (topoclient.ErrManagementChanged).
func readRecordedLeadershipMode(ctx context.Context, ts topoclient.Store, id *clustermetadatapb.ID, logger *slog.Logger) (clustermetadatapb.LeadershipMode, bool) {
	info, err := ts.GetMultipooler(ctx, id)
	switch {
	case err == nil:
		return info.GetLeadershipMode(), true
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}):
		return 0, false
	}
	logger.WarnContext(ctx, "could not read this pooler's topology record to learn whether it is unmanaged; going by its tablegroup",
		"pooler_id", topoclient.ComponentIDString(id), "error", err)
	return 0, false
}

// bindingSource is where the pooler learns how its tablegroup is served: the default primary of
// its database, dialed with the --multipooler-grpc-* client flags as every dial to a pooler is. A
// cluster whose poolers serve TLS sets them on an unmanaged pooler too.
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

// awaitBinding asks the default primary how the tablegroup is served, until it answers.
func (mp *Multipooler) awaitBinding(ctx context.Context, logger *slog.Logger, tablegroupID int64) (*backingconn.Conn, bool, error) {
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
		conn, managed, err = fetchBinding(ctx, source, tablegroupID)
		return err
	}, func(attempt int, err error) {
		// The wait can be long; warn on the first attempts and then every tenth.
		level := slog.LevelDebug
		if attempt <= 3 || attempt%10 == 0 {
			level = slog.LevelWarn
		}
		logger.Log(ctx, level, "waiting for the catalog to say how this tablegroup is served",
			"tablegroup_id", tablegroupID, "attempt", attempt, "error", err)
	})
	if err != nil {
		return nil, false, fmt.Errorf("the catalog did not say how tablegroup %d is served: %w", tablegroupID, err)
	}
	return conn, managed, nil
}
