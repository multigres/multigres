// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package topoclient

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/multigres/multigres/go/common/mterrors"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// NewMultipooler creates a new Multipooler record with the given name, cell,
// and hostname. The caller is responsible for setting ShardKey before
// registering the record — leaving it partially filled here invited subtle
// bugs (a missing Database lets two databases collide on the same
// tableGroup/shard pair).
func NewMultipooler(name, cell, host string) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
		// The pooler process is up but postgres readiness has not yet been
		// confirmed; the manager transitions this to ACTIVE once the
		// pgMonitor observes postgres responding. The timestamp is set at
		// construction time rather than at write time — the caller
		// (registerFunc) invokes RegisterMultipooler within microseconds
		// of the factory call, so the difference is negligible and avoids
		// plumbing a clock through the factory's call sites.
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{
			Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
			Reason:  "process starting",
			Updated: timestamppb.Now(),
		},
	}
}

// MultipoolerInfo is the container for a Multipooler, read from the topology server.
type MultipoolerInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.Multipooler
}

// String returns a string describing the multipooler.
func (mpi *MultipoolerInfo) String() string {
	return fmt.Sprintf("Multipooler{%v}", ComponentIDString(mpi.Id))
}

// Addr returns hostname:grpc port.
func (mpi *MultipoolerInfo) Addr() string {
	grpcPort, ok := mpi.PortMap["grpc"]
	if !ok {
		return mpi.Hostname
	}
	return fmt.Sprintf("%s:%d", mpi.Hostname, grpcPort)
}

// Version returns the version of this multipooler from last time it was read or updated.
func (mpi *MultipoolerInfo) Version() Version {
	return mpi.version
}

// NewMultipoolerInfo returns a MultipoolerInfo based on multipooler with the
// version set. This function should be only used by Server implementations.
func NewMultipoolerInfo(multipooler *clustermetadatapb.Multipooler, version Version) *MultipoolerInfo {
	return &MultipoolerInfo{version: version, Multipooler: multipooler}
}

// PoolerAddressFor projects a Multipooler into the contact-info subset the
// consensus RPCs (SetPrimary, Promote) take. Returns nil if mp is nil.
func PoolerAddressFor(mp *clustermetadatapb.Multipooler) *clustermetadatapb.PoolerAddress {
	if mp == nil {
		return nil
	}
	return &clustermetadatapb.PoolerAddress{
		Id:           mp.GetId(),
		Host:         mp.GetHostname(),
		PostgresPort: mp.GetPortMap()["postgres"],
	}
}

// GetMultipooler is a high level function to read multipooler data.
func (ts *store) GetMultipooler(ctx context.Context, id *clustermetadatapb.ID) (*MultipoolerInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	poolerPath := path.Join(PoolersPath, string(ComponentIDString(id)), PoolerFile)
	data, version, err := conn.Get(ctx, poolerPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multipooler %q", id))
	}
	multipooler := &clustermetadatapb.Multipooler{}
	if err := proto.Unmarshal(data, multipooler); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multipooler data")
	}

	return &MultipoolerInfo{
		version:     version,
		Multipooler: multipooler,
	}, nil
}

// GetMultipoolerIDsByCell returns all the multipooler IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multipoolers in it.
func (ts *store) GetMultipoolerIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	// List the directory, and parse the IDs
	children, err := conn.List(ctx, PoolersPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			// directory doesn't exist, empty list, no error.
			return nil, nil
		}
		return nil, err
	}

	result := make([]*clustermetadatapb.ID, len(children))
	for i, child := range children {
		multipooler := &clustermetadatapb.Multipooler{}
		if err := proto.Unmarshal(child.Value, multipooler); err != nil {
			return nil, err
		}
		result[i] = multipooler.Id
	}
	return result, nil
}

// GetMultipoolersByCellOptions controls the behavior of GetMultipoolersByCell.
type GetMultipoolersByCellOptions struct {
	// DatabaseShard is the optional database/tablegroup/shard that multipoolers must match.
	// An empty tablegroup value will match all tablegroups in the database.
	// An empty shard value will match all shards in the tablegroup.
	DatabaseShard *DatabaseShard
}

// DatabaseShard represents a database, tablegroup, and shard tuple for filtering.
// Supports hierarchical matching:
// - Database only: matches all tablegroups and shards in database
// - Database + TableGroup: matches all shards in that tablegroup
// - Database + TableGroup + Shard: matches only that specific shard
type DatabaseShard struct {
	Database   string
	TableGroup string // empty = all tablegroups in database
	Shard      string // empty = all shards in tablegroup
}

// GetMultipoolersByCell returns all the multipoolers in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multipoolers couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multipoolers in it.
func (ts *store) GetMultipoolersByCell(ctx context.Context, cellName string, opt *GetMultipoolersByCellOptions) ([]*MultipoolerInfo, error) {
	// Validate filtering hierarchy: Database -> TableGroup -> Shard
	if opt != nil && opt.DatabaseShard != nil {
		ds := opt.DatabaseShard
		// If Shard is specified, TableGroup must be specified
		if ds.Shard != "" && ds.TableGroup == "" {
			return nil, NewError(BadInput, "cannot filter by Shard without specifying TableGroup")
		}
		// If TableGroup is specified, Database must be specified
		if ds.TableGroup != "" && ds.Database == "" {
			return nil, NewError(BadInput, "cannot filter by TableGroup without specifying Database")
		}
	}

	// If the cell doesn't exist, this will return ErrNoNode.
	cellConn, err := ts.ConnForCell(ctx, cellName)
	if err != nil {
		return nil, err
	}
	listResults, err := cellConn.List(ctx, PoolersPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			return nil, nil
		}
		return nil, err
	}

	var capHint int
	if opt != nil && opt.DatabaseShard == nil {
		capHint = len(listResults)
	}

	mtpoolers := make([]*MultipoolerInfo, 0, capHint)
	for n := range listResults {
		multipooler := &clustermetadatapb.Multipooler{}
		if err := proto.Unmarshal(listResults[n].Value, multipooler); err != nil {
			return nil, err
		}
		if opt != nil && opt.DatabaseShard != nil && opt.DatabaseShard.Database != "" {
			sk := multipooler.GetShardKey()
			// Database must match
			if opt.DatabaseShard.Database != sk.GetDatabase() {
				continue
			}
			// If TableGroup is specified, it must match
			if opt.DatabaseShard.TableGroup != "" && opt.DatabaseShard.TableGroup != sk.GetTableGroup() {
				continue
			}
			// If Shard is specified, it must match
			if opt.DatabaseShard.Shard != "" && opt.DatabaseShard.Shard != sk.GetShard() {
				continue
			}
		}
		mtpoolers = append(mtpoolers, &MultipoolerInfo{Multipooler: multipooler, version: listResults[n].Version})
	}
	return mtpoolers, nil
}

// UpdateMultipooler updates the multipooler data only - not associated replication paths.
func (ts *store) UpdateMultipooler(ctx context.Context, mpi *MultipoolerInfo) error {
	conn, err := ts.ConnForCell(ctx, mpi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mpi.Multipooler)
	if err != nil {
		return err
	}
	poolerPath := path.Join(PoolersPath, string(ComponentIDString(mpi.Id)), PoolerFile)
	newVersion, err := conn.Update(ctx, poolerPath, data, mpi.version)
	if err != nil {
		return err
	}
	mpi.version = newVersion

	return nil
}

// UpdateMultipoolerFields is a high level helper to read a multipooler record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multipooler.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultipoolerFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.Multipooler) error) (*clustermetadatapb.Multipooler, error) {
	for {
		mpi, err := ts.GetMultipooler(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(mpi.Multipooler); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultipooler(ctx, mpi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return mpi.Multipooler, err
		}
	}
}

// CreateMultipooler creates a new multipooler and all associated paths.
func (ts *store) CreateMultipooler(ctx context.Context, mtpooler *clustermetadatapb.Multipooler) error {
	conn, err := ts.ConnForCell(ctx, mtpooler.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtpooler)
	if err != nil {
		return err
	}
	poolerPath := path.Join(PoolersPath, string(ComponentIDString(mtpooler.Id)), PoolerFile)
	if _, err := conn.Create(ctx, poolerPath, data); err != nil {
		return err
	}

	return nil
}

// UnregisterMultipooler deletes the specified multipooler.
func (ts *store) UnregisterMultipooler(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	poolerPath := path.Join(PoolersPath, string(ComponentIDString(id)), PoolerFile)
	if err := conn.Delete(ctx, poolerPath, nil); err != nil {
		return err
	}

	return nil
}

// RegisterMultipooler creates or updates a multipooler. If allowUpdate is true,
// and a multipooler with the same ID exists, just update it.
func (ts *store) RegisterMultipooler(ctx context.Context, mtpooler *clustermetadatapb.Multipooler, allowUpdate bool) error {
	err := ts.CreateMultipooler(ctx, mtpooler)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtPooler, err := ts.GetMultipooler(ctx, mtpooler.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtpooler %v: %w", ComponentIDString(mtpooler.Id), err)
		}
		oldMtPooler.Multipooler = proto.Clone(mtpooler).(*clustermetadatapb.Multipooler)
		if err := ts.UpdateMultipooler(ctx, oldMtPooler); err != nil {
			return fmt.Errorf("failed updating mtpooler %v: %w", ComponentIDString(mtpooler.Id), err)
		}
		return nil
	}
	return err
}
