// Copyright 2025 The Multigres Authors.
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

package topo

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/multigres/multigres/go/mterrors"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// NewMultiPooler creates a new MultiPooler record with the given name, cell, and hostname.
// If name is empty, a random name will be generated.
func NewMultiPooler(name string, cell, host string) *clustermetadatapb.MultiPooler {
	if name == "" {
		name = RandomString(8)
	}
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// MultiPoolerInfo is the container for a MultiPooler, read from the topology server.
type MultiPoolerInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.MultiPooler
}

// String returns a string describing the multipooler.
func (mpi *MultiPoolerInfo) String() string {
	return fmt.Sprintf("MultiPooler{%v}", MultiPoolerIDString(mpi.Id))
}

// IDString returns the string representation of the multipooler id
func (mpi *MultiPoolerInfo) IDString() string {
	return MultiPoolerIDString(mpi.Id)
}

// Addr returns hostname:grpc port.
func (mpi *MultiPoolerInfo) Addr() string {
	grpcPort, ok := mpi.PortMap["grpc"]
	if !ok {
		return mpi.Hostname
	}
	return fmt.Sprintf("%s:%d", mpi.Hostname, grpcPort)
}

// Version returns the version of this multipooler from last time it was read or updated.
func (mpi *MultiPoolerInfo) Version() Version {
	return mpi.version
}

// NewMultiPoolerInfo returns a MultiPoolerInfo based on multipooler with the
// version set. This function should be only used by Server implementations.
func NewMultiPoolerInfo(multipooler *clustermetadatapb.MultiPooler, version Version) *MultiPoolerInfo {
	return &MultiPoolerInfo{version: version, MultiPooler: multipooler}
}

// MultiPoolerIDString returns the string representation of a MultiPooler ID
func MultiPoolerIDString(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s-%s-%s", ComponentTypeToString(id.Component), id.Cell, id.Name)
}

// GetMultiPooler is a high level function to read multipooler data.
func (ts *store) GetMultiPooler(ctx context.Context, id *clustermetadatapb.ID) (*MultiPoolerInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	poolerPath := path.Join(PoolersPath, MultiPoolerIDString(id), PoolerFile)
	data, version, err := conn.Get(ctx, poolerPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multipooler %q", id))
	}
	multipooler := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(data, multipooler); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multipooler data")
	}

	return &MultiPoolerInfo{
		version:     version,
		MultiPooler: multipooler,
	}, nil
}

// GetMultiPoolerIDsByCell returns all the multipooler IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multipoolers in it.
func (ts *store) GetMultiPoolerIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
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
		multipooler := &clustermetadatapb.MultiPooler{}
		if err := proto.Unmarshal(child.Value, multipooler); err != nil {
			return nil, err
		}
		result[i] = multipooler.Id
	}
	return result, nil
}

// GetMultiPoolersByCellOptions controls the behavior of GetMultiPoolersByCell.
type GetMultiPoolersByCellOptions struct {
	// DatabaseShard is the optional database/shard that multipoolers must match.
	// An empty shard value will match all shards in the database.
	DatabaseShard *DatabaseShard
}

// DatabaseShard represents a database and shard pair.
type DatabaseShard struct {
	Database string
	Shard    string
}

// GetMultiPoolersByCell returns all the multipoolers in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multipoolers couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multipoolers in it.
func (ts *store) GetMultiPoolersByCell(ctx context.Context, cellName string, opt *GetMultiPoolersByCellOptions) ([]*MultiPoolerInfo, error) {
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

	mtpoolers := make([]*MultiPoolerInfo, 0, capHint)
	for n := range listResults {
		multipooler := &clustermetadatapb.MultiPooler{}
		if err := proto.Unmarshal(listResults[n].Value, multipooler); err != nil {
			return nil, err
		}
		if opt != nil && opt.DatabaseShard != nil && opt.DatabaseShard.Database != "" {
			if opt.DatabaseShard.Database != multipooler.Database {
				continue
			}
			if opt.DatabaseShard.Shard != "" && opt.DatabaseShard.Shard != multipooler.Shard {
				continue
			}
		}
		mtpoolers = append(mtpoolers, &MultiPoolerInfo{MultiPooler: multipooler, version: listResults[n].Version})
	}
	return mtpoolers, nil
}

// UpdateMultiPooler updates the multipooler data only - not associated replication paths.
func (ts *store) UpdateMultiPooler(ctx context.Context, mpi *MultiPoolerInfo) error {
	conn, err := ts.ConnForCell(ctx, mpi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mpi.MultiPooler)
	if err != nil {
		return err
	}
	poolerPath := path.Join(PoolersPath, MultiPoolerIDString(mpi.Id), PoolerFile)
	newVersion, err := conn.Update(ctx, poolerPath, data, mpi.version)
	if err != nil {
		return err
	}
	mpi.version = newVersion

	return nil
}

// UpdateMultiPoolerFields is a high level helper to read a multipooler record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multipooler.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultiPoolerFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiPooler) error) (*clustermetadatapb.MultiPooler, error) {
	for {
		mpi, err := ts.GetMultiPooler(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(mpi.MultiPooler); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultiPooler(ctx, mpi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return mpi.MultiPooler, err
		}
	}
}

// CreateMultiPooler creates a new multipooler and all associated paths.
func (ts *store) CreateMultiPooler(ctx context.Context, mtpooler *clustermetadatapb.MultiPooler) error {
	conn, err := ts.ConnForCell(ctx, mtpooler.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtpooler)
	if err != nil {
		return err
	}
	poolerPath := path.Join(PoolersPath, MultiPoolerIDString(mtpooler.Id), PoolerFile)
	if _, err := conn.Create(ctx, poolerPath, data); err != nil {
		return err
	}

	return nil
}

// DeleteMultiPooler deletes the specified multipooler.
func (ts *store) DeleteMultiPooler(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	poolerPath := path.Join(PoolersPath, MultiPoolerIDString(id), PoolerFile)
	if err := conn.Delete(ctx, poolerPath, nil); err != nil {
		return err
	}

	return nil
}

// InitMultiPooler creates or updates a multipooler. If allowUpdate is true,
// and a multipooler with the same ID exists, just update it.
// If a multipooler is created as primary, and there is already a different
// primary in the shard, allowPrimaryOverride must be set.
func (ts *store) InitMultiPooler(ctx context.Context, mtpooler *clustermetadatapb.MultiPooler, allowPrimaryOverride, allowUpdate bool) error {
	// TODO (@rafa): How are we going to do this? Is the topo suppose to try to discover
	// where is the primary? We no longer have the shard metadata in the topo.
	// In this context how do we discover where the ShardMetadata is???
	err := ts.CreateMultiPooler(ctx, mtpooler)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtPooler, err := ts.GetMultiPooler(ctx, mtpooler.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtpooler %v: %v", MultiPoolerIDString(mtpooler.Id), err)
		}

		// Check we have the same database / shard, and if not,
		// require the allowDifferentShard flag.
		if oldMtPooler.Database != mtpooler.Database || oldMtPooler.Shard != mtpooler.Shard {
			return fmt.Errorf("old mtpooler has shard %v/%v. Cannot override with shard %v/%v. Delete and re-add mtpooler if you want to change the mtpooler's database/shard", oldMtPooler.Database, oldMtPooler.Shard, mtpooler.Database, mtpooler.Shard)
		}
		oldMtPooler.MultiPooler = proto.Clone(mtpooler).(*clustermetadatapb.MultiPooler)
		if err := ts.UpdateMultiPooler(ctx, oldMtPooler); err != nil {
			return fmt.Errorf("failed updating mtpooler %v: %v", MultiPoolerIDString(mtpooler.Id), err)
		}
		return nil
	}
	return err
}
