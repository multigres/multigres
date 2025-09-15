// Copyright 2025 The Supabase, Inc.
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
	"github.com/multigres/multigres/go/tools/stringutil"

	"google.golang.org/protobuf/proto"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// NewMultiOrch creates a new MultiOrch record with the given name, cell, and hostname.
// If name is empty, a random name will be generated.
func NewMultiOrch(name string, cell, host string) *clustermetadatapb.MultiOrch {
	if name == "" {
		name = stringutil.RandomString(8)
	}
	return &clustermetadatapb.MultiOrch{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// MultiOrchInfo is the container for a MultiOrch, read from the topology server.
type MultiOrchInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.MultiOrch
}

// String returns a string describing the multiorch.
func (moi *MultiOrchInfo) String() string {
	return fmt.Sprintf("MultiOrch{%v}", MultiOrchIDString(moi.Id))
}

// IDString returns the string representation of the multiorch id
func (moi *MultiOrchInfo) IDString() string {
	return MultiOrchIDString(moi.Id)
}

// Addr returns hostname:grpc port.
func (moi *MultiOrchInfo) Addr() string {
	grpcPort, ok := moi.PortMap["grpc"]
	if !ok {
		return moi.Hostname
	}
	return fmt.Sprintf("%s:%d", moi.Hostname, grpcPort)
}

// Version returns the version of this multiorch from last time it was read or updated.
func (moi *MultiOrchInfo) Version() Version {
	return moi.version
}

// NewMultiOrchInfo returns a MultiOrchInfo based on multiorch with the
// version set. This function should be only used by Server implementations.
func NewMultiOrchInfo(multiorch *clustermetadatapb.MultiOrch, version Version) *MultiOrchInfo {
	return &MultiOrchInfo{version: version, MultiOrch: multiorch}
}

// MultiOrchIDString returns the string representation of a MultiOrch ID
func MultiOrchIDString(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s-%s-%s", stringutil.ComponentTypeToString(id.Component), id.Cell, id.Name)
}

// GetMultiOrch is a high level function to read multiorch data.
func (ts *store) GetMultiOrch(ctx context.Context, id *clustermetadatapb.ID) (*MultiOrchInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	orchPath := path.Join(OrchsPath, MultiOrchIDString(id), OrchFile)
	data, version, err := conn.Get(ctx, orchPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multiorch %q", id))
	}
	multiorch := &clustermetadatapb.MultiOrch{}
	if err := proto.Unmarshal(data, multiorch); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multiorch data")
	}

	return &MultiOrchInfo{
		version:   version,
		MultiOrch: multiorch,
	}, nil
}

// GetMultiOrchIDsByCell returns all the multiorch IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multiorchs in it.
func (ts *store) GetMultiOrchIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	// List the directory, and parse the IDs
	children, err := conn.List(ctx, OrchsPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			// directory doesn't exist, empty list, no error.
			return nil, nil
		}
		return nil, err
	}

	result := make([]*clustermetadatapb.ID, len(children))
	for i, child := range children {
		multiorch := &clustermetadatapb.MultiOrch{}
		if err := proto.Unmarshal(child.Value, multiorch); err != nil {
			return nil, err
		}
		result[i] = multiorch.Id
	}
	return result, nil
}

// GetMultiOrchsByCell returns all the multiorchs in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multiorchs couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multiorchs in it.
func (ts *store) GetMultiOrchsByCell(ctx context.Context, cellName string) ([]*MultiOrchInfo, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	cellConn, err := ts.ConnForCell(ctx, cellName)
	if err != nil {
		return nil, err
	}
	listResults, err := cellConn.List(ctx, OrchsPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			return nil, nil
		}
		return nil, err
	}

	mtorchs := make([]*MultiOrchInfo, 0, len(listResults))
	for n := range listResults {
		multiorch := &clustermetadatapb.MultiOrch{}
		if err := proto.Unmarshal(listResults[n].Value, multiorch); err != nil {
			return nil, err
		}
		mtorchs = append(mtorchs, &MultiOrchInfo{MultiOrch: multiorch, version: listResults[n].Version})
	}
	return mtorchs, nil
}

// UpdateMultiOrch updates the multiorch data only - not associated replication paths.
func (ts *store) UpdateMultiOrch(ctx context.Context, moi *MultiOrchInfo) error {
	conn, err := ts.ConnForCell(ctx, moi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(moi.MultiOrch)
	if err != nil {
		return err
	}
	orchPath := path.Join(OrchsPath, MultiOrchIDString(moi.Id), OrchFile)
	newVersion, err := conn.Update(ctx, orchPath, data, moi.version)
	if err != nil {
		return err
	}
	moi.version = newVersion

	return nil
}

// UpdateMultiOrchFields is a high level helper to read a multiorch record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multiorch.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultiOrchFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiOrch) error) (*clustermetadatapb.MultiOrch, error) {
	for {
		moi, err := ts.GetMultiOrch(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(moi.MultiOrch); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultiOrch(ctx, moi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return moi.MultiOrch, err
		}
	}
}

// CreateMultiOrch creates a new multiorch and all associated paths.
func (ts *store) CreateMultiOrch(ctx context.Context, mtorch *clustermetadatapb.MultiOrch) error {
	conn, err := ts.ConnForCell(ctx, mtorch.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtorch)
	if err != nil {
		return err
	}
	orchPath := path.Join(OrchsPath, MultiOrchIDString(mtorch.Id), OrchFile)
	if _, err := conn.Create(ctx, orchPath, data); err != nil {
		return err
	}

	return nil
}

// DeleteMultiOrch deletes the specified multiorch.
func (ts *store) DeleteMultiOrch(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	orchPath := path.Join(OrchsPath, MultiOrchIDString(id), OrchFile)
	if err := conn.Delete(ctx, orchPath, nil); err != nil {
		return err
	}

	return nil
}

// InitMultiOrch creates or updates a multiorch. If allowUpdate is true,
// and a multiorch with the same ID exists, just update it.
func (ts *store) InitMultiOrch(ctx context.Context, mtorch *clustermetadatapb.MultiOrch, allowUpdate bool) error {
	err := ts.CreateMultiOrch(ctx, mtorch)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtOrch, err := ts.GetMultiOrch(ctx, mtorch.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtorch %v: %v", MultiOrchIDString(mtorch.Id), err)
		}

		oldMtOrch.MultiOrch = proto.Clone(mtorch).(*clustermetadatapb.MultiOrch)
		if err := ts.UpdateMultiOrch(ctx, oldMtOrch); err != nil {
			return fmt.Errorf("failed updating mtorch %v: %v", MultiOrchIDString(mtorch.Id), err)
		}
		return nil
	}
	return err
}
