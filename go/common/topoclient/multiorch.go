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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// NewMultiorch creates a new Multiorch record with the given name, cell, and hostname.
func NewMultiorch(name string, cell, host string) *clustermetadatapb.Multiorch {
	return &clustermetadatapb.Multiorch{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// MultiorchInfo is the container for a Multiorch, read from the topology server.
type MultiorchInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.Multiorch
}

// String returns a string describing the multiorch.
func (moi *MultiorchInfo) String() string {
	return fmt.Sprintf("Multiorch{%v}", ComponentIDString(moi.Id))
}

// Addr returns hostname:grpc port.
func (moi *MultiorchInfo) Addr() string {
	grpcPort, ok := moi.PortMap["grpc"]
	if !ok {
		return moi.Hostname
	}
	return fmt.Sprintf("%s:%d", moi.Hostname, grpcPort)
}

// Version returns the version of this multiorch from last time it was read or updated.
func (moi *MultiorchInfo) Version() Version {
	return moi.version
}

// NewMultiorchInfo returns a MultiorchInfo based on multiorch with the
// version set. This function should be only used by Server implementations.
func NewMultiorchInfo(multiorch *clustermetadatapb.Multiorch, version Version) *MultiorchInfo {
	return &MultiorchInfo{version: version, Multiorch: multiorch}
}

// GetMultiorch is a high level function to read multiorch data.
func (ts *store) GetMultiorch(ctx context.Context, id *clustermetadatapb.ID) (*MultiorchInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	orchPath := path.Join(OrchsPath, string(ComponentIDString(id)), OrchFile)
	data, version, err := conn.Get(ctx, orchPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multiorch %q", id))
	}
	multiorch := &clustermetadatapb.Multiorch{}
	if err := proto.Unmarshal(data, multiorch); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multiorch data")
	}

	return &MultiorchInfo{
		version:   version,
		Multiorch: multiorch,
	}, nil
}

// GetMultiorchIDsByCell returns all the multiorch IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multiorchs in it.
func (ts *store) GetMultiorchIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
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
		multiorch := &clustermetadatapb.Multiorch{}
		if err := proto.Unmarshal(child.Value, multiorch); err != nil {
			return nil, err
		}
		result[i] = multiorch.Id
	}
	return result, nil
}

// GetMultiorchsByCell returns all the multiorchs in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multiorchs couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multiorchs in it.
func (ts *store) GetMultiorchsByCell(ctx context.Context, cellName string) ([]*MultiorchInfo, error) {
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

	mtorchs := make([]*MultiorchInfo, 0, len(listResults))
	for n := range listResults {
		multiorch := &clustermetadatapb.Multiorch{}
		if err := proto.Unmarshal(listResults[n].Value, multiorch); err != nil {
			return nil, err
		}
		mtorchs = append(mtorchs, &MultiorchInfo{Multiorch: multiorch, version: listResults[n].Version})
	}
	return mtorchs, nil
}

// UpdateMultiorch updates the multiorch data only - not associated replication paths.
func (ts *store) UpdateMultiorch(ctx context.Context, moi *MultiorchInfo) error {
	conn, err := ts.ConnForCell(ctx, moi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(moi.Multiorch)
	if err != nil {
		return err
	}
	orchPath := path.Join(OrchsPath, string(ComponentIDString(moi.Id)), OrchFile)
	newVersion, err := conn.Update(ctx, orchPath, data, moi.version)
	if err != nil {
		return err
	}
	moi.version = newVersion

	return nil
}

// UpdateMultiorchFields is a high level helper to read a multiorch record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multiorch.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultiorchFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.Multiorch) error) (*clustermetadatapb.Multiorch, error) {
	for {
		moi, err := ts.GetMultiorch(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(moi.Multiorch); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultiorch(ctx, moi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return moi.Multiorch, err
		}
	}
}

// CreateMultiorch creates a new multiorch and all associated paths.
func (ts *store) CreateMultiorch(ctx context.Context, mtorch *clustermetadatapb.Multiorch) error {
	conn, err := ts.ConnForCell(ctx, mtorch.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtorch)
	if err != nil {
		return err
	}
	orchPath := path.Join(OrchsPath, string(ComponentIDString(mtorch.Id)), OrchFile)
	if _, err := conn.Create(ctx, orchPath, data); err != nil {
		return err
	}

	return nil
}

// UnregisterMultiorch deletes the specified multiorch.
func (ts *store) UnregisterMultiorch(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	orchPath := path.Join(OrchsPath, string(ComponentIDString(id)), OrchFile)
	if err := conn.Delete(ctx, orchPath, nil); err != nil {
		return err
	}

	return nil
}

// RegisterMultiorch creates or updates a multiorch. If allowUpdate is true,
// and a multiorch with the same ID exists, just update it.
func (ts *store) RegisterMultiorch(ctx context.Context, mtorch *clustermetadatapb.Multiorch, allowUpdate bool) error {
	err := ts.CreateMultiorch(ctx, mtorch)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtOrch, err := ts.GetMultiorch(ctx, mtorch.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtorch %v: %w", ComponentIDString(mtorch.Id), err)
		}

		oldMtOrch.Multiorch = proto.Clone(mtorch).(*clustermetadatapb.Multiorch)
		if err := ts.UpdateMultiorch(ctx, oldMtOrch); err != nil {
			return fmt.Errorf("failed updating mtorch %v: %w", ComponentIDString(mtorch.Id), err)
		}
		return nil
	}
	return err
}
