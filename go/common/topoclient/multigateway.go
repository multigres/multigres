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

// NewMultigateway creates a new Multigateway record with the given name, cell, and hostname.
func NewMultigateway(name string, cell, host string) *clustermetadatapb.Multigateway {
	return &clustermetadatapb.Multigateway{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// MultigatewayInfo is the container for a Multigateway, read from the topology server.
type MultigatewayInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.Multigateway
}

// String returns a string describing the multigateway.
func (mgi *MultigatewayInfo) String() string {
	return fmt.Sprintf("Multigateway{%v}", ComponentIDString(mgi.Id))
}

// Addr returns hostname:grpc port.
func (mgi *MultigatewayInfo) Addr() string {
	grpcPort, ok := mgi.PortMap["grpc"]
	if !ok {
		return mgi.Hostname
	}
	return fmt.Sprintf("%s:%d", mgi.Hostname, grpcPort)
}

// Version returns the version of this multigateway from last time it was read or updated.
func (mgi *MultigatewayInfo) Version() Version {
	return mgi.version
}

// NewMultigatewayInfo returns a MultigatewayInfo based on multigateway with the
// version set. This function should be only used by Server implementations.
func NewMultigatewayInfo(multigateway *clustermetadatapb.Multigateway, version Version) *MultigatewayInfo {
	return &MultigatewayInfo{version: version, Multigateway: multigateway}
}

// GetMultigateway is a high level function to read multigateway data.
func (ts *store) GetMultigateway(ctx context.Context, id *clustermetadatapb.ID) (*MultigatewayInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	gatewayPath := path.Join(GatewaysPath, string(ComponentIDString(id)), GatewayFile)
	data, version, err := conn.Get(ctx, gatewayPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multigateway %q", id))
	}
	multigateway := &clustermetadatapb.Multigateway{}
	if err := proto.Unmarshal(data, multigateway); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multigateway data")
	}

	return &MultigatewayInfo{
		version:      version,
		Multigateway: multigateway,
	}, nil
}

// GetMultigatewayIDsByCell returns all the multigateway IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multigateways in it.
func (ts *store) GetMultigatewayIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	// List the directory, and parse the IDs
	children, err := conn.List(ctx, GatewaysPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			// directory doesn't exist, empty list, no error.
			return nil, nil
		}
		return nil, err
	}

	result := make([]*clustermetadatapb.ID, len(children))
	for i, child := range children {
		multigateway := &clustermetadatapb.Multigateway{}
		if err := proto.Unmarshal(child.Value, multigateway); err != nil {
			return nil, err
		}
		result[i] = multigateway.Id
	}
	return result, nil
}

// GetMultigatewaysByCell returns all the multigateways in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multigateways couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multigateways in it.
func (ts *store) GetMultigatewaysByCell(ctx context.Context, cellName string) ([]*MultigatewayInfo, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	cellConn, err := ts.ConnForCell(ctx, cellName)
	if err != nil {
		return nil, err
	}
	listResults, err := cellConn.List(ctx, GatewaysPath)
	if err != nil {
		if errors.Is(err, &TopoError{Code: NoNode}) {
			return nil, nil
		}
		return nil, err
	}

	mtgateways := make([]*MultigatewayInfo, 0, len(listResults))
	for n := range listResults {
		multigateway := &clustermetadatapb.Multigateway{}
		if err := proto.Unmarshal(listResults[n].Value, multigateway); err != nil {
			return nil, err
		}
		mtgateways = append(mtgateways, &MultigatewayInfo{Multigateway: multigateway, version: listResults[n].Version})
	}
	return mtgateways, nil
}

// UpdateMultigateway updates the multigateway data only - not associated replication paths.
func (ts *store) UpdateMultigateway(ctx context.Context, mgi *MultigatewayInfo) error {
	conn, err := ts.ConnForCell(ctx, mgi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mgi.Multigateway)
	if err != nil {
		return err
	}
	gatewayPath := path.Join(GatewaysPath, string(ComponentIDString(mgi.Id)), GatewayFile)
	newVersion, err := conn.Update(ctx, gatewayPath, data, mgi.version)
	if err != nil {
		return err
	}
	mgi.version = newVersion

	return nil
}

// UpdateMultigatewayFields is a high level helper to read a multigateway record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multigateway.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultigatewayFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.Multigateway) error) (*clustermetadatapb.Multigateway, error) {
	for {
		mgi, err := ts.GetMultigateway(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(mgi.Multigateway); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultigateway(ctx, mgi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return mgi.Multigateway, err
		}
	}
}

// CreateMultigateway creates a new multigateway and all associated paths.
func (ts *store) CreateMultigateway(ctx context.Context, mtgateway *clustermetadatapb.Multigateway) error {
	conn, err := ts.ConnForCell(ctx, mtgateway.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtgateway)
	if err != nil {
		return err
	}
	gatewayPath := path.Join(GatewaysPath, string(ComponentIDString(mtgateway.Id)), GatewayFile)
	if _, err := conn.Create(ctx, gatewayPath, data); err != nil {
		return err
	}

	return nil
}

// UnregisterMultigateway deletes the specified multigateway.
func (ts *store) UnregisterMultigateway(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	gatewayPath := path.Join(GatewaysPath, string(ComponentIDString(id)), GatewayFile)
	if err := conn.Delete(ctx, gatewayPath, nil); err != nil {
		return err
	}

	return nil
}

// RegisterMultigateway creates or updates a multigateway. If allowUpdate is true,
// and a multigateway with the same ID exists, just update it.
func (ts *store) RegisterMultigateway(ctx context.Context, mtgateway *clustermetadatapb.Multigateway, allowUpdate bool) error {
	err := ts.CreateMultigateway(ctx, mtgateway)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtGateway, err := ts.GetMultigateway(ctx, mtgateway.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtgateway %v: %w", ComponentIDString(mtgateway.Id), err)
		}

		oldMtGateway.Multigateway = proto.Clone(mtgateway).(*clustermetadatapb.Multigateway)
		if err := ts.UpdateMultigateway(ctx, oldMtGateway); err != nil {
			return fmt.Errorf("failed updating mtgateway %v: %w", ComponentIDString(mtgateway.Id), err)
		}
		return nil
	}
	return err
}
