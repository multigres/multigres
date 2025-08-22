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

// NewMultiGateway creates a new MultiGateway record with the given name, cell, and hostname.
// If name is empty, a random name will be generated.
func NewMultiGateway(name string, cell, host string) *clustermetadatapb.MultiGateway {
	if name == "" {
		name = RandomString(8)
	}
	return &clustermetadatapb.MultiGateway{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIGATEWAY,
			Cell:      cell,
			Name:      name,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// MultiGatewayInfo is the container for a MultiGateway, read from the topology server.
type MultiGatewayInfo struct {
	version Version // node version - used to prevent stomping concurrent writes
	*clustermetadatapb.MultiGateway
}

// String returns a string describing the multigateway.
func (mgi *MultiGatewayInfo) String() string {
	return fmt.Sprintf("MultiGateway{%v}", MultiGatewayIDString(mgi.Id))
}

// IDString returns the string representation of the multigateway id
func (mgi *MultiGatewayInfo) IDString() string {
	return MultiGatewayIDString(mgi.Id)
}

// Addr returns hostname:grpc port.
func (mgi *MultiGatewayInfo) Addr() string {
	grpcPort, ok := mgi.PortMap["grpc"]
	if !ok {
		return mgi.Hostname
	}
	return fmt.Sprintf("%s:%d", mgi.Hostname, grpcPort)
}

// Version returns the version of this multigateway from last time it was read or updated.
func (mgi *MultiGatewayInfo) Version() Version {
	return mgi.version
}

// NewMultiGatewayInfo returns a MultiGatewayInfo based on multigateway with the
// version set. This function should be only used by Server implementations.
func NewMultiGatewayInfo(multigateway *clustermetadatapb.MultiGateway, version Version) *MultiGatewayInfo {
	return &MultiGatewayInfo{version: version, MultiGateway: multigateway}
}

// MultiGatewayIDString returns the string representation of a MultiGateway ID
func MultiGatewayIDString(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s-%s-%s", ComponentTypeToString(id.Component), id.Cell, id.Name)
}

// GetMultiGateway is a high level function to read multigateway data.
func (ts *store) GetMultiGateway(ctx context.Context, id *clustermetadatapb.ID) (*MultiGatewayInfo, error) {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get connection for cell %q", id.Cell))
	}

	gatewayPath := path.Join(GatewaysPath, MultiGatewayIDString(id), GatewayFile)
	data, version, err := conn.Get(ctx, gatewayPath)
	if err != nil {
		return nil, mterrors.Wrap(err, fmt.Sprintf("unable to get multigateway %q", id))
	}
	multigateway := &clustermetadatapb.MultiGateway{}
	if err := proto.Unmarshal(data, multigateway); err != nil {
		return nil, mterrors.Wrap(err, "failed to unmarshal multigateway data")
	}

	return &MultiGatewayInfo{
		version:      version,
		MultiGateway: multigateway,
	}, nil
}

// GetMultiGatewayIDsByCell returns all the multigateway IDs in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no multigateways in it.
func (ts *store) GetMultiGatewayIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
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
		multigateway := &clustermetadatapb.MultiGateway{}
		if err := proto.Unmarshal(child.Value, multigateway); err != nil {
			return nil, err
		}
		result[i] = multigateway.Id
	}
	return result, nil
}

// GetMultiGatewaysByCell returns all the multigateways in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns ErrPartialResult if some multigateways couldn't be read. The results in the slice are incomplete.
// It returns (nil, nil) if the cell exists, but there are no multigateways in it.
func (ts *store) GetMultiGatewaysByCell(ctx context.Context, cellName string) ([]*MultiGatewayInfo, error) {
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

	mtgateways := make([]*MultiGatewayInfo, 0, len(listResults))
	for n := range listResults {
		multigateway := &clustermetadatapb.MultiGateway{}
		if err := proto.Unmarshal(listResults[n].Value, multigateway); err != nil {
			return nil, err
		}
		mtgateways = append(mtgateways, &MultiGatewayInfo{MultiGateway: multigateway, version: listResults[n].Version})
	}
	return mtgateways, nil
}

// UpdateMultiGateway updates the multigateway data only - not associated replication paths.
func (ts *store) UpdateMultiGateway(ctx context.Context, mgi *MultiGatewayInfo) error {
	conn, err := ts.ConnForCell(ctx, mgi.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mgi.MultiGateway)
	if err != nil {
		return err
	}
	gatewayPath := path.Join(GatewaysPath, MultiGatewayIDString(mgi.Id), GatewayFile)
	newVersion, err := conn.Update(ctx, gatewayPath, data, mgi.version)
	if err != nil {
		return err
	}
	mgi.version = newVersion

	return nil
}

// UpdateMultiGatewayFields is a high level helper to read a multigateway record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated multigateway.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts *store) UpdateMultiGatewayFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiGateway) error) (*clustermetadatapb.MultiGateway, error) {
	for {
		mgi, err := ts.GetMultiGateway(ctx, id)
		if err != nil {
			return nil, err
		}
		if err = update(mgi.MultiGateway); err != nil {
			if errors.Is(err, &TopoError{Code: NoUpdateNeeded}) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateMultiGateway(ctx, mgi); !errors.Is(err, &TopoError{Code: BadVersion}) {
			return mgi.MultiGateway, err
		}
	}
}

// CreateMultiGateway creates a new multigateway and all associated paths.
func (ts *store) CreateMultiGateway(ctx context.Context, mtgateway *clustermetadatapb.MultiGateway) error {
	conn, err := ts.ConnForCell(ctx, mtgateway.Id.Cell)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(mtgateway)
	if err != nil {
		return err
	}
	gatewayPath := path.Join(GatewaysPath, MultiGatewayIDString(mtgateway.Id), GatewayFile)
	if _, err := conn.Create(ctx, gatewayPath, data); err != nil {
		return err
	}

	return nil
}

// DeleteMultiGateway deletes the specified multigateway.
func (ts *store) DeleteMultiGateway(ctx context.Context, id *clustermetadatapb.ID) error {
	conn, err := ts.ConnForCell(ctx, id.Cell)
	if err != nil {
		return err
	}

	gatewayPath := path.Join(GatewaysPath, MultiGatewayIDString(id), GatewayFile)
	if err := conn.Delete(ctx, gatewayPath, nil); err != nil {
		return err
	}

	return nil
}

// InitMultiGateway creates or updates a multigateway. If allowUpdate is true,
// and a multigateway with the same ID exists, just update it.
func (ts *store) InitMultiGateway(ctx context.Context, mtgateway *clustermetadatapb.MultiGateway, allowUpdate bool) error {
	err := ts.CreateMultiGateway(ctx, mtgateway)
	if errors.Is(err, &TopoError{Code: NodeExists}) && allowUpdate {
		// Try to update then
		oldMtGateway, err := ts.GetMultiGateway(ctx, mtgateway.Id)
		if err != nil {
			return fmt.Errorf("failed reading existing mtgateway %v: %v", MultiGatewayIDString(mtgateway.Id), err)
		}

		oldMtGateway.MultiGateway = proto.Clone(mtgateway).(*clustermetadatapb.MultiGateway)
		if err := ts.UpdateMultiGateway(ctx, oldMtGateway); err != nil {
			return fmt.Errorf("failed updating mtgateway %v: %v", MultiGatewayIDString(mtgateway.Id), err)
		}
		return nil
	}
	return err
}
