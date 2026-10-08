// Copyright 2026 Supabase, Inc.
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

package multiadmin

import (
	"context"
	"errors"
	"net/url"
	"path"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
)

var topologyName = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]*$`)

func validCellName(name string) bool {
	return topologyName.MatchString(name) && name != topoclient.GlobalCell
}

func (s *MultiadminServer) CreateCell(ctx context.Context, req *multiadminpb.CreateCellRequest) (*multiadminpb.CreateCellResponse, error) {
	cell := req.GetCell()
	if cell == nil || !validCellName(cell.Name) || len(cell.ServerAddresses) == 0 || !path.IsAbs(cell.Root) || path.Clean(cell.Root) != cell.Root {
		return nil, status.Error(codes.InvalidArgument, "cell requires a valid name, topology addresses, and an absolute clean root")
	}
	for _, address := range cell.ServerAddresses {
		if strings.TrimSpace(address) == "" {
			return nil, status.Error(codes.InvalidArgument, "topology addresses must not be empty")
		}
	}
	err := s.ts.CreateCell(ctx, cell.Name, cell)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
		existing, readErr := s.ts.GetCell(ctx, cell.Name)
		if readErr != nil {
			return nil, managementTopologyError(readErr)
		}
		if existing.Name == "" {
			existing.Name = cell.Name
		}
		if !proto.Equal(existing, cell) {
			return nil, status.Error(codes.AlreadyExists, "cell exists with different configuration")
		}
		return &multiadminpb.CreateCellResponse{Cell: existing}, nil
	}
	if err != nil {
		return nil, managementTopologyError(err)
	}
	return &multiadminpb.CreateCellResponse{Cell: proto.Clone(cell).(*clustermetadatapb.Cell)}, nil
}

func (s *MultiadminServer) CreateDatabase(ctx context.Context, req *multiadminpb.CreateDatabaseRequest) (*multiadminpb.CreateDatabaseResponse, error) {
	database := req.GetDatabase()
	if database == nil || !topologyName.MatchString(database.Name) || len(database.Cells) == 0 {
		return nil, status.Error(codes.InvalidArgument, "database requires a valid name and at least one cell")
	}
	if _, err := consensus.NewPolicyFromProto(database.BootstrapDurabilityPolicy); err != nil {
		return nil, status.Error(codes.InvalidArgument, "a supported bootstrap durability policy is required")
	}
	if err := validateInitialBackupLocation(database.BackupLocation); err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, cell := range database.Cells {
		if !validCellName(cell) || seen[cell] {
			return nil, status.Error(codes.InvalidArgument, "database cells must be valid and unique")
		}
		seen[cell] = true
		if _, err := s.ts.GetCell(ctx, cell); err != nil {
			if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
				return nil, status.Error(codes.FailedPrecondition, "a referenced cell does not exist")
			}
			return nil, managementTopologyError(err)
		}
	}
	err := s.ts.CreateDatabase(ctx, database.Name, database)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
		existing, readErr := s.ts.GetDatabase(ctx, database.Name)
		if readErr != nil {
			return nil, managementTopologyError(readErr)
		}
		if existing.Name == "" {
			existing.Name = database.Name
		}
		if !proto.Equal(existing, database) {
			return nil, status.Error(codes.AlreadyExists, "database exists with different configuration")
		}
		return &multiadminpb.CreateDatabaseResponse{Database: existing}, nil
	}
	if err != nil {
		return nil, managementTopologyError(err)
	}
	return &multiadminpb.CreateDatabaseResponse{Database: proto.Clone(database).(*clustermetadatapb.Database)}, nil
}

func validateInitialBackupLocation(location *clustermetadatapb.BackupLocation) error {
	if location == nil {
		return nil
	}
	// Later repository generations are selected by the running shard's repository catalog.
	if location.AuthoritativeGeneration < 0 || location.AuthoritativeGeneration > 1 {
		return status.Error(codes.InvalidArgument, "initial backup repository generation must be zero or one")
	}
	switch config := location.Location.(type) {
	case *clustermetadatapb.BackupLocation_Filesystem:
		if config.Filesystem != nil && path.IsAbs(config.Filesystem.Path) {
			return nil
		}
	case *clustermetadatapb.BackupLocation_S3:
		if config.S3 != nil && strings.TrimSpace(config.S3.Bucket) != "" && strings.TrimSpace(config.S3.Region) != "" {
			if config.S3.Endpoint == "" {
				return nil
			}
			endpoint, err := url.Parse(config.S3.Endpoint)
			if err == nil && (endpoint.Scheme == "http" || endpoint.Scheme == "https") && endpoint.Host != "" && endpoint.User == nil {
				return nil
			}
		}
	}
	return status.Error(codes.InvalidArgument, "backup location requires an absolute filesystem path or an S3 bucket, region, and valid optional HTTP endpoint")
}

func managementPoolerID(id *clustermetadatapb.ID) (*clustermetadatapb.ID, error) {
	if id == nil || !validCellName(id.Cell) || !topologyName.MatchString(id.Name) || (id.Component != clustermetadatapb.ID_UNKNOWN && id.Component != clustermetadatapb.ID_MULTIPOOLER) {
		return nil, status.Error(codes.InvalidArgument, "a multipooler cell and name are required")
	}
	result := proto.Clone(id).(*clustermetadatapb.ID)
	result.Component = clustermetadatapb.ID_MULTIPOOLER
	return result, nil
}

func (s *MultiadminServer) registrationConnection(ctx context.Context, id *clustermetadatapb.ID) (topoclient.Conn, error) {
	conn, err := s.ts.ConnForCell(ctx, id.Cell)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return nil, status.Error(codes.FailedPrecondition, "cell configuration is missing; registration absence cannot be established")
	}
	if err != nil {
		return nil, managementTopologyError(err)
	}
	return conn, nil
}

func (s *MultiadminServer) GetPoolerRegistration(ctx context.Context, req *multiadminpb.GetPoolerRegistrationRequest) (*multiadminpb.GetPoolerRegistrationResponse, error) {
	id, err := managementPoolerID(req.GetPoolerId())
	if err != nil {
		return nil, err
	}
	conn, err := s.registrationConnection(ctx, id)
	if err != nil {
		return nil, err
	}
	info, err := topoclient.GetMultipoolerFromConn(ctx, conn, id)
	if err != nil {
		return nil, managementTopologyError(err)
	}
	if info.Version() == nil {
		return nil, status.Error(codes.Internal, "topology returned a registration without a version")
	}
	return &multiadminpb.GetPoolerRegistrationResponse{Pooler: info.Multipooler, Version: info.Version().String()}, nil
}

func (s *MultiadminServer) RetirePooler(ctx context.Context, req *multiadminpb.RetirePoolerRequest) (*multiadminpb.RetirePoolerResponse, error) {
	id, err := managementPoolerID(req.GetPoolerId())
	if err != nil {
		return nil, err
	}
	shard := req.GetShardKey()
	if shard == nil || !topologyName.MatchString(shard.Database) || !topologyName.MatchString(shard.TableGroup) || strings.TrimSpace(shard.Shard) == "" || strings.TrimSpace(req.GetIncarnationId()) == "" || strings.TrimSpace(req.GetVersion()) == "" {
		return nil, status.Error(codes.InvalidArgument, "shard, incarnation ID, and registration version are required")
	}
	if !req.FencingAcknowledged {
		return nil, status.Error(codes.FailedPrecondition, "positive infrastructure fencing must be acknowledged")
	}
	conn, err := s.registrationConnection(ctx, id)
	if err != nil {
		return nil, err
	}
	info, err := topoclient.GetMultipoolerFromConn(ctx, conn, id)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return &multiadminpb.RetirePoolerResponse{}, nil
	}
	if err != nil {
		return nil, managementTopologyError(err)
	}
	if info.IncarnationId == "" {
		return nil, status.Error(codes.FailedPrecondition, "registration has no process incarnation ID; upgrade the member before using conditional retirement")
	}
	if info.Version() == nil {
		return nil, status.Error(codes.Internal, "topology returned a registration without a version")
	}
	if !proto.Equal(info.Id, id) || !proto.Equal(info.ShardKey, shard) || info.IncarnationId != req.IncarnationId || info.Version().String() != req.Version {
		return nil, status.Error(codes.Aborted, "registration identity, shard, incarnation, or version changed")
	}
	// Use the same connection for the read and conditional delete, even if cell configuration changes.
	err = topoclient.DeleteMultipoolerFromConn(ctx, conn, id, info.Version())
	if err != nil && !errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return nil, managementTopologyError(err)
	}
	return &multiadminpb.RetirePoolerResponse{}, nil
}

func managementTopologyError(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, &topoclient.TopoError{Code: topoclient.Timeout}):
		return status.Error(codes.DeadlineExceeded, "topology deadline exceeded; mutation outcome may be uncertain")
	case errors.Is(err, context.Canceled), errors.Is(err, &topoclient.TopoError{Code: topoclient.Interrupted}):
		return status.Error(codes.Canceled, "topology operation canceled; mutation outcome may be uncertain")
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}):
		return status.Error(codes.NotFound, "topology record not found")
	case errors.Is(err, &topoclient.TopoError{Code: topoclient.BadVersion}):
		return status.Error(codes.Aborted, "registration changed before deletion")
	default:
		if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
			return st.Err()
		}
		return status.Error(codes.Unavailable, "topology operation failed; mutation outcome may be uncertain")
	}
}
