// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package grpcmigrationservice serves the Multigres Migrator migration RPCs on the
// multipooler. The handlers are primary-gated (via MigrationCoordinatorIfPrimary)
// and delegate to the co-located migration coordinator; multiadmin forwards
// operator commands here after resolving the shard's current primary.
package grpcmigrationservice

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

type migrationService struct {
	migratorpb.UnimplementedMigratorServer
	manager *manager.MultipoolerManager
}

// RegisterMigrationServices wires the migration gRPC service into the pooler's
// gRPC server and opts the manager into the migration coordinator (reconcile
// poller) when the "migration" service is enabled in the service map.
func RegisterMigrationServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	manager.RegisterPoolerManagerServices = append(manager.RegisterPoolerManagerServices, func(pm *manager.MultipoolerManager) {
		if grpc.CheckServiceMap("migration", senv) {
			pm.StartMigrationCoordinator()
			migratorpb.RegisterMigratorServer(grpc.Server, &migrationService{manager: pm})
		}
	})
}

func (s *migrationService) CreateMigration(ctx context.Context, req *migratorpb.CreateMigrationRequest) (*migratorpb.CreateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.CreateMigration(ctx, migration.CreateParams{
		SourceDSN:      req.SourceDsn,
		TargetDatabase: req.TargetDatabase,
		TargetShard:    req.TargetShard,
		Tables:         req.Tables,
		SequenceMargin: req.SequenceMargin,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.CreateMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) StartMigration(ctx context.Context, req *migratorpb.StartMigrationRequest) (*migratorpb.StartMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.StartMigration(ctx, req.Id)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.StartMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) GetMigrations(ctx context.Context, req *migratorpb.GetMigrationsRequest) (*migratorpb.GetMigrationsResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	if req.Id != "" {
		proj, err := coord.GetMigration(ctx, req.Id)
		if err != nil {
			return nil, toGRPC(err)
		}
		return &migratorpb.GetMigrationsResponse{Migrations: []*migratorpb.Migration{projToProto(proj)}}, nil
	}
	projs, err := coord.ListMigrations(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	out := make([]*migratorpb.Migration, len(projs))
	for i, p := range projs {
		out[i] = projToProto(p)
	}
	return &migratorpb.GetMigrationsResponse{Migrations: out}, nil
}

func (s *migrationService) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.DropMigration(ctx, req.Id, migration.DropOptions{
		Wait:        req.Wait,
		WaitTimeout: time.Duration(req.WaitTimeoutSeconds) * time.Second,
		Force:       req.Force,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DropMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) UpdateMigration(ctx context.Context, req *migratorpb.UpdateMigrationRequest) (*migratorpb.UpdateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	if req.UpdateMask == nil || len(req.UpdateMask.Paths) == 0 {
		return nil, status.Error(codes.InvalidArgument, "update_mask is required")
	}
	var p migration.UpdateParams
	for _, path := range req.UpdateMask.Paths {
		switch path {
		case "source_dsn":
			v := req.SourceDsn
			p.SourceDSN = &v
		case "sequence_margin":
			v := req.SequenceMargin
			p.SequenceMargin = &v
		case "tables":
			v := req.Tables
			p.Tables = &v
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported update_mask path %q", path)
		}
	}
	proj, err := coord.UpdateMigration(ctx, req.Id, p)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.UpdateMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	var dir migration.Direction
	switch req.Direction {
	case migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT:
		dir = migration.DirectionImport
	case migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT:
		dir = migration.DirectionExport
	default:
		return nil, status.Error(codes.InvalidArgument, "direction must be IMPORT or EXPORT")
	}
	proj, err := coord.SetMigrationDirection(ctx, req.Id, dir)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.SetMigrationDirectionResponse{Migration: projToProto(proj)}, nil
}

// toGRPC maps coordinator errors to gRPC status errors.
func toGRPC(err error) error {
	if errors.Is(err, migration.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return mterrors.ToGRPC(err)
}

func projToProto(p *migration.Projection) *migratorpb.Migration {
	m := &migratorpb.Migration{
		Id:               p.ID,
		Source:           p.Source,
		TargetDatabase:   p.TargetDatabase,
		TargetShard:      p.TargetShard,
		Tables:           p.Tables,
		Phase:            phaseToProto(p.Phase),
		ActiveDirection:  dirToProto(p.ActiveDirection),
		LastError:        p.LastError,
		TotalRelations:   p.TotalRelations,
		ReadyRelations:   p.ReadyRelations,
		CaughtUp:         p.CaughtUp,
		PublicationName:  p.PublicationName,
		SubscriptionName: p.SubscriptionName,
		CreatedAt:        timestamppb.New(p.CreatedAt),
	}
	if p.StreamingSince != nil {
		m.StreamingSince = timestamppb.New(*p.StreamingSince)
	}
	return m
}

func phaseToProto(p migration.Phase) migratorpb.MigrationPhase {
	switch p {
	case migration.PhaseCreated:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_CREATED
	case migration.PhaseValidating:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_VALIDATING
	case migration.PhaseSchemaCopy:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SCHEMA_COPY
	case migration.PhaseCreatePublication:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_CREATE_PUBLICATION
	case migration.PhaseCopying:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_COPYING
	case migration.PhaseStreaming:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_STREAMING
	case migration.PhaseSwitching:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING
	case migration.PhaseCompleting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_COMPLETING
	case migration.PhaseFailed:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_FAILED
	default:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_UNSPECIFIED
	}
}

func dirToProto(d migration.Direction) migratorpb.MigrationDirection {
	switch d {
	case migration.DirectionImport:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT
	case migration.DirectionExport:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT
	default:
		return migratorpb.MigrationDirection_MIGRATION_DIRECTION_UNSPECIFIED
	}
}
