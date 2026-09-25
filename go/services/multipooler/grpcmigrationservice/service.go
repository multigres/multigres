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
	// Reject options the backend cannot yet honor with a typed feature_not_supported
	// (SQLSTATE 0A000), so a caller shipping the full grammar surfaces an actionable
	// message rather than having the clause silently dropped.
	if req.SourcePublication != "" {
		return nil, toGRPC(mterrors.NewFeatureNotSupported(
			"migrator: source_publication (reusing a pre-created publication) is not yet supported"))
	}
	if req.PublishViaPartitionRoot {
		return nil, toGRPC(mterrors.NewFeatureNotSupported(
			"migrator: publish_via_partition_root is not yet supported"))
	}
	tables, err := foldTableSelection(req)
	if err != nil {
		return nil, toGRPC(err)
	}
	copyData := true // default when unset
	if req.CopyData != nil {
		copyData = *req.CopyData
	}
	proj, err := coord.CreateMigration(ctx, migration.CreateParams{
		SourceDSN:      req.SourceDsn,
		TargetDatabase: req.TargetDatabase,
		TargetShard:    req.TargetShard,
		Name:           req.Name,
		Tables:         tables,
		CopyData:       copyData,
		SkipSchemaCopy: req.SkipSchemaCopy,
		SequenceMargin: req.SequenceMargin,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.CreateMigrationResponse{Migration: projToProto(proj)}, nil
}

// foldTableSelection folds the structured selection (all_tables plus the
// heterogeneous objects list) into the flat pattern list the coordinator
// resolves: "*" (all owned tables), "schema.*" (all owned tables in a schema),
// or "schema.table". all_tables and objects are unioned (the resolver
// de-duplicates). A table object carrying a column list, WHERE filter, or
// include_descendants is rejected with a typed feature_not_supported error
// (those clauses are not yet backed).
func foldTableSelection(req *migratorpb.CreateMigrationRequest) ([]string, error) {
	var patterns []string
	if req.AllTables {
		patterns = append(patterns, "*")
	}
	for _, obj := range req.Objects {
		switch o := obj.GetObject().(type) {
		case *migratorpb.SelectionObject_Schema:
			patterns = append(patterns, o.Schema+".*")
		case *migratorpb.SelectionObject_Table:
			ts := o.Table
			if len(ts.Columns) > 0 || ts.Where != "" || ts.IncludeDescendants {
				return nil, mterrors.NewFeatureNotSupported(
					"migrator: per-table column lists, WHERE row filters, and include_descendants are not yet supported")
			}
			patterns = append(patterns, ts.QualifiedName)
		default:
			return nil, mterrors.NewFeatureNotSupported(
				"migrator: a selection object must set either a table or a schema")
		}
	}
	return patterns, nil
}

// migrationRef picks the addressing key from a request: the explicit id when
// set, else the name. The coordinator resolves id first, then a unique name.
func migrationRef(id, name string) string {
	if id != "" {
		return id
	}
	return name
}

func (s *migrationService) StartMigration(ctx context.Context, req *migratorpb.StartMigrationRequest) (*migratorpb.StartMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.StartMigration(ctx, migrationRef(req.Id, req.Name))
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
	if ref := migrationRef(req.Id, req.Name); ref != "" {
		proj, err := coord.GetMigration(ctx, ref)
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
	proj, err := coord.DropMigration(ctx, migrationRef(req.Id, req.Name), migration.DropOptions{
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
	proj, err := coord.UpdateMigration(ctx, migrationRef(req.Id, req.Name), p)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.UpdateMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) ActivateMigration(ctx context.Context, req *migratorpb.ActivateMigrationRequest) (*migratorpb.ActivateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	opts := migration.ActivateOptions{
		MaxLagBytes: req.MaxLagBytes,
		WaitTimeout: time.Duration(req.WaitTimeoutSeconds) * time.Second,
	}
	proj, err := coord.Activate(ctx, migrationRef(req.Id, req.Name), opts)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.ActivateMigrationResponse{Migration: projToProto(proj)}, nil
}

func (s *migrationService) DeactivateMigration(ctx context.Context, req *migratorpb.DeactivateMigrationRequest) (*migratorpb.DeactivateMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.Deactivate(ctx, migrationRef(req.Id, req.Name))
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DeactivateMigrationResponse{Migration: projToProto(proj)}, nil
}

// toGRPC maps coordinator errors to gRPC status errors.
func toGRPC(err error) error {
	if errors.Is(err, migration.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, migration.ErrNotReady) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return mterrors.ToGRPC(err)
}

func projToProto(p *migration.Projection) *migratorpb.Migration {
	m := &migratorpb.Migration{
		Id:               p.ID,
		Name:             p.Name,
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
		LagBytes:         p.LagBytes,
		LagSeconds:       p.LagSeconds,
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
	case migration.PhaseImporting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_IMPORTING
	case migration.PhaseExporting:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_EXPORTING
	case migration.PhaseSwitchingToImport:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_IMPORT
	case migration.PhaseSwitchingToExport:
		return migratorpb.MigrationPhase_MIGRATION_PHASE_SWITCHING_TO_EXPORT
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
