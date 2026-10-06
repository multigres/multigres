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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager"
	"github.com/multigres/multigres/go/services/multipooler/internal/migration"
)

// migrationCoordinatorProvider is the subset of *manager.MultipoolerManager
// every handler in this file needs: obtaining the primary-gated migration
// coordinator. Narrowing the field to this interface (rather than the
// concrete manager) lets tests substitute a coordinator built directly via
// migration.NewCoordinator against a mock query service, instead of standing
// up a real multipooler manager and Postgres connection.
type migrationCoordinatorProvider interface {
	MigrationCoordinatorIfPrimary(ctx context.Context) (*migration.Coordinator, error)
}

type migrationService struct {
	migratorpb.UnimplementedMigratorServer
	manager migrationCoordinatorProvider
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
		ConnectionName:   req.GetMigration().GetConnectionName(),
		TargetDatabase:   req.GetMigration().GetTarget().GetDatabase(),
		TargetShard:      req.GetMigration().GetTarget().GetShard(),
		TargetTableGroup: req.GetMigration().GetTarget().GetTableGroup(),
		Name:             req.GetMigration().GetName(),
		Tables:           foldTableSelection(req.GetMigration().GetObjects()),
		CopyData:         !req.GetOptions().GetSkipCopyData(),
		SkipSchemaCopy:   req.GetOptions().GetSkipSchemaCopy(),
		SequenceMargin:   req.GetMigration().GetSequenceMargin(),
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.CreateMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

// foldTableSelection folds the structured selection (a table list, a schema
// list, or all tables owned by the source role) into the flat pattern list the
// coordinator resolves: "*" (all owned tables), "schema.*" (all owned tables in
// a schema), or "schema.table". A nil objects, or an "all" arm explicitly set to
// false, yields no patterns — CreateMigration then rejects the request for
// selecting no tables.
func foldTableSelection(objects *migratorpb.SelectionObject) []string {
	if objects == nil {
		return nil
	}
	switch o := objects.GetObject().(type) {
	case *migratorpb.SelectionObject_All:
		if !o.All {
			return nil
		}
		return []string{"*"}
	case *migratorpb.SelectionObject_Schema:
		patterns := make([]string, 0, len(o.Schema.GetSchemata()))
		for _, s := range o.Schema.GetSchemata() {
			patterns = append(patterns, s+".*")
		}
		return patterns
	case *migratorpb.SelectionObject_Table:
		return o.Table.GetQualifiedNames()
	default:
		return nil
	}
}

// toRef picks the addressing key from a request: the explicit id when set,
// else the name. The coordinator resolves id first, then a unique name. Used
// for both migrations and connections — migration.Ref is the shared
// addressing type for both.
func toRef(id int64, name string) migration.Ref {
	return migration.Ref{ID: id, Name: name}
}

func (s *migrationService) GetMigration(ctx context.Context, req *migratorpb.GetMigrationRequest) (*migratorpb.GetMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.GetMigration(ctx, toRef(req.GetRef().GetId(), req.GetRef().GetName()))
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.GetMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

// ListMigrations returns the ids of every migration. At most one migration
// exists at a time today (CreateMigration enforces it), so this returns 0 or
// 1 ids; callers fetch full details for each via GetMigration.
func (s *migrationService) ListMigrations(ctx context.Context, _ *migratorpb.ListMigrationsRequest) (*migratorpb.ListMigrationsResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	projs, err := coord.ListMigrations(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	ids := make([]int64, len(projs))
	for i, p := range projs {
		ids[i] = p.ID
	}
	return &migratorpb.ListMigrationsResponse{Ids: ids}, nil
}

func (s *migrationService) GetMigrationJournal(ctx context.Context, req *migratorpb.GetMigrationJournalRequest) (*migratorpb.GetMigrationJournalResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	entries, err := coord.GetMigrationJournal(ctx, toRef(req.Id, req.Name))
	if err != nil {
		return nil, toGRPC(err)
	}
	out := make([]*migratorpb.MigrationJournalEntry, len(entries))
	for i, e := range entries {
		out[i] = journalEntryToProto(e)
	}
	return &migratorpb.GetMigrationJournalResponse{Entries: out}, nil
}

func (s *migrationService) DropMigration(ctx context.Context, req *migratorpb.DropMigrationRequest) (*migratorpb.DropMigrationResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	proj, err := coord.DropMigration(ctx, toRef(req.GetRef().GetId(), req.GetRef().GetName()), migration.DropOptions{
		Wait:        req.Wait,
		WaitTimeout: time.Duration(req.WaitTimeoutSeconds) * time.Second,
		Force:       req.Force,
	})
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DropMigrationResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) SetMigrationDirection(ctx context.Context, req *migratorpb.SetMigrationDirectionRequest) (*migratorpb.SetMigrationDirectionResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	opts := migration.ActivateOptions{
		MaxLagBytes: req.GetMaxLagBytes(),
		WaitTimeout: time.Duration(req.GetWaitTimeoutSeconds()) * time.Second,
		CallerRole:  req.GetCallerRole(),
	}
	proj, err := coord.SetMigrationDirection(ctx, toRef(req.GetRef().GetId(), req.GetRef().GetName()), directionFromProto(req.GetDirection()), opts)
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.SetMigrationDirectionResponse{Migration: migToProto(proj), Status: statusToProto(proj)}, nil
}

func (s *migrationService) CreateConnection(ctx context.Context, req *migratorpb.CreateConnectionRequest) (*migratorpb.CreateConnectionResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	conn := &migration.Connection{
		Name: req.GetConnection().GetName(),
		DSN:  req.GetConnection().GetDsn(),
	}
	if err := coord.CreateConnection(ctx, conn); err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.CreateConnectionResponse{Connection: connToProto(conn)}, nil
}

func (s *migrationService) GetConnection(ctx context.Context, req *migratorpb.GetConnectionRequest) (*migratorpb.GetConnectionResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	conn, err := coord.GetConnection(ctx, toRef(req.GetRef().GetId(), req.GetRef().GetName()))
	if err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.GetConnectionResponse{Connection: connToProto(conn)}, nil
}

func (s *migrationService) ListConnections(ctx context.Context, _ *migratorpb.ListConnectionsRequest) (*migratorpb.ListConnectionsResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	conns, err := coord.ListConnections(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	out := make([]*migratorpb.Connection, len(conns))
	for i, c := range conns {
		out[i] = connToProto(c)
	}
	return &migratorpb.ListConnectionsResponse{Connections: out}, nil
}

func (s *migrationService) DropConnection(ctx context.Context, req *migratorpb.DropConnectionRequest) (*migratorpb.DropConnectionResponse, error) {
	coord, err := s.manager.MigrationCoordinatorIfPrimary(ctx)
	if err != nil {
		return nil, toGRPC(err)
	}
	conn, err := coord.GetConnection(ctx, toRef(req.GetRef().GetId(), req.GetRef().GetName()))
	if err != nil {
		if req.GetIfExists() && errors.Is(err, migration.ErrConnectionNotFound) {
			return &migratorpb.DropConnectionResponse{}, nil
		}
		return nil, toGRPC(err)
	}
	if err := coord.DropConnection(ctx, conn.ID); err != nil {
		return nil, toGRPC(err)
	}
	return &migratorpb.DropConnectionResponse{}, nil
}

func connToProto(c *migration.Connection) *migratorpb.Connection {
	return &migratorpb.Connection{
		Id:        c.ID,
		Name:      c.Name,
		Dsn:       c.DSN,
		CreatedAt: timestamppb.New(c.CreatedAt),
	}
}

// toGRPC maps coordinator errors to gRPC status errors.
func toGRPC(err error) error {
	if errors.Is(err, migration.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, migration.ErrConnectionNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, migration.ErrNotReady) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return mterrors.ToGRPC(err)
}

// migToProto maps a projection to its static Migration configuration. Objects
// reports the concrete tables resolved at create time (wildcards are already
// expanded), regardless of whether the request selected them by table list,
// schema list, or "all".
func migToProto(p *migration.Projection) *migratorpb.MigrationRecord {
	return &migratorpb.MigrationRecord{
		Id:             p.ID,
		Name:           p.Name,
		ConnectionName: p.ConnectionName,
		Target: &clustermetadatapb.ShardKey{
			Database:   p.TargetDatabase,
			TableGroup: p.TargetTableGroup,
			Shard:      p.TargetShard,
		},
		Objects:        tablesToSelectionObject(p.Tables),
		SequenceMargin: p.SequenceMargin,
	}
}

func tablesToSelectionObject(tables []string) *migratorpb.SelectionObject {
	if len(tables) == 0 {
		return nil
	}
	return &migratorpb.SelectionObject{
		Object: &migratorpb.SelectionObject_Table{
			Table: &migratorpb.TableSpec{QualifiedNames: tables},
		},
	}
}

// statusToProto maps a projection to its live MigrationStatus.
func statusToProto(p *migration.Projection) *migratorpb.MigrationStatus {
	s := &migratorpb.MigrationStatus{
		Id:               p.ID,
		Phase:            phaseToProto(p.Phase),
		LastError:        p.LastError,
		TotalRelations:   p.TotalRelations,
		ReadyRelations:   p.ReadyRelations,
		CaughtUp:         p.CaughtUp,
		PublicationName:  p.PublicationName,
		SubscriptionName: p.SubscriptionName,
		CreatedAt:        timestamppb.New(p.CreatedAt),
		ActiveDirection:  dirToProto(p.ActiveDirection),
		LagBytes:         p.LagBytes,
		LagSeconds:       p.LagSeconds,
	}
	if p.StreamingSince != nil {
		s.StreamingSince = timestamppb.New(*p.StreamingSince)
	}
	return s
}

// journalEntryToProto maps a coordinator journal entry to its proto form. The
// journal never contains credentials, so all fields pass through unredacted.
func journalEntryToProto(e *migration.JournalEntry) *migratorpb.MigrationJournalEntry {
	return &migratorpb.MigrationJournalEntry{
		Seq:           e.Seq,
		MigrationId:   e.MigrationID,
		MigrationName: e.MigrationName,
		Event:         eventToProto(e.Event),
		Phase:         phaseToProto(e.Phase),
		Direction:     dirToProto(e.Direction),
		FromLsn:       e.FromLSN,
		ToLsn:         e.ToLSN,
		LastError:     e.LastError,
		Detail:        e.Detail,
		CreatedAt:     timestamppb.New(e.CreatedAt),
	}
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

// directionFromProto is dirToProto's inverse, for SetMigrationDirectionRequest.
// An unspecified/unrecognized value maps to the zero Direction, which
// Coordinator.SetMigrationDirection rejects with "invalid direction" rather
// than silently defaulting to either side.
func directionFromProto(d migratorpb.MigrationDirection) migration.Direction {
	switch d {
	case migratorpb.MigrationDirection_MIGRATION_DIRECTION_IMPORT:
		return migration.DirectionImport
	case migratorpb.MigrationDirection_MIGRATION_DIRECTION_EXPORT:
		return migration.DirectionExport
	default:
		return ""
	}
}

func eventToProto(e migration.JournalEvent) migratorpb.MigrationEvent {
	switch e {
	case migration.JournalEventCreate:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_CREATE
	case migration.JournalEventStart:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_START
	case migration.JournalEventPhase:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_PHASE
	case migration.JournalEventActivate:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_ACTIVATE
	case migration.JournalEventDeactivate:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_DEACTIVATE
	case migration.JournalEventDrop:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_DROP
	case migration.JournalEventFailed:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_FAILED
	default:
		return migratorpb.MigrationEvent_MIGRATION_EVENT_UNSPECIFIED
	}
}
