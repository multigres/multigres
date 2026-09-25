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

package migration

import "context"

// The Coordinator collaborates with three side-effecting dependencies: the
// migration Store (state persistence), the target (local Postgres via the admin
// pool), and the source (the external old-database over the operator DSN). In
// production these are the concrete *Store, *target, and *source; the Coordinator
// holds them behind the interfaces below purely so unit tests can substitute
// in-memory/error-injecting fakes and drive the orchestration's error and
// crash-recovery branches without a live Postgres. The seams are internal wiring
// only — they do not affect the gRPC/RPC surface, NewCoordinator's signature, or
// any Coordinator method signature; the real types satisfy them unchanged (see the
// var _ assertions).

// migrationStore is the subset of *Store the Coordinator uses.
type migrationStore interface {
	EnsureSchema(ctx context.Context) error
	Insert(ctx context.Context, m *Migration) error
	Update(ctx context.Context, m *Migration) error
	Delete(ctx context.Context, id string) error
	GetByRef(ctx context.Context, ref string) (*Migration, error)
	List(ctx context.Context) ([]*Migration, error)
}

// migrationTarget is the subset of *target the Coordinator uses: the local
// (Multigres) Postgres side, driven through the admin pool.
type migrationTarget interface {
	ApplySchema(ctx context.Context, schemaSQL string) error
	DropTables(ctx context.Context, tables []string) error
	CreatePublication(ctx context.Context, name string, tables []string, migrationID string) error
	DropPublication(ctx context.Context, name string) error
	CreateSubscription(ctx context.Context, name, conninfo, publication string, copyData bool) error
	DropSubscription(ctx context.Context, name string) error
	AlterSubscriptionConnection(ctx context.Context, name, conninfo string) error
	SubscriptionStatus(ctx context.Context, name string) (*SubscriptionStatus, error)
	SubscriptionExists(ctx context.Context, name string) (bool, error)
	PublicationExists(ctx context.Context, name string) (bool, error)
	CurrentLSN(ctx context.Context) (string, error)
	SlotExists(ctx context.Context, name string) (bool, error)
	CreateLogicalSlot(ctx context.Context, name string) error
	AdvanceSlot(ctx context.Context, name, targetLSN string) error
	DropLogicalSlot(ctx context.Context, name string) error
	WaitSlotConfirmed(ctx context.Context, slot, targetLSN string) error
	ReplicationLag(ctx context.Context, slot string) (lagBytes uint64, lagSeconds float64, present bool, err error)
	AdvanceSequences(ctx context.Context, tables []string, margin int64) error
	ddlConn() ddlConn
}

// migrationSource is the subset of *source the Coordinator uses: the external
// old-database side, reached over the operator-supplied DSN. Its methods carry the
// operation context implicitly (cached on the source at open time), matching the
// concrete *source.
type migrationSource interface {
	Validate(patterns []string) (info *SourceInfo, resolved []string, warnings []string, err error)
	Info() (*SourceInfo, error)
	DumpSchema(tables []string) (string, error)
	CreatePublication(name string, tables []string, migrationID string) error
	DropPublication(name string) error
	CreateSubscription(name, conninfo, publication string, copyData bool, slotName string) error
	DropSubscription(name string) error
	SetReadOnly(ro bool) error
	CurrentLSN() (string, error)
	ReplicationLag(slot string) (lagBytes uint64, lagSeconds float64, present bool, err error)
	SubscriptionExists(name string) (bool, error)
	PublicationExists(name string) (bool, error)
	WaitSlotConfirmed(slot, targetLSN string) error
	AdvanceSequences(tables []string, margin int64) error
	close()
	ddlConn() ddlConn
}

// Compile-time assertions that the concrete production types satisfy the ports.
var (
	_ migrationStore  = (*Store)(nil)
	_ migrationTarget = (*target)(nil)
	_ migrationSource = (*source)(nil)
)
