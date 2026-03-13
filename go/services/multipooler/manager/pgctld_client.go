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

package manager

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// protectedPgctldClient wraps a pgctld client and enforces action lock requirements
// for state-changing operations. This prevents concurrent modifications to postgres
// state (e.g., monitor restarting postgres while a manual operation is in progress).
//
// State-changing operations (Start, Stop, Restart, InitDataDir, PgRewind, CrashRecovery, ReloadConfig)
// require the caller to hold the action lock. Read-only operations (Status, Version)
// can be called without the lock.
type protectedPgctldClient struct {
	client pgctldpb.PgCtldClient
}

// Compile-time assertion that protectedPgctldClient implements pgctldpb.PgCtldClient
var _ pgctldpb.PgCtldClient = (*protectedPgctldClient)(nil)

// NewProtectedPgctldClient creates a new protected pgctld client wrapper.
func NewProtectedPgctldClient(client pgctldpb.PgCtldClient) pgctldpb.PgCtldClient {
	return &protectedPgctldClient{
		client: client,
	}
}

// Start starts PostgreSQL. Requires action lock to be held by caller.
func (p *protectedPgctldClient) Start(ctx context.Context, req *pgctldpb.StartRequest, opts ...grpc.CallOption) (*pgctldpb.StartResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("Start requires action lock to be held: %w", err)
	}
	return p.client.Start(ctx, req, opts...)
}

// Stop stops PostgreSQL. Requires action lock to be held by caller.
func (p *protectedPgctldClient) Stop(ctx context.Context, req *pgctldpb.StopRequest, opts ...grpc.CallOption) (*pgctldpb.StopResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("Stop requires action lock to be held: %w", err)
	}
	return p.client.Stop(ctx, req, opts...)
}

// Restart restarts PostgreSQL. Requires action lock to be held by caller.
func (p *protectedPgctldClient) Restart(ctx context.Context, req *pgctldpb.RestartRequest, opts ...grpc.CallOption) (*pgctldpb.RestartResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("Restart requires action lock to be held: %w", err)
	}
	return p.client.Restart(ctx, req, opts...)
}

// ReloadConfig reloads PostgreSQL configuration. Requires action lock to be held by caller.
func (p *protectedPgctldClient) ReloadConfig(ctx context.Context, req *pgctldpb.ReloadConfigRequest, opts ...grpc.CallOption) (*pgctldpb.ReloadConfigResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("ReloadConfig requires action lock to be held: %w", err)
	}
	return p.client.ReloadConfig(ctx, req, opts...)
}

// InitDataDir initializes the PostgreSQL data directory. Requires action lock to be held by caller.
func (p *protectedPgctldClient) InitDataDir(ctx context.Context, req *pgctldpb.InitDataDirRequest, opts ...grpc.CallOption) (*pgctldpb.InitDataDirResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("InitDataDir requires action lock to be held: %w", err)
	}
	return p.client.InitDataDir(ctx, req, opts...)
}

// PgRewind rewinds the PostgreSQL data directory. Requires action lock to be held by caller.
func (p *protectedPgctldClient) PgRewind(ctx context.Context, req *pgctldpb.PgRewindRequest, opts ...grpc.CallOption) (*pgctldpb.PgRewindResponse, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, fmt.Errorf("PgRewind requires action lock to be held: %w", err)
	}
	return p.client.PgRewind(ctx, req, opts...)
}

// Status returns PostgreSQL status. Does not require action lock (read-only operation).
func (p *protectedPgctldClient) Status(ctx context.Context, req *pgctldpb.StatusRequest, opts ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	return p.client.Status(ctx, req, opts...)
}

// Version returns PostgreSQL version information. Does not require action lock (read-only operation).
func (p *protectedPgctldClient) Version(ctx context.Context, req *pgctldpb.VersionRequest, opts ...grpc.CallOption) (*pgctldpb.VersionResponse, error) {
	return p.client.Version(ctx, req, opts...)
}
