// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolerserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/admin"
)

// GetAuthCredentials retrieves authentication credentials (SCRAM hash) for a
// PostgreSQL user, for the gateway to authenticate clients with SCRAM-SHA-256,
// whatever the transport: the gRPC handler and, in minigres, the gateway's
// direct calls.
//
// It uses an admin connection directly, since normally a non-superuser wouldn't
// have access to password hashes and at the time of this request we wouldn't
// have authenticated that we have permission to run queries under any other
// user's role.
//
// It validates the request before anything else, even on a nil server, which
// it reports as not initialized.
func (s *QueryPoolerServer) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	// Validate request.
	if req.Username == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "username is required")
	}
	if req.Database == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "database is required")
	}
	if s == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pooler not initialized")
	}

	// Admin credential fetch (not a query). Treat like a new reservation so it is
	// buffered during any drain — the same behavior as before the two-stage drain.
	if err := s.StartRequest(nil, RequestNewReservation); err != nil {
		return nil, err
	}

	poolManager := s.PoolManager()
	if poolManager == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "pool manager not initialized")
	}

	// Time the full credential-query path (admin acquire + pg_authid
	// lookup + decode) for mg.pooler.auth.credential_query.duration so
	// admin-pool contention shows up before it cascades into
	// gateway-visible auth latency. The duration is recorded on every
	// exit, error or not; the error counter only fires on failures so
	// the success rate is implicit.
	recorder := poolManager.CredentialQueryRecorder()
	start := time.Now()
	var errorType string
	defer func() {
		if recorder != nil {
			recorder.RecordCredentialQuery(ctx, time.Since(start), errorType)
		}
	}()

	// An admin connection:
	// - has permission to read password hashes
	// - also avoids a chicken-egg scenario of needing to create and use a role-specific connection
	//   to figure out if the caller should have access to that role-specific connection.
	conn, err := poolManager.GetAdminConn(ctx)
	if err != nil {
		errorType = connpoolmanager.CredentialQueryErrorPoolAcquireFailed
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "failed to get admin connection: %v", err)
	}
	defer conn.Recycle()

	// Get the role auth info (password hash + rolreplication) using the admin
	// connection. This queries pg_authid, which requires superuser access.
	authInfo, err := conn.Conn.GetRolAuthInfo(ctx, req.Username)
	if err != nil {
		switch {
		case errors.Is(err, admin.ErrUserNotFound):
			errorType = connpoolmanager.CredentialQueryErrorUserNotFound
			return nil, mterrors.Errorf(mtrpcpb.Code_NOT_FOUND, "user %q not found", req.Username)
		case errors.Is(err, admin.ErrLoginDisabled):
			// Emit as a PgDiagnostic so the SQLSTATE (28000) is the
			// distinguishing signal at the gateway, not the gRPC code.
			// gRPC auth interceptors use codes.PermissionDenied /
			// codes.Unauthenticated for transport failures; keying on code
			// alone would misclassify an mTLS or authz error as an app-level
			// "role not permitted to log in" rejection to the end user.
			errorType = connpoolmanager.CredentialQueryErrorLoginDisabled
			return nil, mterrors.NewPgError(
				"FATAL", mterrors.PgSSInvalidAuthSpec,
				fmt.Sprintf("role %q is not permitted to log in", req.Username),
				"",
			)
		case errors.Is(err, admin.ErrPasswordExpired):
			// SQLSTATE 28P01 matches PG's opaque "password authentication
			// failed" error for expired passwords. PgDiagnostic detail
			// survives the gRPC round trip and the gateway matches on it.
			errorType = connpoolmanager.CredentialQueryErrorPasswordExpired
			return nil, mterrors.NewPgError(
				"FATAL", mterrors.PgSSAuthFailed,
				fmt.Sprintf("password authentication failed for user %q", req.Username),
				"",
			)
		default:
			// Genuine DB-level failure (SQL error, unexpected result shape,
			// connection drop mid-query). Tagged db_error so operators can
			// alert on it without false positives from the user_not_found
			// baseline.
			errorType = connpoolmanager.CredentialQueryErrorDB
			return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL, "failed to get role password: %v", err)
		}
	}

	return &multipoolerpb.GetAuthCredentialsResponse{
		ScramHash:         authInfo.ScramHash,
		IsReplicationRole: authInfo.IsReplicationRole,
	}, nil
}
