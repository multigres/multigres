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

package grpcpoolerservice

import (
	"context"
	"errors"

	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
)

// serveControl runs one call of one of the manager's control-plane services and converts its
// error for gRPC. An error that carries a gRPC status with details is passed through as it is; any
// other keeps the message context it was wrapped in.
func serveControl[P, Req, Resp any](
	ctx context.Context,
	provider func() (P, error),
	req Req,
	call func(P, context.Context, Req) (Resp, error),
) (Resp, error) {
	var zero Resp
	p, err := provider()
	if err != nil {
		return zero, mterrors.ToGRPC(err)
	}
	resp, err := call(p, ctx, req)
	if err != nil {
		var withStatus interface{ GRPCStatus() *status.Status }
		if errors.As(err, &withStatus) {
			if st := withStatus.GRPCStatus(); len(st.Details()) > 0 {
				return zero, st.Err()
			}
		}
		return zero, mterrors.ToGRPC(err)
	}
	return resp, nil
}

// serveMetadata runs one call of the manager's metadata service.
func serveMetadata[Req, Resp any](ctx context.Context, s *poolerService, req Req, call func(poolerserver.MetadataProvider, context.Context, Req) (Resp, error)) (Resp, error) {
	return serveControl(ctx, s.pooler.MetadataProvider, req, call)
}

// GetBackingConnection returns a named backing connection. Only the default
// primary serves it; the manager enforces that.
func (s *poolerService) GetBackingConnection(ctx context.Context, req *multipoolerpb.GetBackingConnectionRequest) (*multipoolerpb.GetBackingConnectionResponse, error) {
	return serveMetadata(ctx, s, req, poolerserver.MetadataProvider.GetBackingConnection)
}
