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

	"connectrpc.com/connect"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiadminconnect "github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
)

// connectAdapter wraps MultiAdminServer to implement the connect-go handler interface.
// Each method unwraps the connect.Request envelope, delegates to MultiAdminServer, and wraps the response.
type connectAdapter struct {
	*MultiAdminServer
}

// Compile-time check that connectAdapter implements the connect handler interface.
var _ multiadminconnect.MultiAdminServiceHandler = (*connectAdapter)(nil)

func (a *connectAdapter) GetCell(ctx context.Context, req *connect.Request[multiadminpb.GetCellRequest]) (*connect.Response[multiadminpb.GetCellResponse], error) {
	resp, err := a.MultiAdminServer.GetCell(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetDatabase(ctx context.Context, req *connect.Request[multiadminpb.GetDatabaseRequest]) (*connect.Response[multiadminpb.GetDatabaseResponse], error) {
	resp, err := a.MultiAdminServer.GetDatabase(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetCellNames(ctx context.Context, req *connect.Request[multiadminpb.GetCellNamesRequest]) (*connect.Response[multiadminpb.GetCellNamesResponse], error) {
	resp, err := a.MultiAdminServer.GetCellNames(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetDatabaseNames(ctx context.Context, req *connect.Request[multiadminpb.GetDatabaseNamesRequest]) (*connect.Response[multiadminpb.GetDatabaseNamesResponse], error) {
	resp, err := a.MultiAdminServer.GetDatabaseNames(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetGateways(ctx context.Context, req *connect.Request[multiadminpb.GetGatewaysRequest]) (*connect.Response[multiadminpb.GetGatewaysResponse], error) {
	resp, err := a.MultiAdminServer.GetGateways(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetPoolers(ctx context.Context, req *connect.Request[multiadminpb.GetPoolersRequest]) (*connect.Response[multiadminpb.GetPoolersResponse], error) {
	resp, err := a.MultiAdminServer.GetPoolers(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetOrchs(ctx context.Context, req *connect.Request[multiadminpb.GetOrchsRequest]) (*connect.Response[multiadminpb.GetOrchsResponse], error) {
	resp, err := a.MultiAdminServer.GetOrchs(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) Backup(ctx context.Context, req *connect.Request[multiadminpb.BackupRequest]) (*connect.Response[multiadminpb.BackupResponse], error) {
	resp, err := a.MultiAdminServer.Backup(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) RestoreFromBackup(ctx context.Context, req *connect.Request[multiadminpb.RestoreFromBackupRequest]) (*connect.Response[multiadminpb.RestoreFromBackupResponse], error) {
	resp, err := a.MultiAdminServer.RestoreFromBackup(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetBackupJobStatus(ctx context.Context, req *connect.Request[multiadminpb.GetBackupJobStatusRequest]) (*connect.Response[multiadminpb.GetBackupJobStatusResponse], error) {
	resp, err := a.MultiAdminServer.GetBackupJobStatus(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetBackups(ctx context.Context, req *connect.Request[multiadminpb.GetBackupsRequest]) (*connect.Response[multiadminpb.GetBackupsResponse], error) {
	resp, err := a.MultiAdminServer.GetBackups(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) ExpireBackups(ctx context.Context, req *connect.Request[multiadminpb.ExpireBackupsRequest]) (*connect.Response[multiadminpb.ExpireBackupsResponse], error) {
	resp, err := a.MultiAdminServer.ExpireBackups(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) VerifyBackups(ctx context.Context, req *connect.Request[multiadminpb.VerifyBackupsRequest]) (*connect.Response[multiadminpb.VerifyBackupsResponse], error) {
	resp, err := a.MultiAdminServer.VerifyBackups(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetPoolerStatus(ctx context.Context, req *connect.Request[multiadminpb.GetPoolerStatusRequest]) (*connect.Response[multiadminpb.GetPoolerStatusResponse], error) {
	resp, err := a.MultiAdminServer.GetPoolerStatus(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) SetPostgresRestartsEnabled(ctx context.Context, req *connect.Request[multiadminpb.SetPostgresRestartsEnabledRequest]) (*connect.Response[multiadminpb.SetPostgresRestartsEnabledResponse], error) {
	resp, err := a.MultiAdminServer.SetPostgresRestartsEnabled(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetGatewayQueries(ctx context.Context, req *connect.Request[multiadminpb.GetGatewayQueriesRequest]) (*connect.Response[multiadminpb.GetGatewayQueriesResponse], error) {
	resp, err := a.MultiAdminServer.GetGatewayQueries(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) GetGatewayConsolidator(ctx context.Context, req *connect.Request[multiadminpb.GetGatewayConsolidatorRequest]) (*connect.Response[multiadminpb.GetGatewayConsolidatorResponse], error) {
	resp, err := a.MultiAdminServer.GetGatewayConsolidator(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

func (a *connectAdapter) ApplyCertifiedRuleChange(ctx context.Context, req *connect.Request[multiadminpb.ApplyCertifiedRuleChangeRequest]) (*connect.Response[multiadminpb.ApplyCertifiedRuleChangeResponse], error) {
	resp, err := a.MultiAdminServer.ApplyCertifiedRuleChange(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}
