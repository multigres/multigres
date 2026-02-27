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

package multigateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/pgprotocol/cancelkey"
	"github.com/multigres/multigres/go/common/topoclient"
	multigatewayservicepb "github.com/multigres/multigres/go/pb/multigatewayservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// CancelManager handles cross-gateway query cancellation.
// It implements both server.CancelHandler (for incoming PostgreSQL cancel requests)
// and MultiGatewayServiceServer (for gRPC forwarded cancel requests).
type CancelManager struct {
	multigatewayservicepb.UnimplementedMultiGatewayServiceServer

	// localCancelFn attempts to cancel a connection on this gateway.
	localCancelFn func(pid, secret uint32) bool
	// ownPrefix is this gateway's PID prefix.
	ownPrefix uint32
	// ts is the topology store for discovering other gateways.
	ts topoclient.Store
	// logger for cancel operations.
	logger *slog.Logger

	// clientsMu protects clients.
	clientsMu sync.Mutex
	// clients caches gRPC clients by address.
	clients map[string]multigatewayservicepb.MultiGatewayServiceClient
}

// NewCancelManager creates a new CancelManager.
func NewCancelManager(
	localCancelFn func(pid, secret uint32) bool,
	ownPrefix uint32,
	ts topoclient.Store,
	logger *slog.Logger,
) *CancelManager {
	return &CancelManager{
		localCancelFn: localCancelFn,
		ownPrefix:     ownPrefix,
		ts:            ts,
		logger:        logger,
		clients:       make(map[string]multigatewayservicepb.MultiGatewayServiceClient),
	}
}

// RegisterWithGRPCServer registers the CancelManager as a gRPC service.
func (cm *CancelManager) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multigatewayservicepb.RegisterMultiGatewayServiceServer(grpcServer, cm)
}

// HandleCancelRequest implements server.CancelHandler.
// It decodes the PID prefix and either cancels locally or forwards to the owning gateway.
func (cm *CancelManager) HandleCancelRequest(ctx context.Context, processID, secretKey uint32) {
	prefix, _ := cancelkey.DecodePID(processID)

	if prefix == cm.ownPrefix {
		cm.localCancelFn(processID, secretKey)
		return
	}

	// Forward to the gateway that owns this PID prefix.
	if err := cm.forwardCancel(ctx, prefix, processID, secretKey); err != nil {
		cm.logger.Warn("failed to forward cancel request",
			"target_prefix", prefix,
			"process_id", processID,
			"error", err,
		)
	}
}

// CancelQuery implements MultiGatewayServiceServer.
// This is called by other gateways forwarding cancel requests via gRPC.
func (cm *CancelManager) CancelQuery(ctx context.Context, req *multigatewayservicepb.CancelQueryRequest) (*multigatewayservicepb.CancelQueryResponse, error) {
	cm.localCancelFn(req.ProcessId, req.SecretKey)
	return &multigatewayservicepb.CancelQueryResponse{}, nil
}

// forwardCancel finds the gateway with the given PID prefix and forwards the cancel request.
func (cm *CancelManager) forwardCancel(ctx context.Context, targetPrefix, processID, secretKey uint32) error {
	addr, err := cm.findGatewayByPrefix(ctx, targetPrefix)
	if err != nil {
		return err
	}

	client, err := cm.getClient(addr)
	if err != nil {
		return fmt.Errorf("connecting to gateway at %s: %w", addr, err)
	}

	_, err = client.CancelQuery(ctx, &multigatewayservicepb.CancelQueryRequest{
		ProcessId: processID,
		SecretKey: secretKey,
	})
	return err
}

// findGatewayByPrefix scans all cells to find the gateway with the given PID prefix.
func (cm *CancelManager) findGatewayByPrefix(ctx context.Context, prefix uint32) (string, error) {
	// TODO: It should be a good idea to cache gateways by prefix, and only fetch if we don't
	// find any one from our cached already. Reduces topo pressure.
	cells, err := cm.ts.GetCellNames(ctx)
	if err != nil {
		return "", fmt.Errorf("getting cell names: %w", err)
	}

	for _, cell := range cells {
		gateways, err := cm.ts.GetMultiGatewaysByCell(ctx, cell)
		if err != nil {
			cm.logger.Warn("failed to get gateways for cell", "cell", cell, "error", err)
			continue
		}

		for _, gw := range gateways {
			if gw.GetPidPrefix() == prefix {
				grpcPort := gw.PortMap["grpc"]
				if grpcPort == 0 {
					return "", fmt.Errorf("gateway %s has no grpc port", gw.GetHostname())
				}
				return fmt.Sprintf("%s:%d", gw.GetHostname(), grpcPort), nil
			}
		}
	}

	return "", fmt.Errorf("no gateway found with pid_prefix=%d", prefix)
}

// getClient returns a cached gRPC client for the given address, creating one if needed.
func (cm *CancelManager) getClient(addr string) (multigatewayservicepb.MultiGatewayServiceClient, error) {
	// TODO: client's should have a timeout to be auto-closed eventually so that we don't
	// keep too many gateway-gateway connections open if they're not being used.
	cm.clientsMu.Lock()
	defer cm.clientsMu.Unlock()

	if client, ok := cm.clients[addr]; ok {
		return client, nil
	}

	conn, err := grpccommon.NewClient(addr,
		grpccommon.WithDialOptions(grpccommon.LocalClientDialOptions()...),
	)
	if err != nil {
		return nil, err
	}

	client := multigatewayservicepb.NewMultiGatewayServiceClient(conn)
	cm.clients[addr] = client
	return client, nil
}
