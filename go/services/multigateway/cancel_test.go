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
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/topoclient"
	multigatewayservicepb "github.com/multigres/multigres/go/pb/multigatewayservice"
)

func testCancelLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestCancelManager_LocalCancel(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	var canceledPID, canceledSecret uint32
	primaryCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(primaryCancelFn, nil, ownPrefix, nil, testCancelLogger())
	defer cm.Close()

	handler := cm.ForListener(false)
	handler.HandleCancelRequest(context.Background(), pid, 99999)

	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(99999), canceledSecret)
}

func TestCancelManager_LocalReplicaCancel(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	primaryCalled := false
	primaryCancelFn := func(p, s uint32) bool {
		primaryCalled = true
		return false
	}

	var canceledPID, canceledSecret uint32
	replicaCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(primaryCancelFn, replicaCancelFn, ownPrefix, nil, testCancelLogger())
	defer cm.Close()

	handler := cm.ForListener(true)
	handler.HandleCancelRequest(context.Background(), pid, 99999)

	assert.False(t, primaryCalled, "should not call primary cancel for replica cancel request")
	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(99999), canceledSecret)
}

func TestCancelManager_RemoteForward(t *testing.T) {
	ownPrefix := uint32(5)
	targetPrefix := uint32(10)
	pid := pid.EncodePID(targetPrefix, 42)

	primaryCalled := false
	primaryCancelFn := func(p, s uint32) bool {
		primaryCalled = true
		return false
	}

	// Create a mock topology store that returns a gateway with the target prefix.
	gw := topoclient.NewMultiGateway("gw-target", "zone-1", "gw-target.example.com")
	gw.PortMap["grpc"] = 15000
	gw.PidPrefix = targetPrefix
	mockTS := &mockTopoStore{
		cells: []string{"zone-1"},
		gatewaysByCell: map[string][]*topoclient.MultiGatewayInfo{
			"zone-1": {
				topoclient.NewMultiGatewayInfo(gw, nil),
			},
		},
	}

	cm := NewCancelManager(primaryCancelFn, nil, ownPrefix, mockTS, testCancelLogger())
	defer cm.Close()

	// ForListener(false).HandleCancelRequest should try to forward but will fail
	// at the gRPC dial since there's no real server. The important thing is it
	// doesn't call local cancel.
	handler := cm.ForListener(false)
	handler.HandleCancelRequest(context.Background(), pid, 99999)

	assert.False(t, primaryCalled, "should not call local cancel for remote prefix")
}

func TestCancelManager_GRPCHandler(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	var canceledPID, canceledSecret uint32
	primaryCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(primaryCancelFn, nil, ownPrefix, nil, testCancelLogger())
	defer cm.Close()

	resp, err := cm.CancelQuery(context.Background(), &multigatewayservicepb.CancelQueryRequest{
		ProcessId: pid,
		SecretKey: 12345,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(12345), canceledSecret)
}

func TestCancelManager_GRPCHandlerReplica(t *testing.T) {
	ownPrefix := uint32(5)
	pid := pid.EncodePID(ownPrefix, 42)

	primaryCalled := false
	primaryCancelFn := func(p, s uint32) bool {
		primaryCalled = true
		return false
	}

	var canceledPID, canceledSecret uint32
	replicaCancelFn := func(p, s uint32) bool {
		canceledPID = p
		canceledSecret = s
		return true
	}

	cm := NewCancelManager(primaryCancelFn, replicaCancelFn, ownPrefix, nil, testCancelLogger())
	defer cm.Close()

	resp, err := cm.CancelQuery(context.Background(), &multigatewayservicepb.CancelQueryRequest{
		ProcessId: pid,
		SecretKey: 12345,
		Replica:   true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.False(t, primaryCalled, "should not call primary cancel for replica gRPC request")
	assert.Equal(t, pid, canceledPID)
	assert.Equal(t, uint32(12345), canceledSecret)
}

// mockTopoStore implements the topology store methods needed by CancelManager.
type mockTopoStore struct {
	topoclient.Store // embed to satisfy the interface
	cells            []string
	gatewaysByCell   map[string][]*topoclient.MultiGatewayInfo
}

func (m *mockTopoStore) GetCellNames(ctx context.Context) ([]string, error) {
	return m.cells, nil
}

func (m *mockTopoStore) GetMultiGatewaysByCell(ctx context.Context, cellName string) ([]*topoclient.MultiGatewayInfo, error) {
	return m.gatewaysByCell[cellName], nil
}
