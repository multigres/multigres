// Copyright 2025 Supabase, Inc.
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

package rpcclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ResponseWithDelay wraps a response with an optional delay for testing timeouts.
// If Delay is set, the fake client will sleep for that duration before returning.
// If the context is cancelled during the delay, the context error is returned.
type ResponseWithDelay[T any] struct {
	Response T
	Delay    time.Duration
}

// FakeClient implements MultiPoolerClient for testing purposes.
// It provides a simple in-memory implementation that can be configured
// to return specific responses or errors for testing.
//
// This is useful for:
//   - Unit testing multiorch coordinator logic without real gRPC servers
//   - Integration tests that need predictable multipooler responses
//   - Simulating error conditions and edge cases
//
// Usage:
//
//	fake := rpcclient.NewFakeClient()
//	fake.SetStatusResponse("pooler-1", &multipoolermanagerdatapb.StatusResponse{...})
//	// Or with a delay to simulate slow/unresponsive poolers:
//	fake.SetStatusResponseWithDelay("pooler-2", &multipoolermanagerdatapb.StatusResponse{...}, 5*time.Second)
//	resp, err := fake.Status(ctx, pooler, &multipoolermanagerdatapb.StatusRequest{})
type FakeClient struct {
	mu sync.RWMutex

	// Consensus service responses - keyed by pooler ID
	BeginTermResponses       map[string]*consensusdatapb.BeginTermResponse
	ConsensusStatusResponses map[string]*consensusdatapb.StatusResponse
	LeadershipViewResponses  map[string]*consensusdatapb.LeadershipViewResponse
	CanReachPrimaryResponses map[string]*consensusdatapb.CanReachPrimaryResponse

	// Manager service responses - keyed by pooler ID
	InitializeEmptyPrimaryResponses          map[string]*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse
	StateResponses                           map[string]*multipoolermanagerdatapb.StateResponse
	WaitForLSNResponses                      map[string]*multipoolermanagerdatapb.WaitForLSNResponse
	SetPrimaryConnInfoResponses              map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse
	StartReplicationResponses                map[string]*multipoolermanagerdatapb.StartReplicationResponse
	StopReplicationResponses                 map[string]*multipoolermanagerdatapb.StopReplicationResponse
	StandbyReplicationStatusResponses        map[string]*multipoolermanagerdatapb.StandbyReplicationStatusResponse
	StatusResponses                          map[string]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]
	ResetReplicationResponses                map[string]*multipoolermanagerdatapb.ResetReplicationResponse
	StopReplicationAndGetStatusResponses     map[string]*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse
	ConfigureSynchronousReplicationResponses map[string]*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse
	UpdateSynchronousStandbyListResponses    map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse
	PrimaryStatusResponses                   map[string]*multipoolermanagerdatapb.PrimaryStatusResponse
	PrimaryPositionResponses                 map[string]*multipoolermanagerdatapb.PrimaryPositionResponse
	GetFollowersResponses                    map[string]*multipoolermanagerdatapb.GetFollowersResponse
	PromoteResponses                         map[string]*multipoolermanagerdatapb.PromoteResponse
	DemoteResponses                          map[string]*multipoolermanagerdatapb.DemoteResponse
	UndoDemoteResponses                      map[string]*multipoolermanagerdatapb.UndoDemoteResponse
	ChangeTypeResponses                      map[string]*multipoolermanagerdatapb.ChangeTypeResponse
	SetTermResponses                         map[string]*multipoolermanagerdatapb.SetTermResponse
	GetDurabilityPolicyResponses             map[string]*multipoolermanagerdatapb.GetDurabilityPolicyResponse
	CreateDurabilityPolicyResponses          map[string]*multipoolermanagerdatapb.CreateDurabilityPolicyResponse
	BackupResponses                          map[string]*multipoolermanagerdatapb.BackupResponse
	RestoreFromBackupResponses               map[string]*multipoolermanagerdatapb.RestoreFromBackupResponse
	GetBackupsResponses                      map[string]*multipoolermanagerdatapb.GetBackupsResponse
	GetBackupByJobIdResponses                map[string]*multipoolermanagerdatapb.GetBackupByJobIdResponse
	RewindToSourceResponses                  map[string]*multipoolermanagerdatapb.RewindToSourceResponse

	// Errors to return - keyed by pooler ID
	Errors map[string]error

	// CallLog tracks which methods were called for verification in tests
	CallLog []string

	// Request tracking for verification in tests
	PromoteRequests map[string]*multipoolermanagerdatapb.PromoteRequest
}

// NewFakeClient creates a new FakeClient with empty response maps.
func NewFakeClient() *FakeClient {
	return &FakeClient{
		BeginTermResponses:                       make(map[string]*consensusdatapb.BeginTermResponse),
		ConsensusStatusResponses:                 make(map[string]*consensusdatapb.StatusResponse),
		LeadershipViewResponses:                  make(map[string]*consensusdatapb.LeadershipViewResponse),
		CanReachPrimaryResponses:                 make(map[string]*consensusdatapb.CanReachPrimaryResponse),
		InitializeEmptyPrimaryResponses:          make(map[string]*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse),
		StateResponses:                           make(map[string]*multipoolermanagerdatapb.StateResponse),
		WaitForLSNResponses:                      make(map[string]*multipoolermanagerdatapb.WaitForLSNResponse),
		SetPrimaryConnInfoResponses:              make(map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse),
		StartReplicationResponses:                make(map[string]*multipoolermanagerdatapb.StartReplicationResponse),
		StopReplicationResponses:                 make(map[string]*multipoolermanagerdatapb.StopReplicationResponse),
		StandbyReplicationStatusResponses:        make(map[string]*multipoolermanagerdatapb.StandbyReplicationStatusResponse),
		StatusResponses:                          make(map[string]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]),
		ResetReplicationResponses:                make(map[string]*multipoolermanagerdatapb.ResetReplicationResponse),
		StopReplicationAndGetStatusResponses:     make(map[string]*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse),
		ConfigureSynchronousReplicationResponses: make(map[string]*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse),
		UpdateSynchronousStandbyListResponses:    make(map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse),
		PrimaryStatusResponses:                   make(map[string]*multipoolermanagerdatapb.PrimaryStatusResponse),
		PrimaryPositionResponses:                 make(map[string]*multipoolermanagerdatapb.PrimaryPositionResponse),
		GetFollowersResponses:                    make(map[string]*multipoolermanagerdatapb.GetFollowersResponse),
		PromoteResponses:                         make(map[string]*multipoolermanagerdatapb.PromoteResponse),
		DemoteResponses:                          make(map[string]*multipoolermanagerdatapb.DemoteResponse),
		UndoDemoteResponses:                      make(map[string]*multipoolermanagerdatapb.UndoDemoteResponse),
		ChangeTypeResponses:                      make(map[string]*multipoolermanagerdatapb.ChangeTypeResponse),
		SetTermResponses:                         make(map[string]*multipoolermanagerdatapb.SetTermResponse),
		GetDurabilityPolicyResponses:             make(map[string]*multipoolermanagerdatapb.GetDurabilityPolicyResponse),
		CreateDurabilityPolicyResponses:          make(map[string]*multipoolermanagerdatapb.CreateDurabilityPolicyResponse),
		BackupResponses:                          make(map[string]*multipoolermanagerdatapb.BackupResponse),
		RestoreFromBackupResponses:               make(map[string]*multipoolermanagerdatapb.RestoreFromBackupResponse),
		GetBackupsResponses:                      make(map[string]*multipoolermanagerdatapb.GetBackupsResponse),
		GetBackupByJobIdResponses:                make(map[string]*multipoolermanagerdatapb.GetBackupByJobIdResponse),
		RewindToSourceResponses:                  make(map[string]*multipoolermanagerdatapb.RewindToSourceResponse),
		Errors:                                   make(map[string]error),
		CallLog:                                  make([]string, 0),
		PromoteRequests:                          make(map[string]*multipoolermanagerdatapb.PromoteRequest),
	}
}

// Helper methods

func (f *FakeClient) getPoolerID(pooler *clustermetadatapb.MultiPooler) string {
	if pooler == nil || pooler.Id == nil {
		return ""
	}
	return topoclient.MultiPoolerIDString(pooler.Id)
}

func (f *FakeClient) logCall(method string, poolerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CallLog = append(f.CallLog, fmt.Sprintf("%s(%s)", method, poolerID))
}

// GetCallLog returns a copy of the call log in a thread-safe manner.
func (f *FakeClient) GetCallLog() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make([]string, len(f.CallLog))
	copy(result, f.CallLog)
	return result
}

// ResetCallLog clears the call log in a thread-safe manner.
func (f *FakeClient) ResetCallLog() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.CallLog = nil
}

func (f *FakeClient) checkError(poolerID string) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err, ok := f.Errors[poolerID]; ok {
		return err
	}
	return nil
}

// SetStatusResponse sets a Status response for a pooler with no delay.
func (f *FakeClient) SetStatusResponse(poolerID string, resp *multipoolermanagerdatapb.StatusResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.StatusResponses[poolerID] = &ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
		Response: resp,
		Delay:    0,
	}
}

// SetStatusResponseWithDelay sets a Status response for a pooler with a delay.
// The delay simulates a slow or unresponsive pooler for testing timeout behavior.
func (f *FakeClient) SetStatusResponseWithDelay(poolerID string, resp *multipoolermanagerdatapb.StatusResponse, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.StatusResponses[poolerID] = &ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
		Response: resp,
		Delay:    delay,
	}
}

// SetEnableMonitorResponse sets an EnableMonitor response for a pooler.
//
// Consensus Service Methods
//

func (f *FakeClient) BeginTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("BeginTerm", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.BeginTermResponses[poolerID]; ok {
		return resp, nil
	}
	return &consensusdatapb.BeginTermResponse{}, nil
}

func (f *FakeClient) ConsensusStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.StatusRequest) (*consensusdatapb.StatusResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ConsensusStatus", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.ConsensusStatusResponses[poolerID]; ok {
		return resp, nil
	}
	return &consensusdatapb.StatusResponse{}, nil
}

func (f *FakeClient) GetLeadershipView(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.LeadershipViewRequest) (*consensusdatapb.LeadershipViewResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("GetLeadershipView", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.LeadershipViewResponses[poolerID]; ok {
		return resp, nil
	}
	return &consensusdatapb.LeadershipViewResponse{}, nil
}

func (f *FakeClient) CanReachPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.CanReachPrimaryRequest) (*consensusdatapb.CanReachPrimaryResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("CanReachPrimary", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.CanReachPrimaryResponses[poolerID]; ok {
		return resp, nil
	}
	return &consensusdatapb.CanReachPrimaryResponse{}, nil
}

//
// Manager Service Methods - Initialization
//

func (f *FakeClient) InitializeEmptyPrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.InitializeEmptyPrimaryRequest) (*multipoolermanagerdatapb.InitializeEmptyPrimaryResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("InitializeEmptyPrimary", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.InitializeEmptyPrimaryResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.InitializeEmptyPrimaryResponse{}, nil
}

//
// Manager Service Methods - Status and Monitoring
//

func (f *FakeClient) State(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StateRequest) (*multipoolermanagerdatapb.StateResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("State", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.StateResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.StateResponse{}, nil
}

//
// Manager Service Methods - Replication
//

func (f *FakeClient) WaitForLSN(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("WaitForLSN", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.WaitForLSNResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.WaitForLSNResponse{}, nil
}

func (f *FakeClient) SetPrimaryConnInfo(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetPrimaryConnInfoRequest) (*multipoolermanagerdatapb.SetPrimaryConnInfoResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("SetPrimaryConnInfo", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.SetPrimaryConnInfoResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.SetPrimaryConnInfoResponse{}, nil
}

func (f *FakeClient) StartReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("StartReplication", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.StartReplicationResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.StartReplicationResponse{}, nil
}

func (f *FakeClient) StopReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("StopReplication", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.StopReplicationResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.StopReplicationResponse{}, nil
}

func (f *FakeClient) StandbyReplicationStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StandbyReplicationStatusRequest) (*multipoolermanagerdatapb.StandbyReplicationStatusResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("StandbyReplicationStatus", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.StandbyReplicationStatusResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.StandbyReplicationStatusResponse{}, nil
}

func (f *FakeClient) Status(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Status", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	respWithDelay, ok := f.StatusResponses[poolerID]
	f.mu.RUnlock()

	if ok {
		if respWithDelay.Delay > 0 {
			select {
			case <-time.After(respWithDelay.Delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return respWithDelay.Response, nil
	}
	return &multipoolermanagerdatapb.StatusResponse{}, nil
}

func (f *FakeClient) ResetReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ResetReplicationRequest) (*multipoolermanagerdatapb.ResetReplicationResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ResetReplication", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.ResetReplicationResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.ResetReplicationResponse{}, nil
}

func (f *FakeClient) StopReplicationAndGetStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.StopReplicationAndGetStatusRequest) (*multipoolermanagerdatapb.StopReplicationAndGetStatusResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("StopReplicationAndGetStatus", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.StopReplicationAndGetStatusResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.StopReplicationAndGetStatusResponse{}, nil
}

//
// Manager Service Methods - Synchronous Replication
//

func (f *FakeClient) ConfigureSynchronousReplication(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest) (*multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ConfigureSynchronousReplication", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.ConfigureSynchronousReplicationResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.ConfigureSynchronousReplicationResponse{}, nil
}

func (f *FakeClient) UpdateSynchronousStandbyList(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("UpdateSynchronousStandbyList", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.UpdateSynchronousStandbyListResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

//
// Manager Service Methods - Primary Status
//

func (f *FakeClient) PrimaryStatus(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryStatusRequest) (*multipoolermanagerdatapb.PrimaryStatusResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("PrimaryStatus", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.PrimaryStatusResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.PrimaryStatusResponse{}, nil
}

func (f *FakeClient) PrimaryPosition(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PrimaryPositionRequest) (*multipoolermanagerdatapb.PrimaryPositionResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("PrimaryPosition", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.PrimaryPositionResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.PrimaryPositionResponse{}, nil
}

func (f *FakeClient) GetFollowers(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetFollowersRequest) (*multipoolermanagerdatapb.GetFollowersResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("GetFollowers", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.GetFollowersResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.GetFollowersResponse{}, nil
}

//
// Manager Service Methods - Promotion and Demotion
//

func (f *FakeClient) Promote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.PromoteRequest) (*multipoolermanagerdatapb.PromoteResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Promote", poolerID)

	// Record the request for test verification
	f.mu.Lock()
	f.PromoteRequests[poolerID] = request
	f.mu.Unlock()

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.PromoteResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.PromoteResponse{}, nil
}

func (f *FakeClient) Demote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteRequest) (*multipoolermanagerdatapb.DemoteResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Demote", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.DemoteResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.DemoteResponse{}, nil
}

func (f *FakeClient) UndoDemote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UndoDemoteRequest) (*multipoolermanagerdatapb.UndoDemoteResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("UndoDemote", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.UndoDemoteResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.UndoDemoteResponse{}, nil
}

func (f *FakeClient) DemoteStalePrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteStalePrimaryRequest) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("DemoteStalePrimary", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	// Return success by default
	return &multipoolermanagerdatapb.DemoteStalePrimaryResponse{
		Success:         true,
		RewindPerformed: false,
		LsnPosition:     "0/0",
	}, nil
}

//
// Manager Service Methods - Type and Term Management
//

func (f *FakeClient) ChangeType(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ChangeTypeRequest) (*multipoolermanagerdatapb.ChangeTypeResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ChangeType", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.ChangeTypeResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.ChangeTypeResponse{}, nil
}

func (f *FakeClient) SetTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.SetTermRequest) (*multipoolermanagerdatapb.SetTermResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("SetTerm", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.SetTermResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.SetTermResponse{}, nil
}

//
// Manager Service Methods - Durability Policy
//

func (f *FakeClient) GetDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetDurabilityPolicyRequest) (*multipoolermanagerdatapb.GetDurabilityPolicyResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("GetDurabilityPolicy", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.GetDurabilityPolicyResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.GetDurabilityPolicyResponse{}, nil
}

func (f *FakeClient) CreateDurabilityPolicy(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.CreateDurabilityPolicyRequest) (*multipoolermanagerdatapb.CreateDurabilityPolicyResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("CreateDurabilityPolicy", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.CreateDurabilityPolicyResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.CreateDurabilityPolicyResponse{}, nil
}

//
// Manager Service Methods - Backup and Restore
//

func (f *FakeClient) Backup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Backup", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.BackupResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.BackupResponse{}, nil
}

func (f *FakeClient) RestoreFromBackup(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("RestoreFromBackup", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.RestoreFromBackupResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.RestoreFromBackupResponse{}, nil
}

func (f *FakeClient) GetBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("GetBackups", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.GetBackupsResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.GetBackupsResponse{}, nil
}

func (f *FakeClient) GetBackupByJobId(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.GetBackupByJobIdRequest) (*multipoolermanagerdatapb.GetBackupByJobIdResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("GetBackupByJobId", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.GetBackupByJobIdResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.GetBackupByJobIdResponse{}, nil
}

//
// Manager Service Methods - Timeline Repair
//

// RewindToSource performs pg_rewind to synchronize a replica with its source.
func (f *FakeClient) RewindToSource(ctx context.Context, pooler *clustermetadatapb.MultiPooler, req *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("RewindToSource", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.RewindToSourceResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.RewindToSourceResponse{}, nil
}

//
// Manager Service Methods - PostgreSQL Monitoring Control
//
// Connection Management Methods
//

func (f *FakeClient) Close() {
	// No-op for fake client
}

func (f *FakeClient) CloseTablet(pooler *clustermetadatapb.MultiPooler) {
	// No-op for fake client
}
