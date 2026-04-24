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
	"errors"
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
	EmergencyDemoteResponses map[string]*multipoolermanagerdatapb.EmergencyDemoteResponse
	PromoteResponses         map[string]*multipoolermanagerdatapb.PromoteResponse

	// Manager service responses - keyed by pooler ID
	WaitForLSNResponses                 map[string]*multipoolermanagerdatapb.WaitForLSNResponse
	SetPrimaryConnInfoResponses         map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse
	StartReplicationResponses           map[string]*multipoolermanagerdatapb.StartReplicationResponse
	StopReplicationResponses            map[string]*multipoolermanagerdatapb.StopReplicationResponse
	StatusResponses                     map[string]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]
	UpdateConsensusRuleResponses        map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse
	BackupResponses                     map[string]*multipoolermanagerdatapb.BackupResponse
	RestoreFromBackupResponses          map[string]*multipoolermanagerdatapb.RestoreFromBackupResponse
	GetBackupsResponses                 map[string]*multipoolermanagerdatapb.GetBackupsResponse
	GetBackupByJobIdResponses           map[string]*multipoolermanagerdatapb.GetBackupByJobIdResponse
	RewindToSourceResponses             map[string]*multipoolermanagerdatapb.RewindToSourceResponse
	SetPostgresRestartsEnabledResponses map[string]*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse

	// Errors to return - keyed by pooler ID
	Errors map[string]error

	// CallLog tracks which methods were called for verification in tests
	CallLog []string

	// Request tracking for verification in tests
	PromoteRequests   map[string]*multipoolermanagerdatapb.PromoteRequest
	BeginTermRequests map[string][]*consensusdatapb.BeginTermRequest

	// OnManagerHealthStream, if set, is called after each FakeManagerHealthStream
	// is created. Tests use this to capture the stream and inject snapshots.
	OnManagerHealthStream func(poolerID string, stream *FakeManagerHealthStream)
}

// NewFakeClient creates a new FakeClient with empty response maps.
func NewFakeClient() *FakeClient {
	return &FakeClient{
		BeginTermResponses:                  make(map[string]*consensusdatapb.BeginTermResponse),
		ConsensusStatusResponses:            make(map[string]*consensusdatapb.StatusResponse),
		EmergencyDemoteResponses:            make(map[string]*multipoolermanagerdatapb.EmergencyDemoteResponse),
		PromoteResponses:                    make(map[string]*multipoolermanagerdatapb.PromoteResponse),
		WaitForLSNResponses:                 make(map[string]*multipoolermanagerdatapb.WaitForLSNResponse),
		SetPrimaryConnInfoResponses:         make(map[string]*multipoolermanagerdatapb.SetPrimaryConnInfoResponse),
		StartReplicationResponses:           make(map[string]*multipoolermanagerdatapb.StartReplicationResponse),
		StopReplicationResponses:            make(map[string]*multipoolermanagerdatapb.StopReplicationResponse),
		StatusResponses:                     make(map[string]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]),
		UpdateConsensusRuleResponses:        make(map[string]*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse),
		BackupResponses:                     make(map[string]*multipoolermanagerdatapb.BackupResponse),
		RestoreFromBackupResponses:          make(map[string]*multipoolermanagerdatapb.RestoreFromBackupResponse),
		GetBackupsResponses:                 make(map[string]*multipoolermanagerdatapb.GetBackupsResponse),
		GetBackupByJobIdResponses:           make(map[string]*multipoolermanagerdatapb.GetBackupByJobIdResponse),
		RewindToSourceResponses:             make(map[string]*multipoolermanagerdatapb.RewindToSourceResponse),
		SetPostgresRestartsEnabledResponses: make(map[string]*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse),
		Errors:                              make(map[string]error),
		CallLog:                             make([]string, 0),
		PromoteRequests:                     make(map[string]*multipoolermanagerdatapb.PromoteRequest),
		BeginTermRequests:                   make(map[string][]*consensusdatapb.BeginTermRequest),
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

// SetPostgresRestartsEnabledResponse sets a SetPostgresRestartsEnabled response for a pooler.
func (f *FakeClient) SetPostgresRestartsEnabledResponse(poolerID string, resp *multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.SetPostgresRestartsEnabledResponses[poolerID] = resp
}

//
// Consensus Service Methods
//

func (f *FakeClient) BeginTerm(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *consensusdatapb.BeginTermRequest) (*consensusdatapb.BeginTermResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("BeginTerm", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.BeginTermRequests[poolerID] = append(f.BeginTermRequests[poolerID], request)
	resp, ok := f.BeginTermResponses[poolerID]
	f.mu.Unlock()
	if ok {
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

func (f *FakeClient) EmergencyDemote(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.EmergencyDemoteRequest) (*multipoolermanagerdatapb.EmergencyDemoteResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("EmergencyDemote", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.EmergencyDemoteResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.EmergencyDemoteResponse{}, nil
}

func (f *FakeClient) DemoteStalePrimary(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.DemoteStalePrimaryRequest) (*multipoolermanagerdatapb.DemoteStalePrimaryResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("DemoteStalePrimary", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	return &multipoolermanagerdatapb.DemoteStalePrimaryResponse{
		Success:         true,
		RewindPerformed: false,
		LsnPosition:     "0/0",
	}, nil
}

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

func (f *FakeClient) UpdateConsensusRule(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest) (*multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("UpdateConsensusRule", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.UpdateConsensusRuleResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.UpdateSynchronousStandbyListResponse{}, nil
}

//
// Manager Service Methods - Status and Monitoring
//

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

func (f *FakeClient) ExpireBackups(ctx context.Context, pooler *clustermetadatapb.MultiPooler, request *multipoolermanagerdatapb.ExpireBackupsRequest) (*multipoolermanagerdatapb.ExpireBackupsResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ExpireBackups", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	return &multipoolermanagerdatapb.ExpireBackupsResponse{}, nil
}

//
// Manager Service Methods - Timeline Repair
//

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
// Manager Service Methods - PostgreSQL Restart Control
//

func (f *FakeClient) SetPostgresRestartsEnabled(ctx context.Context, pooler *clustermetadatapb.MultiPooler, req *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("SetPostgresRestartsEnabled", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.SetPostgresRestartsEnabledResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse{}, nil
}

//
// Manager Service Methods - Health Streaming
//

// FakeManagerHealthStream implements ManagerHealthStream for testing.
// Recv blocks until the context is cancelled or a response is injected via Ch.
// Sent messages (init, poll) are recorded on the Sent channel.
type FakeManagerHealthStream struct {
	ctx  context.Context
	Ch   chan *multipoolermanagerdatapb.ManagerHealthStreamResponse
	Sent chan *multipoolermanagerdatapb.ManagerHealthStreamClientMessage
}

// Recv blocks until a response is available or the context is cancelled.
func (f *FakeManagerHealthStream) Recv() (*multipoolermanagerdatapb.ManagerHealthStreamResponse, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case resp, ok := <-f.Ch:
		if !ok {
			return nil, errors.New("stream closed")
		}
		return resp, nil
	}
}

// Send records the outgoing message on the Sent channel.
// Non-blocking: if Sent is full the message is dropped (tests should drain it).
func (f *FakeManagerHealthStream) Send(msg *multipoolermanagerdatapb.ManagerHealthStreamClientMessage) error {
	select {
	case f.Sent <- msg:
	default:
	}
	return nil
}

// ManagerHealthStream returns a FakeManagerHealthStream. Tests inject snapshots
// by sending on stream.Ch or close it to simulate disconnection. Outgoing
// messages (init/poll) are readable from stream.Sent.
func (f *FakeClient) ManagerHealthStream(ctx context.Context, pooler *clustermetadatapb.MultiPooler) (ManagerHealthStream, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ManagerHealthStream", poolerID)

	f.mu.RLock()
	err := f.Errors[poolerID]
	f.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	stream := &FakeManagerHealthStream{
		ctx:  ctx,
		Ch:   make(chan *multipoolermanagerdatapb.ManagerHealthStreamResponse),
		Sent: make(chan *multipoolermanagerdatapb.ManagerHealthStreamClientMessage, 16),
	}
	if f.OnManagerHealthStream != nil {
		f.OnManagerHealthStream(poolerID, stream)
	}
	return stream, nil
}

//
// Connection Management Methods
//

func (f *FakeClient) Close() {
	// No-op for fake client
}

func (f *FakeClient) CloseTablet(pooler *clustermetadatapb.MultiPooler) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("CloseTablet", poolerID)
}
