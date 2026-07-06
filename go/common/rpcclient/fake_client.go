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

	"google.golang.org/protobuf/proto"

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

// FakeClient implements MultipoolerClient for testing purposes.
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
	RecruitResponses    map[topoclient.ComponentID]*consensusdatapb.RecruitResponse
	PromoteResponses    map[topoclient.ComponentID]*consensusdatapb.PromoteResponse
	SetPrimaryResponses map[topoclient.ComponentID]*consensusdatapb.SetPrimaryResponse

	// Manager service responses - keyed by pooler ID
	WaitForLSNResponses          map[topoclient.ComponentID]*multipoolermanagerdatapb.WaitForLSNResponse
	StartReplicationResponses    map[topoclient.ComponentID]*multipoolermanagerdatapb.StartReplicationResponse
	StopReplicationResponses     map[topoclient.ComponentID]*multipoolermanagerdatapb.StopReplicationResponse
	StatusResponses              map[topoclient.ComponentID]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]
	UpdateConsensusRuleResponses map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse
	// LastUpdateConsensusRuleRequest captures the most recent UpdateConsensusRule
	// request payload for tests that need to assert on operation/IDs.
	LastUpdateConsensusRuleRequest      *multipoolermanagerdatapb.UpdateConsensusRuleRequest
	BackupResponses                     map[topoclient.ComponentID]*multipoolermanagerdatapb.BackupResponse
	RestoreFromBackupResponses          map[topoclient.ComponentID]*multipoolermanagerdatapb.RestoreFromBackupResponse
	GetBackupsResponses                 map[topoclient.ComponentID]*multipoolermanagerdatapb.GetBackupsResponse
	GetBackupByJobIdResponses           map[topoclient.ComponentID]*multipoolermanagerdatapb.GetBackupByJobIdResponse
	RewindToSourceResponses             map[topoclient.ComponentID]*multipoolermanagerdatapb.RewindToSourceResponse
	SetPostgresRestartsEnabledResponses map[topoclient.ComponentID]*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse

	// Errors to return - keyed by pooler ID
	Errors map[topoclient.ComponentID]error

	// RecruitDelays maps pooler IDs to artificial delays for Recruit calls.
	// Use this to simulate slow or unresponsive nodes for testing timeout behavior.
	// If the context is cancelled during the delay, the context error is returned.
	RecruitDelays map[topoclient.ComponentID]time.Duration

	// RecruitGates maps pooler IDs to gate channels. Recruit blocks until the
	// channel is closed. Use this to precisely control when a Recruit call
	// completes relative to other goroutines, e.g. to sequence Phase 1 and
	// Phase 2 in rule-change tests.
	RecruitGates map[topoclient.ComponentID]chan struct{}

	// CallLog tracks which methods were called for verification in tests
	CallLog []string

	// Request tracking for verification in tests
	PromoteRequests    map[topoclient.ComponentID]*consensusdatapb.PromoteRequest
	SetPrimaryRequests map[topoclient.ComponentID]*consensusdatapb.SetPrimaryRequest

	// OnManagerHealthStream, if set, is called after each FakeManagerHealthStream
	// is created. Tests use this to capture the stream and inject snapshots.
	OnManagerHealthStream func(poolerID topoclient.ComponentID, stream *FakeManagerHealthStream)
}

// NewFakeClient creates a new FakeClient with empty response maps.
func NewFakeClient() *FakeClient {
	return &FakeClient{
		RecruitResponses:                    make(map[topoclient.ComponentID]*consensusdatapb.RecruitResponse),
		PromoteResponses:                    make(map[topoclient.ComponentID]*consensusdatapb.PromoteResponse),
		SetPrimaryResponses:                 make(map[topoclient.ComponentID]*consensusdatapb.SetPrimaryResponse),
		WaitForLSNResponses:                 make(map[topoclient.ComponentID]*multipoolermanagerdatapb.WaitForLSNResponse),
		StartReplicationResponses:           make(map[topoclient.ComponentID]*multipoolermanagerdatapb.StartReplicationResponse),
		StopReplicationResponses:            make(map[topoclient.ComponentID]*multipoolermanagerdatapb.StopReplicationResponse),
		StatusResponses:                     make(map[topoclient.ComponentID]*ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]),
		UpdateConsensusRuleResponses:        make(map[topoclient.ComponentID]*multipoolermanagerdatapb.UpdateConsensusRuleResponse),
		BackupResponses:                     make(map[topoclient.ComponentID]*multipoolermanagerdatapb.BackupResponse),
		RestoreFromBackupResponses:          make(map[topoclient.ComponentID]*multipoolermanagerdatapb.RestoreFromBackupResponse),
		GetBackupsResponses:                 make(map[topoclient.ComponentID]*multipoolermanagerdatapb.GetBackupsResponse),
		GetBackupByJobIdResponses:           make(map[topoclient.ComponentID]*multipoolermanagerdatapb.GetBackupByJobIdResponse),
		RewindToSourceResponses:             make(map[topoclient.ComponentID]*multipoolermanagerdatapb.RewindToSourceResponse),
		SetPostgresRestartsEnabledResponses: make(map[topoclient.ComponentID]*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse),
		Errors:                              make(map[topoclient.ComponentID]error),
		RecruitDelays:                       make(map[topoclient.ComponentID]time.Duration),
		RecruitGates:                        make(map[topoclient.ComponentID]chan struct{}),
		CallLog:                             make([]string, 0),
		PromoteRequests:                     make(map[topoclient.ComponentID]*consensusdatapb.PromoteRequest),
		SetPrimaryRequests:                  make(map[topoclient.ComponentID]*consensusdatapb.SetPrimaryRequest),
	}
}

// Helper methods

func (f *FakeClient) getPoolerID(pooler *clustermetadatapb.Multipooler) topoclient.ComponentID {
	if pooler == nil || pooler.Id == nil {
		return ""
	}
	return topoclient.ComponentIDString(pooler.Id)
}

func (f *FakeClient) logCall(method string, poolerID topoclient.ComponentID) {
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

func (f *FakeClient) checkError(poolerID topoclient.ComponentID) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if err, ok := f.Errors[poolerID]; ok {
		return err
	}
	return nil
}

// SetStatusResponse sets a Status response for a pooler with no delay.
func (f *FakeClient) SetStatusResponse(poolerID topoclient.ComponentID, resp *multipoolermanagerdatapb.StatusResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.StatusResponses[poolerID] = &ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
		Response: resp,
		Delay:    0,
	}
}

// SetStatusResponseWithDelay sets a Status response for a pooler with a delay.
// The delay simulates a slow or unresponsive pooler for testing timeout behavior.
func (f *FakeClient) SetStatusResponseWithDelay(poolerID topoclient.ComponentID, resp *multipoolermanagerdatapb.StatusResponse, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.StatusResponses[poolerID] = &ResponseWithDelay[*multipoolermanagerdatapb.StatusResponse]{
		Response: resp,
		Delay:    delay,
	}
}

// SetPostgresRestartsEnabledResponse sets a SetPostgresRestartsEnabled response for a pooler.
func (f *FakeClient) SetPostgresRestartsEnabledResponse(poolerID topoclient.ComponentID, resp *multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.SetPostgresRestartsEnabledResponses[poolerID] = resp
}

//
// Consensus Service Methods
//

func (f *FakeClient) Recruit(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *consensusdatapb.RecruitRequest) (*consensusdatapb.RecruitResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Recruit", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	delay := f.RecruitDelays[poolerID]
	f.mu.RUnlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f.mu.RLock()
	gate := f.RecruitGates[poolerID]
	f.mu.RUnlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f.mu.RLock()
	resp, ok := f.RecruitResponses[poolerID]
	f.mu.RUnlock()
	if !ok {
		resp = &consensusdatapb.RecruitResponse{}
	}

	// Stamp the request's TermRevocation onto the ConsensusStatus. The real
	// multipooler stores the accepted revocation in its state and returns it in
	// ConsensusStatus; filterByRevocation in BuildSafeProposal matches on it.
	if cs := resp.GetConsensusStatus(); cs != nil {
		cloned := proto.Clone(resp).(*consensusdatapb.RecruitResponse)
		cloned.ConsensusStatus.TermRevocation = request.GetTermRevocation()
		return cloned, nil
	}
	return resp, nil
}

func (f *FakeClient) Promote(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *consensusdatapb.PromoteRequest) (*consensusdatapb.PromoteResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("Promote", poolerID)

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
	return &consensusdatapb.PromoteResponse{}, nil
}

func (f *FakeClient) SetPrimary(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *consensusdatapb.SetPrimaryRequest) (*consensusdatapb.SetPrimaryResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("SetPrimary", poolerID)

	f.mu.Lock()
	if f.SetPrimaryRequests == nil {
		f.SetPrimaryRequests = make(map[topoclient.ComponentID]*consensusdatapb.SetPrimaryRequest)
	}
	f.SetPrimaryRequests[poolerID] = request
	f.mu.Unlock()

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.SetPrimaryResponses[poolerID]; ok {
		return resp, nil
	}
	return &consensusdatapb.SetPrimaryResponse{}, nil
}

func (f *FakeClient) UpdateConsensusRule(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.UpdateConsensusRuleRequest) (*multipoolermanagerdatapb.UpdateConsensusRuleResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("UpdateConsensusRule", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.LastUpdateConsensusRuleRequest = request
	f.mu.Unlock()

	f.mu.RLock()
	defer f.mu.RUnlock()
	if resp, ok := f.UpdateConsensusRuleResponses[poolerID]; ok {
		return resp, nil
	}
	return &multipoolermanagerdatapb.UpdateConsensusRuleResponse{}, nil
}

//
// Manager Service Methods - Status and Monitoring
//

func (f *FakeClient) Status(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.StatusRequest) (*multipoolermanagerdatapb.StatusResponse, error) {
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

func (f *FakeClient) WaitForLSN(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.WaitForLSNRequest) (*multipoolermanagerdatapb.WaitForLSNResponse, error) {
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

func (f *FakeClient) StartReplication(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.StartReplicationRequest) (*multipoolermanagerdatapb.StartReplicationResponse, error) {
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

func (f *FakeClient) StopReplication(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.StopReplicationRequest) (*multipoolermanagerdatapb.StopReplicationResponse, error) {
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

func (f *FakeClient) Backup(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.BackupRequest) (*multipoolermanagerdatapb.BackupResponse, error) {
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

func (f *FakeClient) RestoreFromBackup(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.RestoreFromBackupRequest) (*multipoolermanagerdatapb.RestoreFromBackupResponse, error) {
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

func (f *FakeClient) GetBackups(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.GetBackupsRequest) (*multipoolermanagerdatapb.GetBackupsResponse, error) {
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

func (f *FakeClient) GetBackupByJobId(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.GetBackupByJobIdRequest) (*multipoolermanagerdatapb.GetBackupByJobIdResponse, error) {
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

func (f *FakeClient) ExpireBackups(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.ExpireBackupsRequest) (*multipoolermanagerdatapb.ExpireBackupsResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("ExpireBackups", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	return &multipoolermanagerdatapb.ExpireBackupsResponse{}, nil
}

func (f *FakeClient) VerifyBackups(ctx context.Context, pooler *clustermetadatapb.Multipooler, request *multipoolermanagerdatapb.VerifyBackupsRequest) (*multipoolermanagerdatapb.VerifyBackupsResponse, error) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("VerifyBackups", poolerID)

	if err := f.checkError(poolerID); err != nil {
		return nil, err
	}

	return &multipoolermanagerdatapb.VerifyBackupsResponse{}, nil
}

//
// Manager Service Methods - Timeline Repair
//

func (f *FakeClient) RewindToSource(ctx context.Context, pooler *clustermetadatapb.Multipooler, req *multipoolermanagerdatapb.RewindToSourceRequest) (*multipoolermanagerdatapb.RewindToSourceResponse, error) {
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

func (f *FakeClient) SetPostgresRestartsEnabled(ctx context.Context, pooler *clustermetadatapb.Multipooler, req *multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest) (*multipoolermanagerdatapb.SetPostgresRestartsEnabledResponse, error) {
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
func (f *FakeClient) ManagerHealthStream(ctx context.Context, pooler *clustermetadatapb.Multipooler) (ManagerHealthStream, error) {
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

func (f *FakeClient) CloseTablet(pooler *clustermetadatapb.Multipooler) {
	poolerID := f.getPoolerID(pooler)
	f.logCall("CloseTablet", poolerID)
}
