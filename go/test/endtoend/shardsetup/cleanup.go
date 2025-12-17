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

package shardsetup

import (
	"context"
	"fmt"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ResetTerm resets the consensus term to 1 with all fields cleared.
// This is used during initialization and cleanup to ensure a clean state.
// Follows the pattern from multipooler/setup_test.go:resetTerm.
func ResetTerm(ctx context.Context, client multipoolermanagerpb.MultiPoolerManagerClient) error {
	initialTerm := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    1,
		AcceptedTermFromCoordinatorId: nil,
		LastAcceptanceTime:            nil,
		LeaderId:                      nil,
	}

	_, err := client.SetTerm(ctx, &multipoolermanagerdatapb.SetTermRequest{Term: initialTerm})
	if err != nil {
		return fmt.Errorf("failed to set term: %w", err)
	}
	return nil
}

// SetPoolerType sets the pooler type using the provided manager client.
// Follows the pattern from multipooler/setup_test.go:setPoolerType.
func SetPoolerType(ctx context.Context, client multipoolermanagerpb.MultiPoolerManagerClient, poolerType clustermetadatapb.PoolerType) error {
	_, err := client.ChangeType(ctx, &multipoolermanagerdatapb.ChangeTypeRequest{
		PoolerType: poolerType,
	})
	if err != nil {
		return fmt.Errorf("failed to set pooler type: %w", err)
	}
	return nil
}

// ValidatePoolerType checks that the pooler type in topology matches the expected value.
// Follows the pattern from multipooler/setup_test.go:validatePoolerType.
func ValidatePoolerType(ctx context.Context, client multipoolermanagerpb.MultiPoolerManagerClient, expectedType clustermetadatapb.PoolerType, nodeName string) error {
	status, err := client.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("%s failed to get status: %w", nodeName, err)
	}

	if status.Status == nil {
		return fmt.Errorf("%s status response has nil Status field", nodeName)
	}

	if status.Status.PoolerType != expectedType {
		return fmt.Errorf("%s pooler type=%s (expected %s)", nodeName, status.Status.PoolerType.String(), expectedType.String())
	}

	return nil
}

// ValidateTerm checks that the consensus term matches the expected value.
// Follows the pattern from multipooler/setup_test.go:validateTerm.
func ValidateTerm(ctx context.Context, client consensuspb.MultiPoolerConsensusClient, expectedTerm int64, nodeName string) error {
	status, err := client.Status(ctx, &consensusdatapb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("%s failed to get consensus status: %w", nodeName, err)
	}

	if status.CurrentTerm != expectedTerm {
		return fmt.Errorf("%s term=%d (expected %d)", nodeName, status.CurrentTerm, expectedTerm)
	}

	return nil
}

// RestorePrimaryAfterDemotion restores the original primary to primary state after it was demoted.
// Uses Force=true and resets term to 1 for simplicity in test cleanup.
// Follows the pattern from multipooler/setup_test.go:restorePrimaryAfterDemotion.
func RestorePrimaryAfterDemotion(ctx context.Context, client multipoolermanagerpb.MultiPoolerManagerClient) error {
	// Set term back to 1
	_, err := client.SetTerm(ctx, &multipoolermanagerdatapb.SetTermRequest{
		Term: &multipoolermanagerdatapb.ConsensusTerm{TermNumber: 1},
	})
	if err != nil {
		return fmt.Errorf("failed to set term on primary: %w", err)
	}

	// Stop replication on primary
	_, err = client.StopReplication(ctx, &multipoolermanagerdatapb.StopReplicationRequest{})
	if err != nil {
		return fmt.Errorf("failed to stop replication on primary: %w", err)
	}

	// Get current LSN
	statusResp, err := client.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get primary replication status: %w", err)
	}

	// Force promote primary back
	_, err = client.Promote(ctx, &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm: 1,
		ExpectedLsn:   statusResp.Status.LastReplayLsn,
		Force:         true,
	})
	if err != nil {
		return fmt.Errorf("failed to promote primary: %w", err)
	}

	return nil
}
