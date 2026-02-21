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
	"testing"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// GetCurrentTerm returns the current consensus term from a node.
// Use this instead of hardcoded term values for test isolation.
func GetCurrentTerm(ctx context.Context, client consensuspb.MultiPoolerConsensusClient) (int64, error) {
	resp, err := client.Status(ctx, &consensusdatapb.StatusRequest{})
	if err != nil {
		return 0, fmt.Errorf("failed to get consensus status: %w", err)
	}
	return resp.CurrentTerm, nil
}

// MustGetCurrentTerm returns the current term or fails the test.
func MustGetCurrentTerm(t *testing.T, ctx context.Context, client consensuspb.MultiPoolerConsensusClient) int64 {
	t.Helper()
	term, err := GetCurrentTerm(ctx, client)
	if err != nil {
		t.Fatalf("failed to get current term: %v", err)
	}
	return term
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
// Uses Force=true to bypass term validation for simplicity in test cleanup.
func RestorePrimaryAfterDemotion(ctx context.Context, t *testing.T, manager multipoolermanagerpb.MultiPoolerManagerClient, consensus consensuspb.MultiPoolerConsensusClient) error {
	t.Helper()

	// Stop replication on primary
	_, err := manager.StopReplication(ctx, &multipoolermanagerdatapb.StopReplicationRequest{})
	if err != nil {
		return fmt.Errorf("failed to stop replication on primary: %w", err)
	}

	// Get current LSN
	statusResp, err := manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get primary replication status: %w", err)
	}

	// Force promote primary back - term value doesn't matter when Force=true
	// (Force bypasses term validation)
	_, err = consensus.Promote(ctx, &consensusdatapb.PromoteRequest{
		ConsensusTerm: 0, // Ignored when Force=true
		ExpectedLsn:   statusResp.Status.LastReplayLsn,
		Force:         true,
	})
	if err != nil {
		return fmt.Errorf("failed to promote primary: %w", err)
	}

	return nil
}

// SaveGUCs queries multiple GUC values and saves them to a map.
// Returns a map of gucName -> value. Empty values are preserved.
func SaveGUCs(ctx context.Context, client *MultiPoolerTestClient, gucNames []string) map[string]string {
	saved := make(map[string]string)
	for _, gucName := range gucNames {
		value, err := QueryStringValue(ctx, client, "SHOW "+gucName)
		if err == nil {
			saved[gucName] = value
		}
	}
	return saved
}

// RestoreGUCs restores GUC values from a saved map using ALTER SYSTEM.
// Empty values are treated as RESET (restore to default).
func RestoreGUCs(ctx context.Context, t *testing.T, client *MultiPoolerTestClient, savedGucs map[string]string, instanceName string) {
	t.Helper()
	for gucName, gucValue := range savedGucs {
		var query string
		if gucValue == "" {
			query = "ALTER SYSTEM RESET " + gucName
		} else {
			query = fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", gucName, gucValue)
		}
		_, err := client.ExecuteQuery(ctx, query, 1)
		if err != nil {
			t.Logf("Warning: Failed to restore %s on %s in cleanup: %v", gucName, instanceName, err)
		}
	}

	// Reload configuration to apply changes
	_, err := client.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 1)
	if err != nil {
		t.Logf("Warning: Failed to reload config on %s in cleanup: %v", instanceName, err)
	}
}

// ValidateGUCValue queries a GUC and returns an error if it doesn't match the expected value.
// Follows the pattern from multipooler/setup_test.go:validateGUCValue.
func ValidateGUCValue(ctx context.Context, client *MultiPoolerTestClient, gucName, expected, instanceName string) error {
	value, err := QueryStringValue(ctx, client, "SHOW "+gucName)
	if err != nil {
		return fmt.Errorf("%s failed to query %s: %w", instanceName, gucName, err)
	}
	if value != expected {
		return fmt.Errorf("%s has %s='%s' (expected '%s')", instanceName, gucName, value, expected)
	}
	return nil
}
