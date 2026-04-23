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
	"time"

	"github.com/stretchr/testify/require"

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
func RestorePrimaryAfterDemotion(ctx context.Context, t *testing.T, client *MultipoolerClient) error {
	t.Helper()

	// After EmergencyDemote restarts postgres as standby, the monitor loop reconciles
	// the stored pooler type from PRIMARY to REPLICA asynchronously. StopReplication
	// requires type=REPLICA, so wait for convergence before calling it.
	require.Eventually(t, func() bool {
		return ValidatePoolerType(context.Background(), client.Manager, clustermetadatapb.PoolerType_REPLICA, "demoted primary") == nil
	}, 10*time.Second, 100*time.Millisecond, "pooler type should converge to REPLICA after demotion")

	// Stop replication on primary
	_, err := client.Manager.StopReplication(ctx, &multipoolermanagerdatapb.StopReplicationRequest{})
	if err != nil {
		return fmt.Errorf("failed to stop replication on primary: %w", err)
	}

	// Get current LSN from manager status
	statusResp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get primary status: %w", err)
	}

	var lastReplayLSN string
	if statusResp.Status != nil && statusResp.Status.ReplicationStatus != nil {
		lastReplayLSN = statusResp.Status.ReplicationStatus.LastReplayLsn
	}

	// Force promote primary back - term value doesn't matter when Force=true
	// (Force bypasses term validation)
	_, err = client.Consensus.Promote(ctx, &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm: 0, // Ignored when Force=true
		ExpectedLsn:   lastReplayLSN,
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

// ReloadConfig calls pg_reload_conf() and waits for the reload to complete
// using pg_conf_load_time() as an event-based completion signal.
//
// pg_reload_conf() sends SIGHUP and returns immediately, before postgres has
// processed the signal. This function waits until pg_conf_load_time() advances
// past the pre-reload value, which happens atomically when postgres finishes
// processing the SIGHUP — at which point all GUC values are guaranteed to
// reflect the latest postgresql.auto.conf.
//
// ctx is used only for the pg_reload_conf() call. The reload-completion wait
// uses its own internal context so that a short caller deadline does not cut
// off the wait on a loaded system.
func ReloadConfig(ctx context.Context, t *testing.T, client *MultiPoolerTestClient, instanceName string) {
	t.Helper()

	loadTimeBefore, err := QueryStringValue(ctx, client, "SELECT pg_conf_load_time()")
	// pg_conf_load_tim() should not normally fail, but if it does, log the
	// error so that we can debug the issue.
	require.NoError(t, err, "Failed to get pg_conf_load_time on %s: %v", instanceName, err)

	_, err = client.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 1)
	require.NoError(t, err, "Failed to reload config on %s: %v", instanceName, err)

	// Use a fresh context for polling so a short caller ctx does not cut off the wait.
	pollCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var funcCallError error
	require.Eventually(t, func() bool {
		loadTimeAfter, err := QueryStringValue(pollCtx, client, "SELECT pg_conf_load_time()")
		// pg_conf_load_tim() should not normally fail, but if it does, stop
		// polling and report it, instead of polling until timeout.
		if err != nil {
			funcCallError = err
			return true
		}
		return loadTimeAfter != loadTimeBefore
	}, 30*time.Second, 10*time.Millisecond,
		"%s: pg_conf_load_time did not advance after pg_reload_conf()", instanceName)

	require.NoError(t, funcCallError, "Error calling pg_conf_load_time on %s: %v", instanceName, funcCallError)
}

// RestoreGUCs restores GUC values from a saved map using ALTER SYSTEM, then
// calls ReloadConfig to apply the changes and wait for the reload to complete.
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

	ReloadConfig(ctx, t, client, instanceName)
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
