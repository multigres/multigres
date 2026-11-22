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

package manager

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

func TestRoleGuardrails_ValidateWithoutLogging(t *testing.T) {
	for _, guard := range []struct {
		name       string
		check      func(*MultipoolerManager, context.Context) error
		inRecovery bool
	}{
		{"primary", (*MultipoolerManager).checkPrimaryGuardrails, false},
		{"standby", (*MultipoolerManager).checkReplicaGuardrails, true},
	} {
		t.Run(guard.name, func(t *testing.T) {
			for _, outcome := range []string{"allowed", "role mismatch", "failed probe", "failed precondition probe", "undecodable probe"} {
				t.Run(outcome, func(t *testing.T) {
					pm, qs := newTestManagerWithMock(t, "default", "0-inf")
					t.Cleanup(func() { require.NoError(t, pm.topoClient.Close()) })
					var logs syncBuffer
					pm.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
					inRecovery := guard.inRecovery
					var probeErr error
					switch outcome {
					case "role mismatch":
						inRecovery = !inRecovery
					case "failed probe":
						probeErr = mterrors.New(mtrpcpb.Code_UNAVAILABLE, "cannot read recovery mode")
					case "failed precondition probe":
						probeErr = mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "cannot read recovery mode")
					}
					if probeErr != nil {
						qs.AddQueryPatternWithError("SELECT pg_is_in_recovery", probeErr)
					} else if outcome == "undecodable probe" {
						qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult(nil, nil))
					} else {
						qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{inRecovery}}))
					}

					err := guard.check(pm, t.Context())

					switch outcome {
					case "allowed":
						require.NoError(t, err)
					case "role mismatch":
						require.Error(t, err)
						assert.True(t, isRoleMismatch(err))
						assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
						assert.Equal(t, codes.FailedPrecondition, status.Code(mterrors.ToGRPC(err)))
					default:
						require.Error(t, err)
						assert.False(t, isRoleMismatch(err), "a failed observation is not an observed role mismatch")
						if probeErr != nil {
							assert.ErrorIs(t, err, probeErr)
							assert.Equal(t, mterrors.Code(probeErr), mterrors.Code(err))
						}
					}
					assert.Equal(t, 1, qs.AdminQueryCount())
					assert.Empty(t, logs.String(), "validation returns the error for its caller to handle")
				})
			}
		})
	}
}

func TestGetRoutingSnapshot_DemotionIsQuiet(t *testing.T) {
	for _, afterRead := range []bool{false, true} {
		name := "before read"
		if afterRead {
			name = "after read"
		}
		t.Run(name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			t.Cleanup(func() { require.NoError(t, env.ts.Close()) })
			var logs syncBuffer
			env.pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
			if afterRead {
				env.qs.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))
				env.expectSnapshotRead(9, 2)
			}
			env.qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))

			resp, err := env.pm.GetRoutingSnapshot(t.Context(), sinceVersion(9))

			require.Error(t, err)
			assert.Nil(t, resp)
			assert.True(t, isRoleMismatch(err), "classification survives the UNAVAILABLE wrapper")
			assert.Equal(t, mtrpcpb.Code_UNAVAILABLE, mterrors.Code(err))
			assert.Equal(t, codes.Unavailable, status.Code(mterrors.ToGRPC(err)))
			assert.Empty(t, logs.String(), "the caller rediscovers the primary and retries")
		})
	}
}

func TestReconcileTableGroups_RoleChangesAreQuiet(t *testing.T) {
	for _, outcome := range []string{"standby", "no longer elected primary", "failed precondition probe"} {
		t.Run(outcome, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			t.Cleanup(func() { require.NoError(t, env.ts.Close()) })
			var logs syncBuffer
			env.pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
			switch outcome {
			case "standby":
				env.qs.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
			case "no longer elected primary":
				env.pm.stateManager = NewStateManager(env.pm.logger, env.pm.record, nilConsensusStatus)
				env.pm.stateManager.pgMode = pgmode.Primary
				env.expectPrimary()
			case "failed precondition probe":
				env.qs.AddQueryPatternWithError("SELECT pg_is_in_recovery", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "recovery probe unavailable"))
			}
			// Simulate losing leadership after the pass's initial role check.
			// Both the cancelled creation and the finished drop attempt a write.
			env.expectCreations(creatingOperationRow(time.Now().UnixMicro(), true))
			env.expectDroppings(droppingRow(testTableGroupID, "commerce"))

			pending, err := env.pm.reconcileTableGroups(t.Context())
			env.pm.recordOperationError(t.Context(), testOpID, operationStateCreating, "", "topology unavailable")

			require.NoError(t, err)
			assert.Equal(t, 2, pending, "a new primary can finish both operations")
			assert.Empty(t, env.queriesMatching("UPDATE"))
			assert.Empty(t, env.queriesMatching("DELETE"))
			if outcome == "failed precondition probe" {
				assert.Contains(t, logs.String(), "recovery probe unavailable")
				assert.Contains(t, logs.String(), "tablegroup creation not advanced")
				assert.Contains(t, logs.String(), "dropping tablegroup not deleted")
				assert.Contains(t, logs.String(), "failed to record operation error")
			} else {
				assert.Empty(t, logs.String(), "expected role changes leave operations pending without logging")
			}
		})
	}
}
