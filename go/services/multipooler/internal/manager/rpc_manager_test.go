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

package manager

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus/consensustest"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// addDatabaseToTopo creates a database in the topology with a backup location
func addDatabaseToTopo(t *testing.T, ts topoclient.Store, database string) {
	t.Helper()
	ctx := context.Background()
	err := ts.CreateDatabase(ctx, database, &clustermetadatapb.Database{
		Name:           database,
		BackupLocation: utils.FilesystemBackupLocation("/var/backups/pgbackrest"),
	})
	require.NoError(t, err)
}

// expectRewindPositionFloorMocks adds the query expectations for the
// pause-replication/wait-stabilize/measure-position sequence that
// restartAsStandbyLocked's wantRewind branch now runs before pg_rewind (see
// ConsensusPromises.SetRecruitBlockedUntil) — pauseReplication(REPLAY_AND_RECEIVER),
// waitForReplayComplete, and ConsensusStatus's rule-store read. Most patterns
// are added via AddQueryPattern (repeatable), so callers don't need to track
// exact poll counts. The reload-config pair is the exception: it's added via
// AddQueryPatternOnce (this reload fires exactly once, before pg_rewind), so
// it's fully consumed here and doesn't shadow or get consumed by a second,
// textually-identical reload a test's own setPrimaryConnInfoLocked call runs
// later (which needs to see its own load-time value change to detect
// completion, not this call's).
func expectRewindPositionFloorMocks(m *mock.QueryService) {
	replStatusCols := []string{"replay_lsn", "receive_lsn", "is_paused", "pause_state", "last_xact_replay_ts", "primary_conninfo", "status", "last_msg_receive_time", "wal_receiver_status_interval", "wal_receiver_timeout"}
	replStatusRow := [][]any{{nil, nil, true, "paused", nil, "", nil, nil, nil, nil}}

	// pauseReplication(REPLAY_AND_RECEIVER): resetPrimaryConnInfo, then reload.
	// The reload reads the config load time as a Unix epoch (see readConfLoadTime),
	// so the before/after mocks return distinct epoch seconds.
	m.AddQueryPattern("ALTER SYSTEM RESET primary_conninfo", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"0"}}))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	m.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1"}}))

	// restartAsStandbyLocked clears restore_command before the replay-completion
	// wait (a rewinding cohort member must replay only local WAL / stream from the
	// leader). resetRestoreCommand does ALTER SYSTEM RESET + a second reload cycle;
	// register it after the pause reload above so the Once load-time pairs are
	// consumed in execution order. (stopRestoreCommand goes through the mock pgctld
	// gRPC server, so it needs no SQL mock here.)
	m.AddQueryPattern("ALTER SYSTEM RESET restore_command", mock.MakeQueryResult(nil, nil))
	m.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"2"}}))
	m.AddQueryPatternOnce("SELECT pg_reload_conf", mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	m.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"3"}}))

	// ConsensusStatus -> Rules().ObservePosition -> readCurrentRule's SELECT.
	// Column shape mirrors consensus.mockDecidedReadResult (a decided rule,
	// no pending proposal). Registered before the broader
	// "pg_last_wal_receive_lsn" pattern below: readCurrentRule's own SQL
	// computes current_lsn via a COALESCE that happens to contain
	// "pg_last_wal_receive_lsn()" as a substring, so the more specific
	// pattern must be tried first or it never gets reached.
	m.AddQueryPattern("SELECT decision_coordinator_term, decision_leader_subterm, leader_id, coordinator_id, cohort_members",
		mock.MakeQueryResult(
			[]string{
				"decision_coordinator_term", "decision_leader_subterm", "leader_id", "coordinator_id", "cohort_members",
				"durability_policy_name", "durability_quorum_type", "durability_required_count", "created_at",
				"proposal_coordinator_term", "proposal_leader_subterm", "proposal_leader_id", "proposal_cohort_members",
				"proposal_durability_policy_name", "proposal_durability_quorum_type", "proposal_durability_required_count",
				"proposal_created_at", "current_lsn",
			},
			[][]any{
				{
					int64(1), int64(0), "zone1_leader-1", "zone1_coordinator-1", "{zone1_member-1,zone1_member-2}",
					"AT_LEAST_2", "QUORUM_TYPE_AT_LEAST_N", int64(2), "2026-01-01 00:00:00+00",
					nil, nil, nil, nil, nil, nil, nil, nil,
					"0/100",
				},
			},
		))

	// waitForReceiverDisconnect: count/status/conninfo snapshot, then a final
	// queryReplicationStatus once the receiver shows disconnected.
	m.AddQueryPattern("SELECT COUNT.*pg_stat_wal_receiver", mock.MakeQueryResult(
		[]string{"count", "status", "primary_conninfo"}, [][]any{{int64(0), "", ""}}))

	// waitForReplayComplete's checkNoWALSource precondition: primary_conninfo empty
	// (not streaming). Anchored so it matches only this query, not the other
	// current_setting('primary_conninfo') reads in this path.
	m.AddQueryPattern(`^SELECT current_setting\('primary_conninfo', true\)`, mock.MakeQueryResult(
		[]string{"primary_conninfo", "restore_command"}, [][]any{{"", ""}}))

	// waitForReplayComplete's queryReplayProgress reads replay_lsn, receive_lsn,
	// is_paused, and the startup wait event in one query. It is the only query
	// containing pg_stat_activity, so it is matched on that; it also contains
	// pg_last_wal_receive_lsn(), so it must be registered BEFORE the broad
	// "pg_last_wal_receive_lsn" pattern below or that one would shadow it.
	// replay == receive (and no wait event) → caught up, so waitForReplayComplete
	// returns via signal 1 on the first poll.
	m.AddQueryPattern("pg_stat_activity", mock.MakeQueryResult(
		[]string{"pg_last_wal_replay_lsn", "pg_last_wal_receive_lsn", "pg_is_wal_replay_paused", "wait_event_type", "wait_event"},
		[][]any{{"0/100", "0/100", false, nil, nil}}))

	// queryReplicationStatus (10-column) for waitForReceiverDisconnect's final
	// read and waitForReplayComplete's returned status.
	m.AddQueryPattern("pg_last_wal_receive_lsn", mock.MakeQueryResult(replStatusCols, replStatusRow))

	// Pause replay, then waitForReplicationPause's queryReplicationStatus poll.
	m.AddQueryPattern("pg_wal_replay_pause", mock.MakeQueryResult(nil, nil))
}

func TestPrimaryPosition(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	tests := []struct {
		name          string
		poolerType    clustermetadatapb.PoolerType
		expectError   bool
		expectedCode  mtrpcpb.Code
		errorContains string
	}{
		{
			name:          "REPLICA pooler returns FAILED_PRECONDITION",
			poolerType:    clustermetadatapb.PoolerType_REPLICA,
			expectError:   true,
			expectedCode:  mtrpcpb.Code_FAILED_PRECONDITION,
			errorContains: "standby mode",
		},
		{
			name:          "PRIMARY pooler passes type check",
			poolerType:    clustermetadatapb.PoolerType_PRIMARY,
			expectError:   true,
			errorContains: "failed to get current WAL LSN", // Will fail on WAL LSN query, not type check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
			defer ts.Close()

			// Create temp directory for pooler-dir
			poolerDir := t.TempDir()
			createPgDataDir(t, poolerDir)

			// Create the database in topology with backup location
			database := "testdb"
			addDatabaseToTopo(t, ts, database)

			multipooler := &clustermetadatapb.Multipooler{
				Id:            serviceID,
				Hostname:      "localhost",
				PortMap:       map[string]int32{"grpc": 8080},
				Type:          tt.poolerType,
				ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   database,
					TableGroup: constants.DefaultTableGroup,
					Shard:      constants.DefaultShard,
				},
			}
			// A PRIMARY record must name itself as leader (the record invariant).
			if tt.poolerType == clustermetadatapb.PoolerType_PRIMARY {
				multipooler.RoutingState = &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY}
			}
			require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

			multipooler.PoolerDir = poolerDir

			config := &Config{
				TopoClient: ts,
			}
			mockQueryService := mock.NewQueryService()
			manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
				withMockController(&mockPoolerController{queryService: mockQueryService}))
			require.NoError(t, err)
			defer manager.ShutdownForTest(t.Context())

			// Set up mock query service for postgresMode checks during test
			isReplica := tt.poolerType == clustermetadatapb.PoolerType_REPLICA
			mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{isReplica}}))

			// Mark as initialized to skip auto-restore (not testing backup functionality)
			err = manager.setInitialized()
			require.NoError(t, err)

			// Start and wait for ready
			senv := servenv.NewServEnv(viperutil.NewRegistry())
			go manager.Start(senv)
			require.Eventually(t, func() bool {
				return manager.GetState() == ManagerStateReady
			}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

			// Call PrimaryPosition
			_, err = manager.PrimaryPosition(ctx)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)

				if tt.expectedCode != 0 {
					code := mterrors.Code(err)
					assert.Equal(t, tt.expectedCode, code)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestActionLock_MutationMethodsTimeout(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()

	// Create the database in topology with backup location
	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	// Create PRIMARY multipooler for testing
	multipooler := &clustermetadatapb.Multipooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		// A PRIMARY record must name itself as leader (the record invariant).
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	config := &Config{
		TopoClient: ts,
	}
	mockQueryService := mock.NewQueryService()
	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	// Set up mock query service for postgresMode check during startup
	mockQueryService.AddQueryPatternOnce("SELECT pg_is_in_recovery", mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	// Start and wait for ready
	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

	// Helper function to hold the lock for a duration
	holdLock := func(duration time.Duration) context.CancelFunc {
		lockCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(duration))
		lockAcquired := make(chan struct{})
		go func() {
			newCtx, err := manager.actionLock.Acquire(lockCtx, "test-lock-holder")
			if err == nil {
				// Signal that the lock was acquired
				close(lockAcquired)
				// Hold the lock for the duration or until cancelled
				<-lockCtx.Done()
				manager.actionLock.Release(newCtx)
			}
		}()
		// Wait for the lock to be acquired
		<-lockAcquired
		return cancel
	}

	tests := []struct {
		name       string
		poolerType clustermetadatapb.PoolerType
		callMethod func(context.Context) error
	}{
		{
			name:       "StartReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				return manager.StartReplication(ctx)
			},
		},
		{
			name:       "StopReplication times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_REPLICA,
			callMethod: func(ctx context.Context) error {
				return manager.StopReplication(ctx, multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY, true /* wait */)
			},
		},
		{
			name:       "UpdateConsensusRule times out when lock is held",
			poolerType: clustermetadatapb.PoolerType_PRIMARY,
			callMethod: func(ctx context.Context) error {
				_, err := manager.UpdateConsensusRule(ctx, multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD, []*clustermetadatapb.ID{serviceID}, &clustermetadatapb.RuleNumber{}, nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Update the pooler type if needed for this test
			if tt.poolerType != multipooler.Type {
				_, err := ts.UpdateMultipoolerFields(ctx, serviceID, func(mp *clustermetadatapb.Multipooler) error {
					mp.Type = tt.poolerType
					return nil
				})
				require.NoError(t, err)
				setPoolerTypeForTest(t, manager, tt.poolerType)
			}

			// Hold the lock for 2 seconds
			cancel := holdLock(2 * time.Second)
			defer cancel()

			// Try to call the method - it should timeout because lock is held
			err := tt.callMethod(utils.WithTimeout(t, 500*time.Millisecond))

			// Verify the error is a timeout/context error
			require.Error(t, err, "Method should fail when lock is held")
			assert.Contains(t, err.Error(), "failed to acquire action lock", "Error should mention lock acquisition failure")

			// Verify the underlying error is context deadline exceeded
			assert.ErrorIs(t, err, context.DeadlineExceeded, "Should be a deadline exceeded error")
		})
	}
}

// createPgDataDir creates the pg_data directory with PG_VERSION file.
// This is needed for setInitialized() to work since it writes a marker file to pg_data.
func createPgDataDir(t *testing.T, poolerDir string) {
	t.Helper()
	pgDataDir := filepath.Join(poolerDir, "pg_data")
	require.NoError(t, os.MkdirAll(pgDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pgDataDir, "PG_VERSION"), []byte("16"), 0o644))
	t.Setenv(constants.PgDataDirEnvVar, pgDataDir)
}

func TestReplicationStatus(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-service",
	}

	t.Run("PRIMARY_pooler_returns_primary_status", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create PRIMARY multipooler
		multipooler := &clustermetadatapb.Multipooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			// A PRIMARY record must name itself as leader (the record invariant).
			RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		mockQueryService := mock.NewQueryService()
		// The committed consensus position names self as leader, so the derived
		// routing role is PRIMARY (with postgres out of recovery below), matching the
		// seeded PRIMARY label rather than the monitor reconciling it to REPLICA.
		pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
			withMockController(&mockPoolerController{queryService: mockQueryService}),
			withFakeRules(&fakeRuleStore{pos: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					LeaderId:   serviceID,
				}},
			}}))
		require.NoError(t, err)
		t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })

		// Status() calls postgresMode() to determine role
		// pg_is_in_recovery returns false (not in recovery = primary)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		// getPrimaryLSN()
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/12345678"}}))
		// getConnectedFollowerIDs()
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		// replication GUCs (synchronous config + max_wal_senders)
		mockQueryService.AddQueryPattern("SELECT current_setting",
			mock.MakeQueryResult(
				[]string{"synchronous_standby_names", "synchronous_commit", "max_wal_senders"},
				[][]any{{"", "on", "5"}}))

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// PoolerType is now derived from the live routing role, which the postgres
		// monitor converges once postgres is probed (running, out of recovery) and the
		// consensus rule names self. The fast-start monitor tick can race ahead of
		// Ready, so drive an explicit iteration and wait for the derived PRIMARY.
		require.Eventually(t, func() bool {
			_, _ = pm.monitorPostgresIteration(ctx)
			return pm.stateManager.RoutingRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
		}, 5*time.Second, 50*time.Millisecond, "monitor should derive PRIMARY routing role")

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.Status.PoolerType)
		assert.NotNil(t, status.Status.PrimaryStatus, "PrimaryStatus should be populated")
		assert.Nil(t, status.Status.ReplicationStatus, "ReplicationStatus should be nil for PRIMARY")
		assert.Equal(t, "0/12345678", status.Status.PrimaryStatus.Lsn)
	})

	t.Run("REPLICA_pooler_returns_replication_status", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create REPLICA multipooler
		multipooler := &clustermetadatapb.Multipooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

		tmpDir := t.TempDir()
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		mockQueryService := mock.NewQueryService()
		pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
			withMockController(&mockPoolerController{queryService: mockQueryService}),
			withFakeRules(&fakeRuleStore{}))
		require.NoError(t, err)
		t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })
		// Mark as initialized to skip auto-restore (not testing backup functionality)
		err = pm.setInitialized()
		require.NoError(t, err)

		// Status() calls postgresMode() - returns true (in recovery = standby)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
		// getStandbyReplayLSN()
		mockQueryService.AddQueryPattern("SELECT pg_last_wal_replay_lsn",
			mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/12345600"}}))
		// queryReplicationStatus()
		mockQueryService.AddQueryPattern("pg_last_wal_receive_lsn",
			mock.MakeQueryResult(
				[]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"primary_conninfo",
					"wal_receiver_status",
					"last_msg_receive_time",
					"wal_receiver_status_interval",
					"wal_receiver_timeout",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming", nil, nil, nil}}))

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call ReplicationStatus
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// Verify response structure
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.Status.PoolerType)
		assert.Nil(t, status.Status.PrimaryStatus, "PrimaryStatus should be nil for REPLICA")
		assert.NotNil(t, status.Status.ReplicationStatus, "ReplicationStatus should be populated")
		assert.Equal(t, "0/12345600", status.Status.ReplicationStatus.LastReplayLsn)
	})

	t.Run("Mismatch_PRIMARY_topology_but_standby_postgres", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create PRIMARY multipooler (but PG will be in standby mode - mismatch!)
		multipooler := &clustermetadatapb.Multipooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			// A PRIMARY record must name itself as leader (the record invariant).
			RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		mockQueryService := mock.NewQueryService()
		pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
			withMockController(&mockPoolerController{queryService: mockQueryService}),
			withFakeRules(&fakeRuleStore{}))
		require.NoError(t, err)
		t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })

		// PostgreSQL is actually a standby (pg_is_in_recovery = true)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"t"}}))
		// getStandbyReplayLSN()
		mockQueryService.AddQueryPattern("SELECT pg_last_wal_replay_lsn",
			mock.MakeQueryResult([]string{"pg_last_wal_replay_lsn"}, [][]any{{"0/12345600"}}))
		// queryReplicationStatus()
		mockQueryService.AddQueryPattern("pg_last_wal_receive_lsn",
			mock.MakeQueryResult(
				[]string{
					"pg_last_wal_replay_lsn",
					"pg_last_wal_receive_lsn",
					"pg_is_wal_replay_paused",
					"pg_get_wal_replay_pause_state",
					"pg_last_xact_replay_timestamp",
					"primary_conninfo",
					"wal_receiver_status",
					"last_msg_receive_time",
					"wal_receiver_status_interval",
					"wal_receiver_timeout",
				},
				[][]any{{"0/12345600", "0/12345678", "f", "not paused", "2025-01-01 00:00:00", "host=primary port=5432 user=repl application_name=test", "streaming", nil, nil, nil}}))

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// The record is seeded PRIMARY, but PoolerType is now derived from the live
		// routing role: postgres reports recovery (standby), so the routing role is
		// REPLICA and the monitor reconciles the label to REPLICA. Status therefore
		// reports REPLICA with a populated ReplicationStatus (not a lasting
		// PRIMARY-label / standby-postgres mismatch, which the derived model prevents).
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.Status.PoolerType)
		assert.Nil(t, status.Status.PrimaryStatus, "PrimaryStatus should be nil since PostgreSQL is a standby")
		assert.NotNil(t, status.Status.ReplicationStatus, "ReplicationStatus should be populated since PostgreSQL is a standby")
	})

	t.Run("Status_returns_cohort_members_from_leadership_history", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		multipooler := &clustermetadatapb.Multipooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_PRIMARY,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			// A PRIMARY record must name itself as leader (the record invariant).
			RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

		tmpDir := t.TempDir()
		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		mockQueryService := mock.NewQueryService()
		pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
			withMockController(&mockPoolerController{queryService: mockQueryService}),
			withFakeRules(&fakeRuleStore{
				pos: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
						CohortMembers: []*clustermetadatapb.ID{
							{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-a"},
							{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler-b"},
						},
					}},
					Lsn: "0/1000000",
				},
			}))
		require.NoError(t, err)
		t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })

		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/1000000"}}))
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		mockQueryService.AddQueryPattern("SELECT current_setting",
			mock.MakeQueryResult(
				[]string{"synchronous_standby_names", "synchronous_commit", "max_wal_senders"},
				[][]any{{"", "on", "5"}}))

		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		cohortMembers := status.ConsensusStatus.GetCurrentPosition().GetPosition().GetDecision().GetCohortMembers()
		require.Len(t, cohortMembers, 2)
		assert.Equal(t, "zone1", cohortMembers[0].Cell)
		assert.Equal(t, "pooler-a", cohortMembers[0].Name)
		assert.Equal(t, clustermetadatapb.ID_MULTIPOOLER, cohortMembers[0].Component)
		assert.Equal(t, "zone1", cohortMembers[1].Cell)
		assert.Equal(t, "pooler-b", cohortMembers[1].Name)
	})

	t.Run("Mismatch_REPLICA_topology_but_primary_postgres", func(t *testing.T) {
		ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
		defer ts.Close()

		pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, &testutil.MockPgCtldService{})
		t.Cleanup(cleanupPgctld)

		// Create the database in topology with backup location
		database := "testdb"
		addDatabaseToTopo(t, ts, database)

		// Create REPLICA multipooler (but PG will be in primary mode - mismatch!)
		multipooler := &clustermetadatapb.Multipooler{
			Id:            serviceID,
			Hostname:      "localhost",
			PortMap:       map[string]int32{"grpc": 8080},
			Type:          clustermetadatapb.PoolerType_REPLICA,
			ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   database,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		}
		require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

		tmpDir := t.TempDir()
		createPgDataDir(t, tmpDir)

		multipooler.PoolerDir = tmpDir

		config := &Config{
			TopoClient: ts,
			PgctldAddr: pgctldAddr,
		}
		mockQueryService := mock.NewQueryService()
		pm, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
			withMockController(&mockPoolerController{queryService: mockQueryService}),
			withFakeRules(&fakeRuleStore{}))
		require.NoError(t, err)
		t.Cleanup(func() { pm.ShutdownForTest(context.Background()) })
		// Mark as initialized to skip auto-restore (not testing backup functionality)
		err = pm.setInitialized()
		require.NoError(t, err)

		// PostgreSQL is actually a primary (pg_is_in_recovery = false)
		mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
			mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{"f"}}))
		// getPrimaryLSN()
		mockQueryService.AddQueryPattern("SELECT pg_current_wal_lsn",
			mock.MakeQueryResult([]string{"pg_current_wal_lsn"}, [][]any{{"0/12345678"}}))
		// getConnectedFollowerIDs()
		mockQueryService.AddQueryPattern("SELECT application_name",
			mock.MakeQueryResult([]string{"application_name"}, nil))
		// replication GUCs (synchronous config + max_wal_senders)
		mockQueryService.AddQueryPattern("SELECT current_setting",
			mock.MakeQueryResult(
				[]string{"synchronous_standby_names", "synchronous_commit", "max_wal_senders"},
				[][]any{{"", "on", "5"}}))

		senv := servenv.NewServEnv(viperutil.NewRegistry())
		go pm.Start(senv)

		require.Eventually(t, func() bool {
			return pm.GetState() == ManagerStateReady
		}, 5*time.Second, 100*time.Millisecond, "Manager should reach Ready state")

		// Call Status - now returns status with mismatch observable
		status, err := pm.Status(ctx)
		require.NoError(t, err)
		require.NotNil(t, status)

		// PoolerType from topology says REPLICA, but status shows primary state
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.Status.PoolerType)
		assert.NotNil(t, status.Status.PrimaryStatus, "PrimaryStatus should be populated since PostgreSQL is a primary")
		assert.Nil(t, status.Status.ReplicationStatus, "ReplicationStatus should be nil since PostgreSQL is a primary")
	})
}

func TestUpdateConsensusRule_HistoryFailurePreventsGUCUpdate(t *testing.T) {
	// This test verifies that if UpdateRule fails during
	// UpdateConsensusRule, the synchronous_standby_names GUC is NOT updated.

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-primary",
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	createPgDataDir(t, poolerDir)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	multipooler := &clustermetadatapb.Multipooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		// A PRIMARY record must name itself as leader (the record invariant).
		RoutingState: &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, multipooler))

	multipooler.PoolerDir = poolerDir

	// Set consensus term
	consensustest.SeedTerm(t, poolerDir, &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 5,
	})

	config := &Config{
		TopoClient: ts,
	}
	mockQueryService := mock.NewQueryService()
	// ObservePosition must succeed so UpdateCohortMembers reaches UpdateRule.
	// updateErr simulates the history write timing out (the failure we're testing).
	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}),
		withFakeRules(&fakeRuleStore{
			pos: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					CohortMembers: []*clustermetadatapb.ID{
						{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-1"},
						{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-2"},
					},
					DurabilityPolicy: testBootstrapPolicy(),
				}},
			},
			updateErr: mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED, "timeout waiting for sync replication"),
		}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	// Load the seeded term from disk (promises is rooted at the pooler dir).
	_, err = manager.consensusMgr.Promises().Load()
	require.NoError(t, err, "Failed to load consensus state")

	// Mock for startup
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	err = manager.setInitialized()
	require.NoError(t, err)

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond)

	// Mock the replication-GUC read (called to get current config)
	// Returns current config with 2 standbys
	mockQueryService.AddQueryPattern("SELECT current_setting",
		mock.MakeQueryResult(
			[]string{"synchronous_standby_names", "synchronous_commit", "max_wal_senders"},
			[][]any{{"FIRST 1 (zone1_replica-1, zone1_replica-2)", "remote_write", "5"}}))

	// We do NOT add expectations for ALTER SYSTEM SET synchronous_standby_names
	// If it gets called, ExpectationsWereMet() will fail

	// Call UpdateConsensusRule to add a new standby
	newStandby := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-3"}

	_, err = manager.UpdateConsensusRule(
		ctx,
		multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD,
		[]*clustermetadatapb.ID{newStandby},
		&clustermetadatapb.RuleNumber{CoordinatorTerm: 5}, // expectedOutgoingRule
		nil, // coordinatorID
	)

	// Verify it failed
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to record replication config history")

	// CRITICAL: Verify that NO ALTER SYSTEM queries were executed
	assert.NoError(t, mockQueryService.ExpectationsWereMet(),
		"If this fails, it means SetPolicy was called despite history insert failure")
}

// TestUpdateConsensusRule_RejectsWhenSelfRevoked verifies the defense-in-depth
// guardrail: a primary whose own committed rule is already revoked by its own
// accepted promise must refuse to write a further rule, rather than relying
// solely on the indirect out-of-recovery side effect of Recruit's demotion.
func TestUpdateConsensusRule_RejectsWhenSelfRevoked(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-primary",
	}

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	createPgDataDir(t, poolerDir)

	database := "testdb"
	addDatabaseToTopo(t, ts, database)

	multipooler := &clustermetadatapb.Multipooler{
		Id:            serviceID,
		Hostname:      "localhost",
		PortMap:       map[string]int32{"grpc": 8080, "postgres": 5432},
		Type:          clustermetadatapb.PoolerType_PRIMARY,
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		RoutingState:  &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}
	require.NoError(t, ts.CreateMultipooler(ctx, multipooler))
	multipooler.PoolerDir = poolerDir

	// This pooler's own committed rule is at term 5, but it has already
	// accepted a revocation below term 6 anchored on that same rule — a
	// coordinator recruited it into a newer term. Its own commit is now
	// self-revoked, even though nothing has restarted it as standby yet.
	consensustest.SeedTerm(t, poolerDir, &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 6,
		OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
	})

	config := &Config{TopoClient: ts}
	mockQueryService := mock.NewQueryService()
	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}),
		withFakeRules(&fakeRuleStore{
			pos: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					CohortMembers: []*clustermetadatapb.ID{
						{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica-1"},
					},
					DurabilityPolicy: testBootstrapPolicy(),
				}},
			},
		}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	_, err = manager.consensusMgr.Promises().Load()
	require.NoError(t, err)

	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{false}}))

	require.NoError(t, manager.setInitialized())

	senv := servenv.NewServEnv(viperutil.NewRegistry())
	go manager.Start(senv)
	require.Eventually(t, func() bool {
		return manager.GetState() == ManagerStateReady
	}, 5*time.Second, 100*time.Millisecond)

	newStandby := &clustermetadatapb.ID{Cell: "zone1", Name: "replica-2"}
	_, err = manager.UpdateConsensusRule(
		ctx,
		multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD,
		[]*clustermetadatapb.ID{newStandby},
		&clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
		nil,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "term has been revoked")
	// No cohort/GUC queries beyond the recovery-mode check above.
	assert.NoError(t, mockQueryService.ExpectationsWereMet())
}

// rewindAsStandbyForTest drives the action-locked rewind core that used to sit
// behind the RewindToSource RPC (removed in favor of the monitor's local
// self-heal): it raises suspectedDivergence and calls restartAsStandbyLocked,
// the shared helper these regression tests exercise. Callers still reach it via
// SetPrimary's stale-primary branch and the monitor's divergence-rewind path.
func rewindAsStandbyForTest(t *testing.T, manager *MultipoolerManager, source *clustermetadatapb.Multipooler) (bool, error) {
	t.Helper()
	return rewindAsStandbyWithContextForTest(t, t.Context(), manager, source)
}

func rewindAsStandbyWithContextForTest(t *testing.T, ctx context.Context, manager *MultipoolerManager, source *clustermetadatapb.Multipooler) (bool, error) {
	t.Helper()
	lockCtx, err := manager.actionLock.Acquire(ctx, "test-rewind")
	require.NoError(t, err)
	defer manager.actionLock.Release(lockCtx)
	if _, err := manager.consensusMgr.SetSuspectedDivergence(lockCtx, true); err != nil {
		return false, err
	}
	return manager.restartAsStandbyLocked(lockCtx, source.Hostname, source.PortMap["postgres"])
}

// TestRestartAsStandby_RealRewindFailureQuarantines verifies that once the real
// pg_rewind has started, a failure prevents any postgres start on the potentially
// partial PGDATA and publishes a quarantine verdict for replacement.
func TestRestartAsStandby_RealRewindFailureQuarantines(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	multipooler := &clustermetadatapb.Multipooler{
		Id:        serviceID,
		PoolerDir: poolerDir,
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PortMap: map[string]int32{
			"postgres": 5432,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}

	mockPgctld := &testutil.MockPgCtldService{
		PgRewindFunc: func(_ context.Context, req *pgctldpb.PgRewindRequest) (*pgctldpb.PgRewindResponse, error) {
			assert.False(t, req.GetDryRun())
			assert.FileExists(t, filepath.Join(poolerDir, pgRewindSentinelFile))
			return nil, errors.New("mock real pg_rewind failed")
		},
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	// Create mock query service to avoid hanging during Open()
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	expectRewindPositionFloorMocks(mockQueryService)

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}

	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	// Create pg_data directory so setInitialized() can write marker file
	createPgDataDir(t, poolerDir)

	err = manager.setInitialized()
	require.NoError(t, err)

	// Assign mock pooler controller BEFORE opening to avoid race conditions

	// Simulate the manager being open and ready (set internal state without starting goroutines)
	manager.mu.Lock()
	manager.isOpen = true
	manager.state = ManagerStateReady
	manager.ctx, manager.cancel = context.WithCancel(ctx)
	manager.mu.Unlock()

	// Create a source pooler
	sourceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "source-pooler",
	}
	source := &clustermetadatapb.Multipooler{
		Id:       sourceID,
		Hostname: "source-host",
		PortMap: map[string]int32{
			"postgres": 5432,
		},
	}

	_, err = rewindAsStandbyForTest(t, manager, source)
	require.ErrorContains(t, err, "mock real pg_rewind failed")

	lifecycle := manager.record.Snapshot().GetLifecycleStatus()
	assert.Equal(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED, lifecycle.GetStatus())
	assert.Contains(t, lifecycle.GetReason(), "pg_rewind failed")
	assert.True(t, manager.postgresRestartsDisabled.Load(), "monitor must not start potentially partial PGDATA")
	assert.Equal(t, clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE, manager.consensusMgr.CohortEligibility())
	assert.Empty(t, mockPgctld.RestartCalls)
	assert.Empty(t, mockPgctld.StartCalls)
	assert.FileExists(t, filepath.Join(poolerDir, pgRewindSentinelFile), "failed rewind must leave durable sentinel")

	// Pause's deferred resume still reopens the manager so it can publish health.
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.isOpen
	}, 2*time.Second, 50*time.Millisecond)
}

// TestRestartAsStandby_RestoresPrimaryConnInfo is a regression test for the bug where
// the rewind path did not call setPrimaryConnInfoLocked after pg_rewind. When actual
// pg_rewind runs it syncs postgresql.auto.conf from the source (which has no
// primary_conninfo), wiping the value set by the earlier SetPrimary call and
// leaving the WAL receiver with no primary to connect to.
func TestRestartAsStandby_RestoresPrimaryConnInfo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	multipooler := &clustermetadatapb.Multipooler{
		Id:        serviceID,
		PoolerDir: poolerDir,
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PortMap: map[string]int32{
			"postgres": 5432,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}

	// Let the single pg_rewind call outlive the initiating RPC deadline. This
	// models the expensive analysis phase that previously died before reaching
	// the protected destructive pass.
	requestCtx, cancelRequest := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRequest()
	mockPgctld := &testutil.MockPgCtldService{}
	mockPgctld.PgRewindFunc = func(callCtx context.Context, req *pgctldpb.PgRewindRequest) (*pgctldpb.PgRewindResponse, error) {
		assert.False(t, req.GetDryRun())
		assert.FileExists(t, filepath.Join(poolerDir, pgRewindSentinelFile))
		<-requestCtx.Done()
		select {
		case <-time.After(100 * time.Millisecond):
		case <-callCtx.Done():
			return nil, errors.New("rewind maintenance did not outlive the initiating RPC")
		}
		return &pgctldpb.PgRewindResponse{
			Message: "pg_rewind completed",
			Output:  "servers diverged at WAL location 0/5000000 on timeline 1",
		}, nil
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	// Track whether primary_conninfo was set with the expected value.
	var primaryConnInfoSet string

	mockQueryService := mock.NewQueryService()
	// Registered first so its one-shot reload patterns are consumed by this
	// test's earlier (rewind-path) reload, not the later setPrimaryConnInfoLocked
	// one below — see expectRewindPositionFloorMocks's doc comment.
	expectRewindPositionFloorMocks(mockQueryService)
	// SELECT 1 for waitForDatabaseConnection
	mockQueryService.AddQueryPattern("SELECT 1",
		mock.MakeQueryResult([]string{"?column?"}, [][]any{{1}}))
	// pg_is_in_recovery check inside setPrimaryConnInfoLocked
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	// Record that ALTER SYSTEM SET primary_conninfo was called
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(query string) { primaryConnInfoSet = query },
	)
	// config load time before reload, read as a Unix epoch (consumed once)
	mockQueryService.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067200"}}))
	// pg_reload_conf
	mockQueryService.AddQueryPattern("SELECT pg_reload_conf",
		mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	// config load time after reload (different value signals reload completed)
	mockQueryService.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067201"}}))

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}

	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	createPgDataDir(t, poolerDir)
	require.NoError(t, manager.setInitialized())

	// Seed postgresql.auto.conf as pg_rewind leaves it: copied from the source
	// primary, so no primary_conninfo entry.
	autoConfPath := filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf")
	require.NoError(t, os.WriteFile(autoConfPath, []byte("# Do not edit this file manually!\n"), 0o600))

	// Snapshot auto.conf at the moment pgctld is asked to restart postgres. The
	// deadlock this guards against (2026-08-12 incident): conninfo arriving only
	// via SQL after start, which a standby that needs leader WAL to reach
	// consistency can never accept.
	var autoConfAtRestart string
	mockPgctld.RestartFunc = func(*pgctldpb.RestartRequest) {
		b, _ := os.ReadFile(autoConfPath)
		autoConfAtRestart = string(b)
	}

	manager.mu.Lock()
	manager.isOpen = true
	manager.state = ManagerStateReady
	manager.ctx, manager.cancel = context.WithCancel(ctx)
	manager.mu.Unlock()

	source := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "source-pooler",
		},
		Hostname: "source-host",
		PortMap:  map[string]int32{"postgres": 5433},
	}

	rewindPerformed, err := rewindAsStandbyWithContextForTest(t, requestCtx, manager, source)
	require.NoError(t, err)
	require.ErrorIs(t, requestCtx.Err(), context.DeadlineExceeded)
	assert.True(t, rewindPerformed, "actual pg_rewind should have run (divergence detected)")

	// REGRESSION TEST: primary_conninfo must already be in postgresql.auto.conf
	// when postgres restarts, so a standby that cannot reach consistency (and
	// thus never accepts the post-start SQL write below) still comes up
	// streaming from the leader instead of held blind.
	assert.Contains(t, autoConfAtRestart, "primary_conninfo = 'host=source-host port=5433 ",
		"auto.conf must carry the source's primary_conninfo before the standby starts")

	// The analysis and any writes happen in one protected pg_rewind call.
	rewindCalls := mockPgctld.PgRewindCalls
	require.Len(t, rewindCalls, 1)
	assert.False(t, rewindCalls[0].DryRun)

	// REGRESSION TEST: primary_conninfo must be set after pg_rewind so the WAL
	// receiver can connect to the primary. Before the fix, this was never called
	// and the WAL receiver had no primary_conninfo to use.
	assert.NotEmpty(t, primaryConnInfoSet, "primary_conninfo must be set after pg_rewind")
	assert.Contains(t, primaryConnInfoSet, "source-host", "primary_conninfo must reference the source host")
	assert.Contains(t, primaryConnInfoSet, "5433", "primary_conninfo must reference the source postgres port")
	assert.NoFileExists(t, filepath.Join(poolerDir, pgRewindSentinelFile), "verified restart must clear rewind sentinel")
}

// TestRestartAsStandby_NoDivergence_StillSetsPrimaryConnInfo guards the helper
// contract introduced in the unified-rewind refactor: restartAsStandbyLocked
// sets primary_conninfo when pg_rewind reports that no rewind is required.
func TestRestartAsStandby_NoDivergence_StillSetsPrimaryConnInfo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerDir := t.TempDir()
	serviceID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "zone1",
		Name:      "test-pooler",
	}

	multipooler := &clustermetadatapb.Multipooler{
		Id:        serviceID,
		PoolerDir: poolerDir,
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PortMap: map[string]int32{
			"postgres": 5432,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}

	// The single protected call lets pg_rewind make the divergence decision and
	// return without mutation when no rewind is required.
	mockPgctld := &testutil.MockPgCtldService{
		PgRewindResponse: &pgctldpb.PgRewindResponse{
			Message: "pg_rewind completed",
			Output:  "servers diverged at WAL location 0/5000000 on timeline 1\nno rewind required",
		},
	}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	var primaryConnInfoSet string

	mockQueryService := mock.NewQueryService()
	// Registered first so its one-shot reload patterns are consumed by this
	// test's earlier (rewind-path) reload, not the later setPrimaryConnInfoLocked
	// one below — see expectRewindPositionFloorMocks's doc comment.
	expectRewindPositionFloorMocks(mockQueryService)
	mockQueryService.AddQueryPattern("SELECT 1",
		mock.MakeQueryResult([]string{"?column?"}, [][]any{{1}}))
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(query string) { primaryConnInfoSet = query },
	)
	mockQueryService.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067200"}}))
	mockQueryService.AddQueryPattern("SELECT pg_reload_conf",
		mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	mockQueryService.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067201"}}))

	config := &Config{
		TopoClient: ts,
		PgctldAddr: pgctldAddr,
	}

	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, config,
		withMockController(&mockPoolerController{queryService: mockQueryService}))
	require.NoError(t, err)
	defer manager.ShutdownForTest(t.Context())

	createPgDataDir(t, poolerDir)
	require.NoError(t, manager.setInitialized())

	// Seed postgresql.auto.conf with a stale conninfo persisted by a previous
	// run: the pre-start file write must replace it in place, not stack a
	// second entry.
	autoConfPath := filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf")
	require.NoError(t, os.WriteFile(autoConfPath,
		[]byte("primary_conninfo = 'host=old-leader port=5432 user=postgres application_name=stale'\n"), 0o600))

	var autoConfAtRestart string
	mockPgctld.RestartFunc = func(*pgctldpb.RestartRequest) {
		b, _ := os.ReadFile(autoConfPath)
		autoConfAtRestart = string(b)
	}

	manager.mu.Lock()
	manager.isOpen = true
	manager.state = ManagerStateReady
	manager.ctx, manager.cancel = context.WithCancel(ctx)
	manager.mu.Unlock()

	source := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "source-pooler",
		},
		Hostname: "source-host",
		PortMap:  map[string]int32{"postgres": 5433},
	}

	rewindPerformed, err := rewindAsStandbyForTest(t, manager, source)
	require.NoError(t, err)
	assert.False(t, rewindPerformed, "pg_rewind reported that no rewrite was required")

	// The no-rewind path must also carry conninfo in auto.conf before the
	// restart, with the stale entry replaced rather than shadowing the new one.
	assert.Contains(t, autoConfAtRestart, "primary_conninfo = 'host=source-host port=5433 ",
		"auto.conf must carry the source's primary_conninfo before the standby starts")
	assert.NotContains(t, autoConfAtRestart, "old-leader",
		"the stale conninfo entry must be replaced, not left behind")

	// One non-dry-run call performs both the decision and any needed rewrite.
	rewindCalls := mockPgctld.PgRewindCalls
	require.Len(t, rewindCalls, 1)
	assert.False(t, rewindCalls[0].DryRun)

	// The contract: primary_conninfo gets set even when no rewind happens.
	assert.NotEmpty(t, primaryConnInfoSet, "primary_conninfo must be set even when no rewind runs")
	assert.Contains(t, primaryConnInfoSet, "source-host", "primary_conninfo must reference the source host")
	assert.Contains(t, primaryConnInfoSet, "5433", "primary_conninfo must reference the source postgres port")
}

// restartAsStandbyNoRewindForTest drives restartAsStandbyLocked WITHOUT raising
// suspectedDivergence, exercising the plain stop/restart path (no pg_rewind, no
// position-floor measurement).
func restartAsStandbyNoRewindForTest(t *testing.T, manager *MultipoolerManager, host string, port int32) error {
	t.Helper()
	lockCtx, err := manager.actionLock.Acquire(t.Context(), "test-restart")
	require.NoError(t, err)
	defer manager.actionLock.Release(lockCtx)
	_, err = manager.restartAsStandbyLocked(lockCtx, host, port)
	return err
}

// newRestartAsStandbyTestManager builds the manager + mock pgctld boilerplate
// shared by the pre-start conninfo file-write tests: a REPLICA pooler with a
// seeded postgresql.auto.conf and a RestartFunc snapshot capturing the file
// content at the moment pgctld is asked to restart postgres.
func newRestartAsStandbyTestManager(t *testing.T, mockQueryService *mock.QueryService, seedAutoConf string) (manager *MultipoolerManager, autoConfPath string, autoConfAtRestart *string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "zone1")
	t.Cleanup(func() { _ = ts.Close() })

	poolerDir := t.TempDir()
	multipooler := &clustermetadatapb.Multipooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      "test-pooler",
		},
		PoolerDir: poolerDir,
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PortMap:   map[string]int32{"postgres": 5432},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   "postgres",
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
	}

	mockPgctld := &testutil.MockPgCtldService{}
	pgctldAddr, cleanupPgctld := testutil.StartMockPgctldServer(t, mockPgctld)
	t.Cleanup(cleanupPgctld)

	manager, err := NewMultipoolerManagerForTesting(t, logger, multipooler, &Config{TopoClient: ts, PgctldAddr: pgctldAddr},
		withMockController(&mockPoolerController{queryService: mockQueryService}))
	require.NoError(t, err)
	t.Cleanup(func() { manager.ShutdownForTest(t.Context()) })

	createPgDataDir(t, poolerDir)
	require.NoError(t, manager.setInitialized())

	autoConfPath = filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf")
	require.NoError(t, os.WriteFile(autoConfPath, []byte(seedAutoConf), 0o600))

	snapshot := new(string)
	mockPgctld.RestartFunc = func(*pgctldpb.RestartRequest) {
		b, _ := os.ReadFile(autoConfPath)
		*snapshot = string(b)
	}

	manager.mu.Lock()
	manager.isOpen = true
	manager.state = ManagerStateReady
	manager.ctx, manager.cancel = context.WithCancel(context.Background())
	manager.mu.Unlock()

	return manager, autoConfPath, snapshot
}

// TestRestartAsStandby_ManualStopSkipsConnInfoFileWrite: when StopReplication
// has set the manual-stop flag, the pre-start file write must NOT bypass the
// admin pause — auto.conf stays untouched and the post-start SQL path refuses
// loudly, preserving the pre-file-write behavior.
func TestRestartAsStandby_ManualStopSkipsConnInfoFileWrite(t *testing.T) {
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT 1",
		mock.MakeQueryResult([]string{"?column?"}, [][]any{{1}}))
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))

	const stale = "primary_conninfo = 'host=old-leader port=5432 user=postgres application_name=stale'\n"
	manager, autoConfPath, autoConfAtRestart := newRestartAsStandbyTestManager(t, mockQueryService, stale)

	manager.walReceiverManuallyStopped.Store(true)

	err := restartAsStandbyNoRewindForTest(t, manager, "source-host", 5433)
	require.Error(t, err, "restart must fail: the SQL conninfo path refuses while replication is manually stopped")
	assert.Contains(t, err.Error(), "manually stopped")

	// The admin pause held: no conninfo write happened, by file or by SQL.
	assert.Equal(t, stale, *autoConfAtRestart, "auto.conf must be untouched at restart time")
	b, readErr := os.ReadFile(autoConfPath)
	require.NoError(t, readErr)
	assert.Equal(t, stale, string(b), "auto.conf must remain untouched after the refused restart")
}

// TestRestartAsStandby_FileWriteFailureFallsBackToSQL: the pre-start file write
// is best-effort — when it fails (unwritable auto.conf here), the restart still
// proceeds and the post-start SQL path establishes the conninfo, preserving the
// helper's "working standby of source on return" contract for nodes that can
// accept connections.
func TestRestartAsStandby_FileWriteFailureFallsBackToSQL(t *testing.T) {
	var primaryConnInfoSet string
	mockQueryService := mock.NewQueryService()
	mockQueryService.AddQueryPattern("SELECT 1",
		mock.MakeQueryResult([]string{"?column?"}, [][]any{{1}}))
	mockQueryService.AddQueryPattern("SELECT pg_is_in_recovery",
		mock.MakeQueryResult([]string{"pg_is_in_recovery"}, [][]any{{true}}))
	mockQueryService.AddQueryPatternWithCallback(
		"ALTER SYSTEM SET primary_conninfo",
		mock.MakeQueryResult(nil, nil),
		func(query string) { primaryConnInfoSet = query },
	)
	mockQueryService.AddQueryPatternOnce("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067200"}}))
	mockQueryService.AddQueryPattern("SELECT pg_reload_conf",
		mock.MakeQueryResult([]string{"pg_reload_conf"}, [][]any{{true}}))
	mockQueryService.AddQueryPattern("pg_conf_load_time",
		mock.MakeQueryResult([]string{"date_part"}, [][]any{{"1704067201"}}))

	const stale = "primary_conninfo = 'host=old-leader port=5432 user=postgres application_name=stale'\n"
	manager, autoConfPath, autoConfAtRestart := newRestartAsStandbyTestManager(t, mockQueryService, stale)

	// Make the file unwritable so the pre-start write fails (logged, not fatal).
	require.NoError(t, os.Chmod(autoConfPath, 0o400))

	err := restartAsStandbyNoRewindForTest(t, manager, "source-host", 5433)
	require.NoError(t, err, "restart must succeed despite the failed file write")

	assert.Contains(t, *autoConfAtRestart, "old-leader",
		"file write failed, so auto.conf still carries the stale entry at restart")
	assert.Contains(t, primaryConnInfoSet, "source-host",
		"the SQL fallback must establish the conninfo after restart")
}

// TestSetAutoConfSetting covers the file-edit SET variant used to write
// primary_conninfo while postgres is stopped (the pre-start write in
// restartAsStandbyLocked): append when absent, replace in place, idempotent
// no-op, token-boundary name matching, GUC-file quoting, and duplicate
// collapse.
func TestSetAutoConfSetting(t *testing.T) {
	ctx := t.Context()

	setup := func(t *testing.T, content string) (pm *MultipoolerManager, autoConfPath string) {
		t.Helper()
		dir := t.TempDir()
		t.Setenv(constants.PgDataDirEnvVar, dir)
		autoConfPath = filepath.Join(dir, "postgresql.auto.conf")
		require.NoError(t, os.WriteFile(autoConfPath, []byte(content), 0o600))
		return &MultipoolerManager{logger: slog.Default()}, autoConfPath
	}
	read := func(t *testing.T, path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		return string(b)
	}

	t.Run("appends_when_absent", func(t *testing.T) {
		pm, path := setup(t, "# Do not edit this file manually!\nrestore_command = 'pgbackrest restore'\n")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 port=5432"))
		assert.Equal(t,
			"# Do not edit this file manually!\nrestore_command = 'pgbackrest restore'\nprimary_conninfo = 'host=h1 port=5432'\n",
			read(t, path))
	})

	t.Run("appends_to_empty_file", func(t *testing.T) {
		pm, path := setup(t, "")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 port=5432"))
		assert.Equal(t, "primary_conninfo = 'host=h1 port=5432'\n", read(t, path))
	})

	t.Run("replaces_in_place", func(t *testing.T) {
		pm, path := setup(t, "primary_conninfo = 'host=old port=1'\nrestore_command = 'pgbackrest restore'\n")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=new port=2"))
		assert.Equal(t,
			"primary_conninfo = 'host=new port=2'\nrestore_command = 'pgbackrest restore'\n",
			read(t, path))
	})

	t.Run("idempotent_skips_write", func(t *testing.T) {
		pm, path := setup(t, "")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 port=5432"))
		before := read(t, path)
		// Make the file unwritable: a second identical set must not attempt a
		// write at all, so it succeeds despite the permissions.
		require.NoError(t, os.Chmod(path, 0o400))
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 port=5432"))
		assert.Equal(t, before, read(t, path))
	})

	t.Run("respects_token_boundaries", func(t *testing.T) {
		pm, path := setup(t, "primary_conninfo_extra = 'keepme'\n")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 port=5432"))
		assert.Equal(t,
			"primary_conninfo_extra = 'keepme'\nprimary_conninfo = 'host=h1 port=5432'\n",
			read(t, path))
	})

	t.Run("quotes_embedded_single_quotes", func(t *testing.T) {
		pm, path := setup(t, "")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1 password=pa'ss"))
		assert.Equal(t, "primary_conninfo = 'host=h1 password=pa''ss'\n", read(t, path))
	})

	t.Run("doubles_backslashes", func(t *testing.T) {
		// The GUC config-file lexer processes backslash escapes inside quoted
		// values, so a lone backslash would be reinterpreted (e.g. \t → tab).
		// Doubling matches ALTER SYSTEM's own writer.
		pm, path := setup(t, "")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", `host=h1 options=-c\ttl=1`))
		assert.Equal(t, `primary_conninfo = 'host=h1 options=-c\\ttl=1'`+"\n", read(t, path))
	})

	t.Run("collapses_duplicates", func(t *testing.T) {
		// Postgres reads the last occurrence, so a surviving stale duplicate
		// below the fresh entry would override it.
		pm, path := setup(t, "primary_conninfo = 'host=a'\nprimary_conninfo = 'host=b'\n")
		require.NoError(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=new"))
		assert.Equal(t, "primary_conninfo = 'host=new'\n", read(t, path))
	})

	t.Run("errors_when_file_missing", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(constants.PgDataDirEnvVar, dir)
		pm := &MultipoolerManager{logger: slog.Default()}
		assert.Error(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1"))
	})

	t.Run("rejects_invalid_names", func(t *testing.T) {
		// A name with spaces, '=', quotes, or line breaks would corrupt the
		// line-oriented file; a caller passing one is a bug worth surfacing.
		pm, path := setup(t, "")
		for _, name := range []string{"", "primary conninfo", "1abc", "name=inject", "a.b.c", "na'me", "nl\nname"} {
			assert.Error(t, pm.setAutoConfSetting(ctx, name, "host=h1"), "name %q must be rejected", name)
		}
		// Valid forms: plain and extension-qualified.
		require.NoError(t, pm.setAutoConfSetting(ctx, "my_ext.some_guc", "v"))
		assert.Contains(t, read(t, path), "my_ext.some_guc = 'v'\n")
	})

	t.Run("rejects_line_break_values", func(t *testing.T) {
		pm, path := setup(t, "")
		assert.Error(t, pm.setAutoConfSetting(ctx, "primary_conninfo", "host=h1\nmalicious=x"))
		assert.Equal(t, "", read(t, path), "file must be untouched after rejection")
	})
}

func TestSetPostgresRestartsEnabledRPC(t *testing.T) {
	ctx := t.Context()

	t.Run("disable", func(t *testing.T) {
		pm := &MultipoolerManager{logger: slog.Default()}

		resp, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, pm.postgresRestartsDisabled.Load(), "restarts should be disabled after RPC")
	})

	t.Run("enable", func(t *testing.T) {
		pm := &MultipoolerManager{logger: slog.Default()}
		pm.postgresRestartsDisabled.Store(true)

		resp, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.False(t, pm.postgresRestartsDisabled.Load(), "restarts should be enabled after RPC")
	})

	t.Run("idempotent_disable", func(t *testing.T) {
		pm := &MultipoolerManager{logger: slog.Default()}

		_, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		_, err = pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err)
		assert.True(t, pm.postgresRestartsDisabled.Load())
	})

	t.Run("idempotent_enable", func(t *testing.T) {
		pm := &MultipoolerManager{logger: slog.Default()}
		pm.postgresRestartsDisabled.Store(true)

		_, err := pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		_, err = pm.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err)
		assert.False(t, pm.postgresRestartsDisabled.Load())
	})
}
