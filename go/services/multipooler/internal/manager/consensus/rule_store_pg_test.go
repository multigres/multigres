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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	testutils "github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/pathutil"
)

// pgTestFixture is the lazily-started postgres instance shared by all PG tests.
// It is nil until the first PG test triggers initialisation via skipIfNoPG.
var (
	pgTestFixture *pgPostgresFixture
	pgTestOnce    sync.Once
	pgTestErr     error
)

// pgPostgresFixture holds a running postgres instance.
type pgPostgresFixture struct {
	dataDir   string // root temp directory
	pgDataDir string // postgres data directory (dataDir/pg_data)
	socketDir string // unix socket directory (dataDir/pg_sockets)
}

// socketFile returns the path to the postgres unix domain socket.
// With listen_addresses=” postgres never binds TCP, so 5432 here is purely
// the filename convention (.s.PGSQL.<port>).  Isolation between concurrent
// test binaries comes from each fixture having its own unique socketDir, not
// from the port number.
func (f *pgPostgresFixture) socketFile() string {
	return filepath.Join(f.socketDir, ".s.PGSQL.5432")
}

// newClientConn opens a new postgres connection via the unix socket.
// The returned Conn must be closed by the caller.
func (f *pgPostgresFixture) newClientConn(ctx context.Context) (*client.Conn, error) {
	return client.Connect(ctx, ctx, &client.Config{
		SocketFile: f.socketFile(),
		User:       "postgres",
		Database:   "postgres",
	})
}

// connQueryService wraps a *client.Conn to implement executor.InternalQueryService.
// Each instance is single-threaded; callers must not share across goroutines.
type connQueryService struct {
	conn *client.Conn
}

var _ executor.InternalQueryService = (*connQueryService)(nil)

func (s *connQueryService) Query(ctx context.Context, query string) (*sqltypes.Result, error) {
	results, err := s.conn.QueryArgs(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}
	return results[0], nil
}

func (s *connQueryService) QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error) {
	results, err := s.conn.QueryArgs(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}
	return results[0], nil
}

func (s *connQueryService) QueryMultiStatement(ctx context.Context, query string) error {
	_, err := s.conn.Query(ctx, query)
	return err
}

func (s *connQueryService) Begin(ctx context.Context) (executor.InternalTx, error) {
	if _, err := s.conn.Query(ctx, "BEGIN"); err != nil {
		return nil, err
	}
	return &connTx{conn: s.conn}, nil
}

// connTx implements executor.InternalTx over a single *client.Conn for tests.
type connTx struct {
	conn     *client.Conn
	finished bool
}

func (tx *connTx) Query(ctx context.Context, query string) (*sqltypes.Result, error) {
	if tx.finished {
		return nil, errors.New("transaction already finished")
	}
	results, err := tx.conn.QueryArgs(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}
	return results[0], nil
}

func (tx *connTx) QueryArgs(ctx context.Context, query string, args ...any) (*sqltypes.Result, error) {
	if tx.finished {
		return nil, errors.New("transaction already finished")
	}
	results, err := tx.conn.QueryArgs(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return &sqltypes.Result{}, nil
	}
	return results[0], nil
}

func (tx *connTx) Commit(ctx context.Context) error {
	if tx.finished {
		return errors.New("transaction already finished")
	}
	tx.finished = true
	_, err := tx.conn.Query(ctx, "COMMIT")
	return err
}

func (tx *connTx) Rollback(ctx context.Context) error {
	if tx.finished {
		return nil
	}
	tx.finished = true
	_, err := tx.conn.Query(ctx, "ROLLBACK")
	return err
}

// TestMain sets up the PATH for PG binaries and the orphan-detection watchdog,
// runs the tests, and tears down the postgres instance if it was started.
// Postgres is started lazily by the first PG test (via skipIfNoPG) so that
// tests without PG dependencies are unaffected.
func TestMain(m *testing.M) {
	// Add bin/ and go/test/endtoend/ to PATH so run_command_if_parent_dies.sh
	// and PostgreSQL binaries are found.
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to prepend bin to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo
	}

	// Record our PID so orphan-detection watchdogs know which process to monitor.
	os.Setenv("MULTIGRES_TEST_PARENT_PID", strconv.Itoa(os.Getpid()))

	code := m.Run()

	if pgTestFixture != nil {
		stopSharedPostgres(pgTestFixture)
		os.RemoveAll(pgTestFixture.dataDir)
	}

	os.Exit(code) //nolint:forbidigo
}

// startSharedPostgres initialises and starts a dedicated postgres instance.
// It sets up the multigres schema via CreateRuleTables so tests do not need to.
func startSharedPostgres(t *testing.T) (*pgPostgresFixture, error) {
	t.Helper()
	// Use /tmp explicitly so the unix socket path stays under the 104-byte macOS limit.
	dataDir, err := os.MkdirTemp("/tmp", "rule_store_pg_test-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	pgDataDir := filepath.Join(dataDir, "pg_data")
	socketDir := filepath.Join(dataDir, "pg_sockets")

	if err := os.MkdirAll(socketDir, 0o700); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}

	// LC_ALL=C avoids locale-library issues on macOS with Homebrew postgres.
	pgEnv := append(os.Environ(), "LC_ALL=C")

	// Initialise the data directory.
	initdb := exec.Command(
		"initdb",
		"-D", pgDataDir,
		"-U", "postgres",
		"--no-instructions",
		"-A", "trust",
	)
	initdb.Env = pgEnv
	if out, err := initdb.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("initdb failed: %w\nOutput: %s", err, out)
	}

	f := &pgPostgresFixture{
		dataDir:   dataDir,
		pgDataDir: pgDataDir,
		socketDir: socketDir,
	}

	logFile := filepath.Join(dataDir, "postgres.log")
	// listen_addresses= disables TCP entirely; postgres only creates a unix socket.
	// The socket filename uses the default port (5432) by convention.
	opts := fmt.Sprintf(
		"-c listen_addresses= -c unix_socket_directories=%s -c logging_collector=off",
		socketDir,
	)

	// Start postgres and wait up to 30 seconds for it to be ready.
	pgStart := exec.Command(
		"pg_ctl", "start",
		"-D", pgDataDir,
		"-o", opts,
		"-l", logFile,
		"-w", "-t", "30",
	)
	pgStart.Env = pgEnv
	if out, err := pgStart.CombinedOutput(); err != nil {
		pgLog, _ := os.ReadFile(logFile)
		return nil, fmt.Errorf("pg_ctl start failed: %w\nOutput: %s\nPostgres log:\n%s", err, out, pgLog)
	}

	// Spawn an orphan-detection watchdog so postgres is stopped even if the test
	// process is killed unexpectedly.  The watchdog runs in its own process group
	// so signals delivered to the test process group do not terminate it early.
	watchdog := exec.Command(
		"run_command_if_parent_dies.sh",
		"pg_ctl", "stop", "-D", pgDataDir, "-m", "fast",
	)
	watchdog.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	watchdog.Env = append(os.Environ(), "MULTIGRES_TESTDATA_DIR="+dataDir)
	if err := watchdog.Start(); err != nil {
		// Non-fatal: the deferred stopSharedPostgres call handles normal teardown.
		fmt.Fprintf(os.Stderr, "Warning: failed to start watchdog: %v\n", err)
	}

	// Create the multigres schema and rule tables so all tests start with a
	// clean, initialised state.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := f.newClientConn(ctx)
	if err != nil {
		_ = exec.Command("pg_ctl", "stop", "-D", pgDataDir, "-m", "fast").Run()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close()

	qs := &connQueryService{conn: conn}
	if _, err := qs.conn.Query(ctx, "CREATE SCHEMA multigres"); err != nil {
		_ = exec.Command("pg_ctl", "stop", "-D", pgDataDir, "-m", "fast").Run()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	logger := slog.New(slog.DiscardHandler)
	rs := NewRuleStore(logger, qs, noopSyncStandbyManager{})
	if err := rs.CreateRuleTables(ctx, testBootstrapPolicy(), testBootstrapID()); err != nil {
		_ = exec.Command("pg_ctl", "stop", "-D", pgDataDir, "-m", "fast").Run()
		return nil, fmt.Errorf("create rule tables: %w", err)
	}

	return f, nil
}

// stopSharedPostgres stops the shared postgres instance.
func stopSharedPostgres(f *pgPostgresFixture) {
	exec.Command("pg_ctl", "stop", "-D", f.pgDataDir, "-m", "fast").Run() //nolint:errcheck
}

// newTestRuleStore creates a ruleStore connected to the shared test postgres.
// The returned conn must be closed when done; close it via t.Cleanup.
func newTestRuleStore(ctx context.Context, t *testing.T) (*ruleStore, *client.Conn) {
	t.Helper()
	conn, err := pgTestFixture.newClientConn(ctx)
	require.NoError(t, err)
	logger := slog.New(slog.DiscardHandler)
	rs := NewRuleStore(logger, &connQueryService{conn: conn}, noopSyncStandbyManager{})
	return rs, conn
}

// resetRuleStoreTables truncates and re-initialises the rule tables so each test
// starts from the initial row.
func resetRuleStoreTables(ctx context.Context, t *testing.T) {
	t.Helper()
	conn, err := pgTestFixture.newClientConn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	qs := &connQueryService{conn: conn}
	_, err = qs.conn.Query(ctx, "DROP TABLE multigres.current_rule, multigres.rule_history")
	require.NoError(t, err)

	logger := slog.New(slog.DiscardHandler)
	rs := NewRuleStore(logger, qs, noopSyncStandbyManager{})
	require.NoError(t, rs.CreateRuleTables(ctx, testBootstrapPolicy(), testBootstrapID()))
}

// skipIfNoPG lazily starts the shared postgres fixture on first call and skips
// the test if PostgreSQL binaries are not available.
func skipIfNoPG(t *testing.T) {
	t.Helper()
	pgTestOnce.Do(func() {
		if !testutils.HasPostgreSQLBinaries() {
			return // pgTestFixture stays nil; tests skip below
		}
		pgTestFixture, pgTestErr = startSharedPostgres(t)
	})
	if pgTestErr != nil {
		t.Fatalf("postgres setup failed: %v", pgTestErr)
	}
	if pgTestFixture == nil {
		t.Skip("PostgreSQL binaries not available")
	}
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

func TestRuleStorePG_ObservePosition_FreshState(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos, "fresh state should still return a position with the current LSN")
	assert.Equal(t, int64(0), pos.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm(), "fresh initial row has coordinator_term=0")
	assert.NotEmpty(t, pos.GetLsn(), "fresh state should include the current WAL LSN")

	// The initial row written by CreateRuleTables must carry the bootstrap durability policy.
	dp := pos.GetPosition().GetDecision().GetDurabilityPolicy()
	require.NotNil(t, dp, "initial row must carry the bootstrap durability policy")
	assert.Equal(t, testBootstrapPolicy().PolicyName, dp.PolicyName)
	assert.Equal(t, testBootstrapPolicy().QuorumType, dp.QuorumType)
	assert.Equal(t, testBootstrapPolicy().RequiredCount, dp.RequiredCount)
}

func TestRuleStorePG_ObservePosition_InitialRowMissing(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	// Wipe the initial row that resetRuleStoreTables just inserted. ObservePosition
	// must surface a specific error rather than returning a nil position.
	_, err := conn.Query(ctx, "DELETE FROM multigres.current_rule")
	require.NoError(t, err)

	_, err = rs.ObservePosition(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "current_rule initial row missing for shard 0")
}

func TestRuleStorePG_ObservePosition_InitialPolicyCarriedThroughUpdate(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")

	// Write a rule without specifying a durability policy — the initial row's policy must carry forward.
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "promotion", "initial", time.Now()))
	require.NoError(t, err)

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)

	dp := pos.GetPosition().GetDecision().GetDurabilityPolicy()
	require.NotNil(t, dp, "initial policy must carry forward through UpdateRule without WithDurabilityPolicy")
	assert.Equal(t, testBootstrapPolicy().PolicyName, dp.PolicyName)
	assert.Equal(t, testBootstrapPolicy().QuorumType, dp.QuorumType)
	assert.Equal(t, testBootstrapPolicy().RequiredCount, dp.RequiredCount)
}

// TestRuleStorePG_ObservePosition_SurfacesStuckProposal seeds a proposal
// directly via raw SQL against current_rule, bypassing UpdateRule entirely —
// there is no write path that produces one yet (see the two-phase-write gap
// noted in UpdateRule). This is the concrete verification that the schema
// migration actually closes the observability gap: once a proposal exists in
// the DB, however it got there, ObservePosition must surface it.
func TestRuleStorePG_ObservePosition_SurfacesStuckProposal(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	now := time.Now()

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", now).
			WithLeader(leaderID).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	// Seed a pending proposal directly, simulating a crash between a WAL
	// write reaching quorum and the coordinator marking it decided.
	proposalCreatedAt := now.Add(time.Minute)
	_, err = conn.Query(ctx, fmt.Sprintf(`
		UPDATE multigres.current_rule
		SET proposal_coordinator_term = 4,
		    proposal_leader_subterm = 0,
		    proposal_leader_id = '%s',
		    proposal_cohort_members = '{%s}',
		    proposal_durability_policy_name = '%s',
		    proposal_durability_quorum_type = '%s',
		    proposal_durability_required_count = %d,
		    proposal_created_at = '%s'
		WHERE shard_id = '0'::bytea`,
		"zone1_leader-2",
		"zone1_member-1,zone1_member-2",
		testBootstrapPolicy().PolicyName,
		testBootstrapPolicy().QuorumType.String(),
		testBootstrapPolicy().RequiredCount,
		proposalCreatedAt.UTC().Format("2006-01-02 15:04:05.999999-07")))
	require.NoError(t, err)

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)

	// The decision is unchanged.
	assert.Equal(t, int64(3), pos.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, "leader-1", pos.GetPosition().GetDecision().GetLeaderId().GetName())

	// The stuck proposal is now observable.
	proposal := pos.GetPosition().GetProposal()
	require.NotNil(t, proposal, "a seeded proposal must be surfaced")
	assert.Equal(t, int64(4), proposal.GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, int64(0), proposal.GetRuleNumber().GetLeaderSubterm())
	require.NotNil(t, proposal.GetLeaderId())
	assert.Equal(t, "leader-2", proposal.GetLeaderId().GetName(), "proposal names its own candidate leader")
	require.Len(t, proposal.GetCohortMembers(), 2)
	assert.Equal(t, "member-1", proposal.GetCohortMembers()[0].GetName())
	assert.Equal(t, "member-2", proposal.GetCohortMembers()[1].GetName())
	assert.Equal(t, testBootstrapPolicy().PolicyName, proposal.GetDurabilityPolicy().GetPolicyName())
	assert.Equal(t, testBootstrapPolicy().QuorumType, proposal.GetDurabilityPolicy().GetQuorumType())
	assert.Equal(t, testBootstrapPolicy().RequiredCount, proposal.GetDurabilityPolicy().GetRequiredCount())
	require.NotNil(t, proposal.GetCreationTime())
	assert.WithinDuration(t, proposalCreatedAt, proposal.GetCreationTime().AsTime(), time.Second,
		"a proposal's own creation_time is observable independent of the decision it's transitioning from")
}

// seedStuckProposal writes a proposal directly via raw SQL, bypassing
// UpdateRule entirely — there is no write path that leaves one behind on
// demand, so this simulates a coordinator that crashed between UpdateRule's
// two write phases (see rule_store.go). Returns the seeded rule number.
func seedStuckProposal(ctx context.Context, t *testing.T, conn *client.Conn, term int64, leaderID *clustermetadatapb.ID, cohort []*clustermetadatapb.ID, policy *clustermetadatapb.DurabilityPolicy) *clustermetadatapb.RuleNumber {
	t.Helper()
	cohortNames := make([]string, len(cohort))
	for i, id := range cohort {
		cohortNames[i] = id.GetCell() + "_" + id.GetName()
	}
	_, err := conn.Query(ctx, fmt.Sprintf(`
		UPDATE multigres.current_rule
		SET proposal_coordinator_term = %d,
		    proposal_leader_subterm = 0,
		    proposal_leader_id = '%s_%s',
		    proposal_cohort_members = '{%s}',
		    proposal_durability_policy_name = '%s',
		    proposal_durability_quorum_type = '%s',
		    proposal_durability_required_count = %d,
		    proposal_created_at = now()
		WHERE shard_id = '0'::bytea`,
		term,
		leaderID.GetCell(), leaderID.GetName(),
		strings.Join(cohortNames, ","),
		policy.GetPolicyName(),
		policy.GetQuorumType().String(),
		policy.GetRequiredCount()))
	require.NoError(t, err)
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: 0}
}

// TestRuleStorePG_UpdateRule_RejectsWhenProposalStuck verifies that an
// ordinary (non-skipOutgoingQuorum) write refuses to proceed while a stuck
// proposal is present: it has no independent safety backing (no cert, no
// verified quorum) to know whether overwriting it would discard durable work.
func TestRuleStorePG_UpdateRule_RejectsWhenProposalStuck(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", time.Now()).
			WithLeader(leaderID).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	seedStuckProposal(ctx, t, conn, 4, leaderID, cohort, testBootstrapPolicy())

	_, err = rs.UpdateRule(ctx, NewRuleUpdate(3, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "undecided proposal")
	assert.Contains(t, err.Error(), "propagation is not yet supported")

	// The rejected write must not have partially applied — decision and
	// proposal are exactly as seeded.
	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), pos.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	require.NotNil(t, pos.GetPosition().GetProposal())
	assert.Equal(t, int64(4), pos.GetPosition().GetProposal().GetRuleNumber().GetCoordinatorTerm())
}

// TestRuleStorePG_UpdateRule_SkipOutgoingQuorumSupersedesStuckProposal
// verifies that skipOutgoingQuorum — the caller having already established
// safety via an externally-certified cert — lets a write proceed even though
// the current row carries a stuck proposal, CASing against that proposal
// (not the stale decision) since that's the position actually being
// superseded.
func TestRuleStorePG_UpdateRule_SkipOutgoingQuorumSupersedesStuckProposal(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", time.Now()).
			WithLeader(leaderID).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	stuckRuleNumber := seedStuckProposal(ctx, t, conn, 4, leaderID, cohort, testBootstrapPolicy())

	newLeaderID := testPoolerID(t, "zone1", "leader-2")
	pos, err := rs.UpdateRule(ctx,
		NewRuleUpdate(5, coordinatorID, "promotion", "recover stuck proposal", time.Now()).
			WithLeader(newLeaderID).
			WithCohort(cohort).
			WithPreviousRule(stuckRuleNumber.CoordinatorTerm, stuckRuleNumber.LeaderSubterm).
			WithSkipOutgoingQuorum().
			WithPromotionHook(func(context.Context) error { return nil }),
	)
	require.NoError(t, err, "skipOutgoingQuorum should supersede the stuck proposal")
	assert.Equal(t, int64(5), pos.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, "leader-2", pos.GetPosition().GetDecision().GetLeaderId().GetName())
	assert.Nil(t, pos.GetPosition().GetProposal(), "the fresh write is fully decided, not stuck")
}

func TestRuleStorePG_UpdateRule_FirstWrite(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	update := NewRuleUpdate(1, coordinatorID, "promotion", "initial primary", time.Now())

	pos, err := rs.UpdateRule(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(1), pos.Position.Decision.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Position.Decision.RuleNumber.LeaderSubterm, "first write in a new term starts at subterm 0")
}

func TestRuleStorePG_UpdateRule_SameTermIncrementsSubterm(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	pos1, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos1.Position.Decision.RuleNumber.LeaderSubterm)

	pos2, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", "second", now))
	require.NoError(t, err)
	assert.Equal(t, int64(1), pos2.Position.Decision.RuleNumber.LeaderSubterm, "second write in same term increments subterm")
}

func TestRuleStorePG_UpdateRule_NewTermResetsSubterm(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Establish term 1 with a few subterms.
	for range 3 {
		_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", "setup", now))
		require.NoError(t, err)
	}

	// Advance to term 2: subterm must reset to 0.
	pos, err := rs.UpdateRule(ctx, NewRuleUpdate(2, coordinatorID, "promotion", "new coordinator", now))
	require.NoError(t, err)
	assert.Equal(t, int64(2), pos.Position.Decision.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Position.Decision.RuleNumber.LeaderSubterm, "new term resets subterm to 0")
}

func TestRuleStorePG_UpdateRule_StaleTermRejected(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Advance to term 2.
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(2, coordinatorID, "promotion", "initial", now))
	require.NoError(t, err)

	// Attempt to write with stale term 1.
	_, err = rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", "stale", now))
	require.Error(t, err, "writing with a stale term must fail")
	assert.EqualError(t, err, "rule update rejected for term 1: current rule is at term 2")
}

func TestRuleStorePG_UpdateRule_ObserveAfterWrite(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	now := time.Now()

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(3, coordinatorID, "promotion", "bootstrap", now).
			WithLeader(leaderID).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(3), pos.Position.Decision.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Position.Decision.RuleNumber.LeaderSubterm)

	require.NotNil(t, pos.Position.Decision.LeaderId)
	assert.Equal(t, "zone1", pos.Position.Decision.LeaderId.Cell)
	assert.Equal(t, "leader-1", pos.Position.Decision.LeaderId.Name)

	require.NotNil(t, pos.Position.Decision.CoordinatorId)
	assert.Equal(t, "zone1", pos.Position.Decision.CoordinatorId.Cell)
	assert.Equal(t, "coordinator-1", pos.Position.Decision.CoordinatorId.Name)

	require.Len(t, pos.Position.Decision.CohortMembers, 2)
	assert.Equal(t, "member-1", pos.Position.Decision.CohortMembers[0].Name)
	assert.Equal(t, "member-2", pos.Position.Decision.CohortMembers[1].Name)

	assert.NotEmpty(t, pos.Lsn, "LSN should be populated after a write")
}

func TestRuleStorePG_UpdateRule_HistoryFields(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	accepted := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
	}
	walPos := "0/1234ABCD"
	op := "replication_config"
	now := time.Now().UTC().Truncate(time.Millisecond)

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(5, coordinatorID, "promotion", "bootstrap failover", now).
			WithLeader(leaderID).
			WithCohort(cohort).
			WithWALPosition(walPos).
			WithOperation(op).
			WithAcceptedMembers(accepted),
	)
	require.NoError(t, err)

	records, err := rs.queryRuleHistory(ctx, 1)
	require.NoError(t, err)
	require.Len(t, records, 1)

	rec := records[0]
	assert.Equal(t, int64(5), rec.CoordinatorTerm)
	assert.Equal(t, int64(0), rec.LeaderSubterm)
	assert.Equal(t, "promotion", rec.EventType)
	assert.Equal(t, "bootstrap failover", rec.Reason)

	require.NotNil(t, rec.LeaderID)
	assert.Equal(t, "zone1", rec.LeaderID.id.Cell)
	assert.Equal(t, "leader-1", rec.LeaderID.id.Name)

	// coordinator_id is stored and returned as the "cell_name" app-name string.
	require.NotNil(t, rec.CoordinatorID)
	assert.Equal(t, "zone1_coordinator-1", *rec.CoordinatorID)

	require.NotNil(t, rec.WALPosition)
	assert.Equal(t, walPos, *rec.WALPosition)

	require.NotNil(t, rec.Operation)
	assert.Equal(t, op, *rec.Operation)

	require.Len(t, rec.CohortMembers, 2)
	assert.Equal(t, "zone1", rec.CohortMembers[0].id.Cell)
	assert.Equal(t, "member-1", rec.CohortMembers[0].id.Name)
	assert.Equal(t, "member-2", rec.CohortMembers[1].id.Name)

	require.Len(t, rec.AcceptedMembers, 1)
	assert.Equal(t, "member-1", rec.AcceptedMembers[0].id.Name)

	assert.WithinDuration(t, now, rec.CreatedAt, time.Second)
}

func TestRuleStorePG_UpdateRule_CASSuccess(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Write the first rule so we know the exact term/subterm.
	pos1, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)
	term := pos1.Position.Decision.RuleNumber.CoordinatorTerm  // 1
	subterm := pos1.Position.Decision.RuleNumber.LeaderSubterm // 0

	// CAS: only proceed if current rule is still (term=1, subterm=0).
	pos2, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "config_change", "cas write", now).
			WithPreviousRule(term, subterm),
	)
	require.NoError(t, err, "CAS should succeed when term/subterm match")
	assert.Equal(t, int64(1), pos2.Position.Decision.RuleNumber.LeaderSubterm, "subterm should advance to 1")
}

func TestRuleStorePG_UpdateRule_CASConflict(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Write once to advance past the initial row's coordinates.
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)

	// CAS with stale coordinates should fail with errRuleConflict.
	_, err = rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "config_change", "stale cas", now).
			WithPreviousRule(0, 0), // initial-row coordinates no longer current
	)
	require.ErrorIs(t, err, errRuleConflict, "CAS with wrong term/subterm must return errRuleConflict")
}

func TestRuleStorePG_UpdateRule_HistoryRecorded(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	for i := range 5 {
		_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", fmt.Sprintf("write %d", i), now))
		require.NoError(t, err)
	}

	records, err := rs.queryRuleHistory(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, records, 5, "every UpdateRule call must append a rule_history row")
}

func TestRuleStorePG_UpdateRule_Concurrent(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	const goroutines = 100
	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	lock := actionlock.NewActionLock()
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			gCtx, lockErr := lock.Acquire(t.Context(), "goroutine-test")
			if lockErr != nil {
				errs[idx] = fmt.Errorf("acquire lock: %w", lockErr)
				return
			}
			defer lock.Release(gCtx)

			conn, err := pgTestFixture.newClientConn(gCtx)
			if err != nil {
				errs[idx] = fmt.Errorf("connect: %w", err)
				return
			}
			defer conn.Close()

			logger := slog.New(slog.DiscardHandler)
			rs := NewRuleStore(logger, &connQueryService{conn: conn}, noopSyncStandbyManager{})
			_, errs[idx] = rs.UpdateRule(gCtx,
				NewRuleUpdate(1, coordinatorID, "config_change",
					fmt.Sprintf("concurrent write %d", idx), now),
			)
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d failed", i)
	}

	// All goroutines serialized via SELECT FOR UPDATE; verify the full audit log.
	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	records, err := rs.queryRuleHistory(ctx, goroutines+1)
	require.NoError(t, err)
	assert.Len(t, records, goroutines, "every concurrent write must produce a history row")

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(1), pos.Position.Decision.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(goroutines-1), pos.Position.Decision.RuleNumber.LeaderSubterm,
		"final subterm should equal goroutines-1 after %d serialized writes", goroutines)
}

func TestRuleStorePG_UpdateRule_DurabilityPolicyRoundTrip(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	// Use a non-default policy so this test cannot accidentally pass against a default value.
	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "MULTI_CELL_AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: 2,
	}
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone2", "member-2"),
	}
	now := time.Now()
	bootstrapRuleState(ctx, t, rs, policy, cohort)

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(7, coordinatorID, "promotion", "initial", now).
			WithDurabilityPolicy(policy).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)

	dp := pos.Position.Decision.GetDurabilityPolicy()
	require.NotNil(t, dp, "durability policy must be persisted and returned by ObservePosition")
	assert.Equal(t, "MULTI_CELL_AT_LEAST_2", dp.PolicyName)
	assert.Equal(t, clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N, dp.QuorumType)
	assert.Equal(t, int32(2), dp.RequiredCount)
}

func TestRuleStorePG_UpdateRule_DurabilityPolicyPreservedOnUpdate(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "MULTI_CELL_AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: 2,
	}
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone2", "member-2"),
	}
	now := time.Now()
	bootstrapRuleState(ctx, t, rs, policy, cohort)

	// Write rule with durability policy.
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(7, coordinatorID, "promotion", "initial", now).
			WithDurabilityPolicy(policy).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	// Write another rule without specifying durability policy; it must be preserved via COALESCE.
	_, err = rs.UpdateRule(ctx,
		NewRuleUpdate(7, coordinatorID, "config_change", "update", now),
	)
	require.NoError(t, err)

	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)

	dp := pos.Position.Decision.GetDurabilityPolicy()
	require.NotNil(t, dp, "durability policy must be preserved across updates that omit it")
	assert.Equal(t, "MULTI_CELL_AT_LEAST_2", dp.PolicyName)
}

func TestRuleStorePG_UpdateRule_DurabilityPolicyInHistory(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "MULTI_CELL_AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: 2,
	}
	cohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone2", "member-2"),
	}
	now := time.Now()
	bootstrapRuleState(ctx, t, rs, policy, cohort)

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(7, coordinatorID, "promotion", "initial", now).
			WithDurabilityPolicy(policy).
			WithCohort(cohort),
	)
	require.NoError(t, err)

	records, err := rs.queryRuleHistory(ctx, 1)
	require.NoError(t, err)
	require.Len(t, records, 1)

	rec := records[0]
	assert.Equal(t, "MULTI_CELL_AT_LEAST_2", rec.DurabilityPolicyName, "rule_history must store durability_policy_name")
}

func TestRuleStorePG_UpdateRule_DurabilityPolicyAchievabilityRejected(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	// Policy requires 2 distinct cells, but cohort is all in zone1.
	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "MULTI_CELL_AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
		RequiredCount: 2,
	}
	singleCellCohort := []*clustermetadatapb.ID{
		testPoolerID(t, "zone1", "member-1"),
		testPoolerID(t, "zone1", "member-2"),
	}
	now := time.Now()

	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(7, coordinatorID, "promotion", "initial", now).
			WithDurabilityPolicy(policy).
			WithCohort(singleCellCohort),
	)
	require.Error(t, err, "UpdateRule must reject a cohort that cannot satisfy the durability policy")
	assert.EqualError(t, err, "cohort cannot achieve durability policy: durability not achievable: proposed cohort spans 1 cells, required 2")
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// testPoolerID builds a *clustermetadatapb.ID for use in test updates.
func testPoolerID(t *testing.T, cell, name string) *clustermetadatapb.ID {
	t.Helper()
	return &clustermetadatapb.ID{Cell: cell, Name: name}
}

// bootstrapRuleState performs an externally-certified write to establish a
// post-bootstrap state. Use this in tests that focus on rule transitions starting
// from a non-sentinel state; the sentinel→first-rule bootstrap transition is handled
// by the Promote RPC with external certification and is not what those tests cover.
func bootstrapRuleState(ctx context.Context, t *testing.T, rs *ruleStore, policy *clustermetadatapb.DurabilityPolicy, cohort []*clustermetadatapb.ID) {
	t.Helper()
	coordinatorID := testPoolerID(t, "zone1", "coordinator-bootstrap")
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "bootstrap", time.Now()).
			WithDurabilityPolicy(policy).
			WithCohort(cohort).
			WithSkipOutgoingQuorum(),
	)
	require.NoError(t, err)
}

// newTestRuleStoreWithRealSSM creates a ruleStore and a real postgresqlSyncStandbyManager,
// each backed by their own postgres connection. localID is used by BuildSyncReplicationConfig
// to identify the primary (for policies that filter same-cell standbys). Connections are
// closed via t.Cleanup.
func newTestRuleStoreWithRealSSM(t *testing.T, localID *clustermetadatapb.ID) (*ruleStore, *postgresqlSyncStandbyManager) {
	t.Helper()
	rsConn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { rsConn.Close() })

	ssmConn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { ssmConn.Close() })

	logger := slog.New(slog.DiscardHandler)
	ssm := NewSyncStandbyManager(logger, &connQueryService{conn: ssmConn}, localID)
	rs := NewRuleStore(logger, &connQueryService{conn: rsConn}, ssm)
	return rs, ssm
}

// resetGUCSettings resets synchronous replication GUCs written by postgresqlSyncStandbyManager
// so tests that call ALTER SYSTEM don't pollute subsequent tests.
func resetGUCSettings(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgTestFixture.newClientConn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	qs := &connQueryService{conn: conn}
	qs.Query(ctx, "ALTER SYSTEM RESET synchronous_commit")        //nolint:errcheck
	qs.Query(ctx, "ALTER SYSTEM RESET synchronous_standby_names") //nolint:errcheck
	qs.Query(ctx, "SELECT pg_reload_conf()")                      //nolint:errcheck
}

// TestRuleStorePG_HasInconsistentGUC_FalseBeforeAnyObservation verifies that
// HasInconsistentGUC returns false when no position has been observed yet.
// A nil cache means we cannot compute the desired GUC, so we should not
// report inconsistency — the monitor will call ObservePosition first.
func TestRuleStorePG_HasInconsistentGUC_FalseBeforeAnyObservation(t *testing.T) {
	skipIfNoPG(t)
	resetRuleStoreTables(t.Context(), t)

	localID := testPoolerID(t, "zone1", "primary-1")
	rs, _ := newTestRuleStoreWithRealSSM(t, localID)

	assert.False(t, rs.HasInconsistentGUC(t.Context()), "nil cache should not report inconsistency before any ObservePosition call")
}

// TestRuleStorePG_GUCReconciliation verifies the full detect-and-fix cycle:
//
//  1. Bootstrap a rule with a real durability policy using noopSyncStandbyManager
//     (simulating a prior process that committed a rule without updating GUC cache).
//  2. Create a fresh ruleStore with a real postgresqlSyncStandbyManager (cold cache).
//  3. After ObservePosition() the cache has the rule but SSM has no cached GUC,
//     so HasInconsistentGUC() should return true.
//  4. ReconcileGUC() should succeed and apply the GUC.
//  5. HasInconsistentGUC() should return false afterwards.
func TestRuleStorePG_GUCReconciliation(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)
	t.Cleanup(func() { resetGUCSettings(t) })

	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
	localID := testPoolerID(t, "zone1", "primary-1")
	cohort := []*clustermetadatapb.ID{
		localID,
		testPoolerID(t, "zone1", "standby-1"),
	}

	// Bootstrap the rule using a noop SSM (simulates a prior primary that wrote
	// the rule but we're starting fresh without a cached GUC).
	bootstrapRS, bootstrapConn := newTestRuleStore(ctx, t)
	defer bootstrapConn.Close()
	bootstrapRuleState(ctx, t, bootstrapRS, policy, cohort)

	// New ruleStore with a real SSM — cold cache.
	rs, ssm := newTestRuleStoreWithRealSSM(t, localID)

	// Warm the rule cache via ObservePosition without touching GUC.
	_, err := rs.ObservePosition(ctx)
	require.NoError(t, err)

	// Cold SSM cache: postgres has the correct values but cache is empty, so
	// NeedsApply queries postgres and returns true (cache != desired).
	assert.True(t, rs.HasInconsistentGUC(ctx), "cold SSM cache with a committed rule should be inconsistent")

	// ReconcileGUC should apply the GUC and populate the SSM cache.
	require.NoError(t, rs.ReconcileGUC(ctx, false /* inRecovery */))

	// After ReconcileGUC, NeedsApply queries postgres and finds it already matches.
	needs, err := ssm.NeedsApply(ctx, commonconsensus.PolicyWithCohort{
		Policy: mustNewPolicyFromProto(t, policy),
		Cohort: cohort,
	})
	require.NoError(t, err)
	assert.False(t, needs, "postgres should already have desired GUC after ReconcileGUC")

	// HasInconsistentGUC should now return false — no spurious re-applies.
	assert.False(t, rs.HasInconsistentGUC(ctx), "HasInconsistentGUC should be false after ReconcileGUC")
}

// TestRuleStorePG_GUCDriftDetectedAndHealed verifies the full detect-and-fix cycle
// when the GUC is changed outside the manager (e.g. manual ALTER SYSTEM + reload):
//
//  1. Establish a known-good GUC state via ReconcileGUC.
//  2. Corrupt the live GUC via a separate connection.
//  3. HasInconsistentGUC detects the drift by querying postgres.
//  4. ReconcileGUC restores the correct values.
//  5. HasInconsistentGUC returns false.
func TestRuleStorePG_GUCDriftDetectedAndHealed(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)
	t.Cleanup(func() { resetGUCSettings(t) })

	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
	localID := testPoolerID(t, "zone1", "primary-1")
	cohort := []*clustermetadatapb.ID{
		localID,
		testPoolerID(t, "zone1", "standby-1"),
	}

	bootstrapRS, bootstrapConn := newTestRuleStore(ctx, t)
	defer bootstrapConn.Close()
	bootstrapRuleState(ctx, t, bootstrapRS, policy, cohort)

	rs, _ := newTestRuleStoreWithRealSSM(t, localID)
	_, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	require.NoError(t, rs.ReconcileGUC(ctx, false))
	require.False(t, rs.HasInconsistentGUC(ctx), "precondition: GUC should be consistent after initial reconcile")

	// Corrupt the live GUC behind the manager's back.
	// Also reset synchronous_commit to 'local' so the ReconcileGUC healing step can
	// commit without blocking — in production actual standbys are present, but in
	// tests we have none.
	corruptConn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	defer corruptConn.Close()
	corruptQS := &connQueryService{conn: corruptConn}
	_, err = corruptQS.Query(t.Context(), "ALTER SYSTEM SET synchronous_standby_names = 'WRONG'")
	require.NoError(t, err)
	_, err = corruptQS.Query(t.Context(), "ALTER SYSTEM SET synchronous_commit = 'local'")
	require.NoError(t, err)
	logger := slog.New(slog.DiscardHandler)
	require.NoError(t, ReloadPostgresConfig(t.Context(), logger, corruptQS))

	// HasInconsistentGUC queries postgres and detects the drift.
	assert.True(t, rs.HasInconsistentGUC(ctx), "drift should be detected after external GUC change")

	// ReconcileGUC restores the correct value.
	require.NoError(t, rs.ReconcileGUC(ctx, false))

	assert.False(t, rs.HasInconsistentGUC(ctx), "GUC should be consistent after ReconcileGUC heals the drift")
}

// TestRuleStorePG_ReconcileGUC_FailsWhenRuleIsLocked verifies that ReconcileGUC
// fails immediately (FOR UPDATE NOWAIT) when another transaction holds the
// current_rule row lock, leaving the SSM cache unchanged.
func TestRuleStorePG_ReconcileGUC_FailsWhenRuleIsLocked(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)
	t.Cleanup(func() { resetGUCSettings(t) })

	policy := &clustermetadatapb.DurabilityPolicy{
		PolicyName:    "AT_LEAST_2",
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
		RequiredCount: 2,
	}
	localID := testPoolerID(t, "zone1", "primary-1")
	cohort := []*clustermetadatapb.ID{
		localID,
		testPoolerID(t, "zone1", "standby-1"),
	}

	bootstrapRS, bootstrapConn := newTestRuleStore(ctx, t)
	defer bootstrapConn.Close()
	bootstrapRuleState(ctx, t, bootstrapRS, policy, cohort)

	rs, ssm := newTestRuleStoreWithRealSSM(t, localID)

	// Hold SELECT FOR UPDATE on current_rule for the duration of the test.
	blockConn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	defer blockConn.Close()

	_, err = blockConn.Query(t.Context(), "BEGIN")
	require.NoError(t, err)
	_, err = blockConn.Query(t.Context(), "SELECT * FROM multigres.current_rule FOR UPDATE")
	require.NoError(t, err)
	defer blockConn.Query(context.Background(), "ROLLBACK") //nolint:errcheck

	// FOR UPDATE NOWAIT should fail immediately rather than blocking.
	start := time.Now()
	err = rs.ReconcileGUC(ctx, false /* inRecovery */)
	elapsed := time.Since(start)

	require.Error(t, err, "ReconcileGUC should fail when current_rule is locked")
	assert.Less(t, elapsed, 500*time.Millisecond, "NOWAIT should fail fast, not block (got %v)", elapsed)

	// SSM cache must be unchanged: no GUC should have been applied.
	needs, err2 := ssm.NeedsApply(ctx, commonconsensus.PolicyWithCohort{
		Policy: mustNewPolicyFromProto(t, policy),
		Cohort: cohort,
	})
	require.NoError(t, err2)
	assert.True(t, needs, "SSM cache must remain cold after a failed ReconcileGUC")
}

// TestRuleStorePG_UpdateRule_FailsWhenRuleIsLocked verifies that UpdateRule
// fails immediately (FOR UPDATE NOWAIT) when another transaction holds the
// current_rule row lock. The error must mention the lock contention.
func TestRuleStorePG_UpdateRule_FailsWhenRuleIsLocked(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	// Hold SELECT FOR UPDATE on current_rule for the duration of the test.
	blockConn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	defer blockConn.Close()

	_, err = blockConn.Query(t.Context(), "BEGIN")
	require.NoError(t, err)
	_, err = blockConn.Query(t.Context(), "SELECT * FROM multigres.current_rule FOR UPDATE")
	require.NoError(t, err)

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	_, err = rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "promotion", "locked", time.Now()))

	require.Error(t, err)
	assert.EqualError(t, err, "failed to read current_rule: ERROR: could not obtain lock on row in relation \"current_rule\"")
}

// TestRuleStorePG_ReconcileGUC_RequiresActionLock verifies that ReconcileGUC
// returns an error when called without the action lock.
func TestRuleStorePG_ReconcileGUC_RequiresActionLock(t *testing.T) {
	skipIfNoPG(t)
	resetRuleStoreTables(t.Context(), t)

	localID := testPoolerID(t, "zone1", "primary-1")
	rs, _ := newTestRuleStoreWithRealSSM(t, localID)

	err := rs.ReconcileGUC(t.Context(), false)
	require.Error(t, err)
	assert.EqualError(t, err, "ReconcileGUC: context does not hold an action lock")
}

// TestRuleStorePG_UpdateRule_RequiresActionLock verifies that UpdateRule returns
// an error when called without the action lock.
func TestRuleStorePG_UpdateRule_RequiresActionLock(t *testing.T) {
	skipIfNoPG(t)
	resetRuleStoreTables(t.Context(), t)

	rs, conn := newTestRuleStore(t.Context(), t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	_, err := rs.UpdateRule(t.Context(), NewRuleUpdate(1, coordinatorID, "promotion", "no lock", time.Now()))
	require.Error(t, err)
	assert.EqualError(t, err, "UpdateRule: context does not hold an action lock")
}

// newTestRuleStoreWithFailingSSM creates a ruleStore backed by the shared
// postgres fixture and a SyncStandbyManager that returns the given errors from
// SetPolicy in order. Used by GUC-failure tests to exercise UpdateRule's
// pre-promote / pre-write / post-write error paths.
func newTestRuleStoreWithFailingSSM(t *testing.T, setPolicyErrs []error) (*ruleStore, *failingSyncStandbyManager) {
	t.Helper()
	conn, err := pgTestFixture.newClientConn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	logger := slog.New(slog.DiscardHandler)
	ssm := &failingSyncStandbyManager{setPolicyErrs: setPolicyErrs}
	rs := NewRuleStore(logger, &connQueryService{conn: conn}, ssm)
	return rs, ssm
}

func TestRuleStorePG_UpdateRule_PreWriteGUCFails(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, _ := newTestRuleStoreWithFailingSSM(t, []error{errors.New("alter system failed")})
	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-write GUC")
	assert.Contains(t, err.Error(), "alter system failed")
}

func TestRuleStorePG_UpdateRule_PrePromoteGUCFails(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, _ := newTestRuleStoreWithFailingSSM(t, []error{errors.New("alter system failed")})
	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	hookCalled := false
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "test", time.Now()).
			WithPromotionHook(func(context.Context) error {
				hookCalled = true
				return nil
			}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre-promote GUC")
	assert.Contains(t, err.Error(), "alter system failed")
	assert.False(t, hookCalled, "promotion hook must not run after pre-promote GUC fails")
}

func TestRuleStorePG_UpdateRule_PromotionHookFails(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, _ := newTestRuleStoreWithFailingSSM(t, nil) // SetPolicy always succeeds
	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "test", time.Now()).
			WithPromotionHook(func(context.Context) error {
				return errors.New("pg_promote refused")
			}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "promotion hook")
	assert.Contains(t, err.Error(), "pg_promote refused")
}

func TestRuleStorePG_UpdateRule_PostWriteGUCFails(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	// First SetPolicy (pre-write) succeeds, second (post-write) fails.
	rs, _ := newTestRuleStoreWithFailingSSM(t, []error{nil, errors.New("post-write alter failed")})
	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	_, err := rs.UpdateRule(ctx, NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "post-write GUC")
	assert.Contains(t, err.Error(), "post-write alter failed")

	// Both writes (phase 1's proposal, phase 2's decision) already committed to
	// postgres before the post-write GUC apply ran — the cache must reflect that
	// even though UpdateRule itself returns an error.
	cached := rs.CachedPosition()
	require.NotNil(t, cached, "the decided write must be cached despite the later GUC failure")
	assert.Equal(t, int64(1), cached.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Nil(t, cached.GetPosition().GetProposal(), "phase 2 clears the proposal it decided")
}

// TestRuleStorePG_UpdateRule_RejectsLeaderChangeOutsidePromotion verifies
// that a non-promotion write can't replace an existing leader: only a
// promotion (which fences the outgoing leader via revocation first) can
// safely hand off leadership.
func TestRuleStorePG_UpdateRule_RejectsLeaderChangeOutsidePromotion(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	leaderID := testPoolerID(t, "zone1", "leader-1")
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "bootstrap", time.Now()).WithLeader(leaderID),
	)
	require.NoError(t, err)

	otherLeaderID := testPoolerID(t, "zone1", "leader-2")
	_, err = rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "config_change", "test", time.Now()).WithLeader(otherLeaderID),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UpdateRule cannot change leader outside of a promotion")

	// The decision must be untouched.
	pos, err := rs.ObservePosition(ctx)
	require.NoError(t, err)
	assert.Equal(t, "leader-1", pos.GetPosition().GetDecision().GetLeaderId().GetName())
}

func TestRuleStorePG_UpdateRule_InvalidLeaderID(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	// Underscore in Name is rejected by NewReplicaID (underscores are the
	// cell_name delimiter).
	badLeader := &clustermetadatapb.ID{Cell: "zone1", Name: "bad_name"}
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "test", time.Now()).
			WithLeader(badLeader),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid leader ID")
}

func TestRuleStorePG_UpdateRule_InvalidCohortID(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	// First member is valid; second has an underscore in Name.
	badCohort := []*clustermetadatapb.ID{
		{Cell: "zone1", Name: "good"},
		{Cell: "zone1", Name: "bad_name"},
	}
	_, err := rs.UpdateRule(ctx,
		NewRuleUpdate(1, coordinatorID, "promotion", "test", time.Now()).
			WithCohort(badCohort),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cohort member ID")
}

func TestRuleStorePG_ReconcileGUC_InvalidPolicy(t *testing.T) {
	skipIfNoPG(t)
	ctx := withTestActionLock(t)
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	// Bypass UpdateRule (which validates) and write QUORUM_TYPE_UNKNOWN
	// directly. The string is a valid enum entry so buildPoolerPosition
	// accepts it, but NewPolicyFromProto rejects it inside ReconcileGUC.
	qs := &connQueryService{conn: conn}
	_, err := qs.QueryArgs(ctx,
		"UPDATE multigres.current_rule SET durability_quorum_type = 'QUORUM_TYPE_UNKNOWN' WHERE shard_id = $1",
		[]byte("0"))
	require.NoError(t, err)

	err = rs.ReconcileGUC(ctx, false /* inRecovery */)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ReconcileGUC: invalid decision durability policy")
}

func TestRuleStorePG_ReadCurrentRule_ParseFailureFromBogusQuorumType(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	// A quorum_type string that doesn't match any enum entry makes
	// buildPoolerPosition fail with "unknown quorum_type", wrapped by
	// readCurrentRule as "failed to parse current_rule".
	qs := &connQueryService{conn: conn}
	_, err := qs.QueryArgs(ctx,
		"UPDATE multigres.current_rule SET durability_quorum_type = 'BOGUS_QUORUM_TYPE' WHERE shard_id = $1",
		[]byte("0"))
	require.NoError(t, err)

	_, err = rs.ObservePosition(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse current_rule")
}

// mustNewPolicyFromProto is a test helper that constructs a consensus.Policy
// from a proto, failing the test on error.
func mustNewPolicyFromProto(t *testing.T, dp *clustermetadatapb.DurabilityPolicy) commonconsensus.DurabilityPolicy {
	t.Helper()
	p, err := commonconsensus.NewPolicyFromProto(dp)
	require.NoError(t, err)
	return p
}
