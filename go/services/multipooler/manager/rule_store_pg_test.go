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

package manager

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/executor"
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
// It sets up the multigres schema via createRuleTables so tests do not need to.
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rs := newRuleStore(logger, qs)
	if err := rs.createRuleTables(ctx); err != nil {
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rs := newRuleStore(logger, &connQueryService{conn: conn})
	return rs, conn
}

// resetRuleStoreTables truncates and re-initialises the rule tables so each test
// starts from the zero-state sentinel row.
func resetRuleStoreTables(ctx context.Context, t *testing.T) {
	t.Helper()
	conn, err := pgTestFixture.newClientConn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	qs := &connQueryService{conn: conn}
	_, err = qs.conn.Query(ctx, "TRUNCATE multigres.current_rule, multigres.rule_history")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rs := newRuleStore(logger, qs)
	require.NoError(t, rs.createRuleTables(ctx))
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

	pos, err := rs.observePosition(ctx)
	require.NoError(t, err)
	assert.Nil(t, pos, "fresh sentinel row (coordinator_term=0) should return nil position")
}

func TestRuleStorePG_UpdateRule_FirstWrite(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	update := newRuleUpdate(1, coordinatorID, "promotion", "initial primary", time.Now())

	pos, err := rs.updateRule(ctx, update)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(1), pos.Rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Rule.RuleNumber.LeaderSubterm, "first write in a new term starts at subterm 0")
}

func TestRuleStorePG_UpdateRule_SameTermIncrementsSubterm(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	pos1, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos1.Rule.RuleNumber.LeaderSubterm)

	pos2, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "config_change", "second", now))
	require.NoError(t, err)
	assert.Equal(t, int64(1), pos2.Rule.RuleNumber.LeaderSubterm, "second write in same term increments subterm")
}

func TestRuleStorePG_UpdateRule_NewTermResetsSubterm(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Establish term 1 with a few subterms.
	for range 3 {
		_, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "config_change", "setup", now))
		require.NoError(t, err)
	}

	// Advance to term 2: subterm must reset to 0.
	pos, err := rs.updateRule(ctx, newRuleUpdate(2, coordinatorID, "promotion", "new coordinator", now))
	require.NoError(t, err)
	assert.Equal(t, int64(2), pos.Rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Rule.RuleNumber.LeaderSubterm, "new term resets subterm to 0")
}

func TestRuleStorePG_UpdateRule_StaleTermRejected(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Advance to term 2.
	_, err := rs.updateRule(ctx, newRuleUpdate(2, coordinatorID, "promotion", "initial", now))
	require.NoError(t, err)

	// Attempt to write with stale term 1.
	_, err = rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "config_change", "stale", now))
	require.Error(t, err, "writing with a stale term must fail")
	assert.Contains(t, err.Error(), "already at equal or higher position")
}

func TestRuleStorePG_UpdateRule_ObserveAfterWrite(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
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

	_, err := rs.updateRule(ctx,
		newRuleUpdate(3, coordinatorID, "promotion", "bootstrap", now).
			withLeader(leaderID).
			withCohort(cohort),
	)
	require.NoError(t, err)

	pos, err := rs.observePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(3), pos.Rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(0), pos.Rule.RuleNumber.LeaderSubterm)

	require.NotNil(t, pos.Rule.PrimaryId)
	assert.Equal(t, "zone1", pos.Rule.PrimaryId.Cell)
	assert.Equal(t, "leader-1", pos.Rule.PrimaryId.Name)

	require.NotNil(t, pos.Rule.CoordinatorId)
	assert.Equal(t, "zone1", pos.Rule.CoordinatorId.Cell)
	assert.Equal(t, "coordinator-1", pos.Rule.CoordinatorId.Name)

	require.Len(t, pos.Rule.CohortMembers, 2)
	assert.Equal(t, "member-1", pos.Rule.CohortMembers[0].Name)
	assert.Equal(t, "member-2", pos.Rule.CohortMembers[1].Name)

	assert.NotEmpty(t, pos.Lsn, "LSN should be populated after a write")
}

func TestRuleStorePG_UpdateRule_HistoryFields(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
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

	_, err := rs.updateRule(ctx,
		newRuleUpdate(5, coordinatorID, "promotion", "bootstrap failover", now).
			withLeader(leaderID).
			withCohort(cohort).
			withWALPosition(walPos).
			withOperation(op).
			withAcceptedMembers(accepted),
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
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Write the first rule so we know the exact term/subterm.
	pos1, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)
	term := pos1.Rule.RuleNumber.CoordinatorTerm  // 1
	subterm := pos1.Rule.RuleNumber.LeaderSubterm // 0

	// CAS: only proceed if current rule is still (term=1, subterm=0).
	pos2, err := rs.updateRule(ctx,
		newRuleUpdate(1, coordinatorID, "config_change", "cas write", now).
			withPreviousRule(term, subterm),
	)
	require.NoError(t, err, "CAS should succeed when term/subterm match")
	assert.Equal(t, int64(1), pos2.Rule.RuleNumber.LeaderSubterm, "subterm should advance to 1")
}

func TestRuleStorePG_UpdateRule_CASConflict(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	// Write once to advance past the sentinel.
	_, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "promotion", "first", now))
	require.NoError(t, err)

	// CAS with stale coordinates should fail with errRuleConflict.
	_, err = rs.updateRule(ctx,
		newRuleUpdate(1, coordinatorID, "config_change", "stale cas", now).
			withPreviousRule(0, 0), // sentinel coordinates no longer current
	)
	require.ErrorIs(t, err, errRuleConflict, "CAS with wrong term/subterm must return errRuleConflict")
}

func TestRuleStorePG_UpdateRule_HistoryRecorded(t *testing.T) {
	skipIfNoPG(t)
	ctx := t.Context()
	resetRuleStoreTables(ctx, t)

	rs, conn := newTestRuleStore(ctx, t)
	defer conn.Close()

	coordinatorID := testPoolerID(t, "zone1", "coordinator-1")
	now := time.Now()

	for i := range 5 {
		_, err := rs.updateRule(ctx, newRuleUpdate(1, coordinatorID, "config_change", fmt.Sprintf("write %d", i), now))
		require.NoError(t, err)
	}

	records, err := rs.queryRuleHistory(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, records, 5, "every updateRule call must append a rule_history row")
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

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			conn, err := pgTestFixture.newClientConn(ctx)
			if err != nil {
				errs[idx] = fmt.Errorf("connect: %w", err)
				return
			}
			defer conn.Close()

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			rs := newRuleStore(logger, &connQueryService{conn: conn})
			_, errs[idx] = rs.updateRule(ctx,
				newRuleUpdate(1, coordinatorID, "config_change",
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

	pos, err := rs.observePosition(ctx)
	require.NoError(t, err)
	require.NotNil(t, pos)
	assert.Equal(t, int64(1), pos.Rule.RuleNumber.CoordinatorTerm)
	assert.Equal(t, int64(goroutines-1), pos.Rule.RuleNumber.LeaderSubterm,
		"final subterm should equal goroutines-1 after %d serialized writes", goroutines)
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// testPoolerID builds a *clustermetadatapb.ID for use in test updates.
func testPoolerID(t *testing.T, cell, name string) *clustermetadatapb.ID {
	t.Helper()
	return &clustermetadatapb.ID{Cell: cell, Name: name}
}
