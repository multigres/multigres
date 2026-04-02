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

package queryserving

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestBufferPlannedFailover verifies that multigateway's failover buffering
// absorbs a planned failover with zero application-visible errors.
//
// The test:
//  1. Creates an isolated 3-node cluster with multiorch and multigateway (buffering enabled).
//  2. Starts continuous writes through multigateway.
//  3. Triggers a planned failover via BeginTerm (emergency demotion).
//  4. Waits for a new primary to be elected.
//  5. Asserts zero failed writes — the buffer should have held all in-flight
//     requests until the new primary appeared.
func TestBufferPlannedFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestBufferPlannedFailover in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping buffer failover test")
	}

	setup, cleanup := newBufferTestCluster(t)
	defer cleanup()

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()

	// Start continuous writes. The query timeout must exceed the buffer window (10s)
	// so that buffered requests survive the failover instead of timing out on the
	// client side.
	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
		shardsetup.WithQueryTimeout(15*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes via multigateway (table: %s)...", validator.TableName())
	validator.Start(t)

	// Wait for writes to accumulate before triggering failover.
	waitForMinWrites(t, validator, 10, 10*time.Second)
	preSuccess, preFailed := validator.Stats()
	t.Logf("Pre-failover writes: %d successful, %d failed", preSuccess, preFailed)

	// Trigger planned failover.
	triggerFailover(t, setup)

	// Wait for writes to resume on the new primary.
	waitForWriteProgress(t, validator, 10*time.Second)

	// Stop writes and check results.
	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Final writes: %d successful, %d failed", successfulWrites, failedWrites)

	assert.Zero(t, failedWrites,
		"buffering should absorb the planned failover with zero failed writes")
	assert.Greater(t, successfulWrites, 0,
		"should have some successful writes")
}

// TestBufferTransactionsAndPreparedStatements verifies that buffering works
// correctly with transactions (BEGIN/COMMIT) and prepared statements (extended
// query protocol). In-flight transactions on the old primary should complete
// via the graceful drain, while new transactions and prepared statements
// should be buffered and retried on the new primary.
func TestBufferTransactionsAndPreparedStatements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	setup, cleanup := newBufferTestCluster(t)
	defer cleanup()

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()

	// Create test tables.
	_, err := gatewayDB.Exec("CREATE TABLE IF NOT EXISTS buf_txn_test (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = gatewayDB.Exec("CREATE TABLE IF NOT EXISTS buf_prep_test (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = gatewayDB.ExecContext(ctx, "DROP TABLE IF EXISTS buf_txn_test")
		_, _ = gatewayDB.ExecContext(ctx, "DROP TABLE IF EXISTS buf_prep_test")
	})

	var (
		wg          sync.WaitGroup
		txnSuccess  atomic.Int64
		txnFailed   atomic.Int64
		prepSuccess atomic.Int64
		prepFailed  atomic.Int64
		stop        = make(chan struct{})
		txnNextID   atomic.Int64
		prepNextID  atomic.Int64
	)

	// Worker: continuous transactions (BEGIN → INSERT → COMMIT).
	runTxnWorker := func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				id := txnNextID.Add(1)
				if err := execTransaction(gatewayDB, id); err != nil {
					txnFailed.Add(1)
				} else {
					txnSuccess.Add(1)
				}
			}
		}
	}

	// Worker: continuous prepared statements (extended query protocol).
	// database/sql uses prepared statements under the hood with $1 params.
	runPrepWorker := func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				id := prepNextID.Add(1)
				if err := execPrepared(gatewayDB, id); err != nil {
					prepFailed.Add(1)
				} else {
					prepSuccess.Add(1)
				}
			}
		}
	}

	// Start workers.
	for range 2 {
		wg.Add(1)
		go runTxnWorker()
	}
	for range 2 {
		wg.Add(1)
		go runPrepWorker()
	}

	// Wait for writes to accumulate.
	require.Eventually(t, func() bool {
		return txnSuccess.Load() >= 10 && prepSuccess.Load() >= 10
	}, 10*time.Second, 50*time.Millisecond, "writes should accumulate before failover")
	t.Logf("Pre-failover: txn=%d/%d prep=%d/%d (success/failed)",
		txnSuccess.Load(), txnFailed.Load(), prepSuccess.Load(), prepFailed.Load())

	// Trigger planned failover.
	triggerFailover(t, setup)

	// Wait for writes to resume on the new primary.
	preCount := txnSuccess.Load() + prepSuccess.Load()
	require.Eventually(t, func() bool {
		return (txnSuccess.Load() + prepSuccess.Load()) > preCount+5
	}, 10*time.Second, 50*time.Millisecond, "writes should resume after failover")

	close(stop)
	wg.Wait()

	t.Logf("Final: txn=%d/%d prep=%d/%d (success/failed)",
		txnSuccess.Load(), txnFailed.Load(), prepSuccess.Load(), prepFailed.Load())

	assert.Zero(t, txnFailed.Load(),
		"transactions should be buffered with zero failures during planned failover")
	assert.Zero(t, prepFailed.Load(),
		"prepared statements should be buffered with zero failures during planned failover")
	assert.Greater(t, txnSuccess.Load(), int64(0), "should have some successful transactions")
	assert.Greater(t, prepSuccess.Load(), int64(0), "should have some successful prepared statements")
}

// TestBufferMultipleFailovers verifies that buffering works across multiple
// consecutive failovers. The primary is failed over back and forth between
// poolers, and continuous writes should see zero errors throughout.
func TestBufferMultipleFailovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}

	// Use longer buffer timeouts than the single-failover tests. Consecutive
	// failovers are slower because multiorch must detect the dead primary,
	// run DemoteStalePrimary (pg_rewind), and restart it before the next
	// failover can proceed. On CI this can exceed the default 10s window.
	setup, cleanup := newBufferTestClusterWithConfig(t,
		"--buffer-enabled",
		"--buffer-window", "30s",
		"--buffer-size", "1000",
		"--buffer-max-failover-duration", "60s",
		"--buffer-min-time-between-failovers", "0s",
		"--buffer-drain-concurrency", "5",
	)
	defer cleanup()

	gatewayDB := openGatewayDB(t, setup)
	defer gatewayDB.Close()

	// Start continuous writes. The query timeout must be at least as long as the
	// buffer window so that buffered requests survive slow failovers instead of
	// timing out on the client side.
	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
		shardsetup.WithQueryTimeout(35*time.Second),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes (table: %s)...", validator.TableName())
	validator.Start(t)

	const numFailovers = 3
	for i := range numFailovers {
		preSuccess, preFailed := validator.Stats()
		t.Logf("Failover %d/%d: pre-stats %d successful, %d failed", i+1, numFailovers, preSuccess, preFailed)

		triggerFailover(t, setup)

		// Verify writes are flowing before triggering the next failover.
		waitForWriteProgress(t, validator, 30*time.Second)

		postSuccess, postFailed := validator.Stats()
		t.Logf("Failover %d/%d: post-stats %d successful, %d failed", i+1, numFailovers, postSuccess, postFailed)
	}

	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Final after %d failovers: %d successful, %d failed", numFailovers, successfulWrites, failedWrites)

	assert.Zero(t, failedWrites,
		"buffering should absorb all %d planned failovers with zero failed writes", numFailovers)
	assert.Greater(t, successfulWrites, 0,
		"should have some successful writes")
}

// --- helpers ---

// newBufferTestCluster creates a standard 3-node cluster with buffering enabled.
func newBufferTestCluster(t *testing.T) (*shardsetup.ShardSetup, func()) {
	t.Helper()
	return newBufferTestClusterWithConfig(t,
		"--buffer-enabled",
		"--buffer-window", "10s",
		"--buffer-size", "1000",
		"--buffer-max-failover-duration", "20s",
		"--buffer-min-time-between-failovers", "0s",
		"--buffer-drain-concurrency", "5",
	)
}

// newBufferTestClusterWithConfig creates a 3-node cluster with custom buffer flags.
func newBufferTestClusterWithConfig(t *testing.T, bufferArgs ...string) (*shardsetup.ShardSetup, func()) {
	t.Helper()
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(3),
		shardsetup.WithMultigateway(),
		func(c *shardsetup.SetupConfig) {
			c.MultigatewayExtraArgs = append(c.MultigatewayExtraArgs, bufferArgs...)
		},
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithPrimaryFailoverGracePeriod("0s", "0s"),
	)
	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary should exist after bootstrap")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	return setup, cleanup
}

// openGatewayDB opens a database/sql connection to the multigateway.
func openGatewayDB(t *testing.T, setup *shardsetup.ShardSetup) *sql.DB {
	t.Helper()
	connStr := fmt.Sprintf(
		"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	require.NoError(t, db.Ping(), "failed to ping multigateway")
	return db
}

// triggerFailover demotes the current primary via BeginTerm and waits for
// a new primary to be elected.
func triggerFailover(t *testing.T, setup *shardsetup.ShardSetup) {
	t.Helper()

	currentPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, currentPrimary)
	currentPrimaryName := currentPrimary.Name

	primaryClient, err := shardsetup.NewMultipoolerClient(currentPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)

	statusResp, err := primaryClient.Manager.Status(
		utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	oldTerm := statusResp.Status.ConsensusTerm.TermNumber

	t.Logf("Triggering failover: BeginTerm on %s (term %d → %d)", currentPrimaryName, oldTerm, oldTerm+1)

	beginTermResp, err := primaryClient.Consensus.BeginTerm(
		utils.WithTimeout(t, 10*time.Second),
		&consensusdatapb.BeginTermRequest{
			Term: oldTerm + 1,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      setup.CellName,
				Name:      "test-coordinator",
			},
			Action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		})
	primaryClient.Close()

	require.NoError(t, err, "BeginTerm should succeed")
	require.True(t, beginTermResp.Accepted, "primary should accept BeginTerm")
	t.Logf("BeginTerm accepted, emergency demotion triggered")

	// Trigger immediate recovery to elect a new primary and fully stabilize the cluster.
	setup.RequireRecovery(t, "multiorch", 90*time.Second)

	newPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, newPrimary, "a primary should exist after recovery")
	t.Logf("Primary after recovery: %s (was: %s)", newPrimary.Name, currentPrimaryName)
}

// execTransaction runs a single INSERT inside a BEGIN/COMMIT transaction.
func execTransaction(db *sql.DB, id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO buf_txn_test (id, val) VALUES ($1, $2)", id, fmt.Sprintf("txn-%d", id)); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// execPrepared runs a single INSERT using a prepared statement.
// database/sql automatically uses the extended query protocol with $1 params.
func execPrepared(db *sql.DB, id int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "INSERT INTO buf_prep_test (id, val) VALUES ($1, $2)")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.ExecContext(ctx, id, fmt.Sprintf("prep-%d", id))
	return err
}

// waitForMinWrites polls until at least minWrites successful writes have been recorded.
func waitForMinWrites(t *testing.T, v *shardsetup.WriterValidator, minWrites int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		s, _ := v.Stats()
		return s >= minWrites
	}, timeout, 50*time.Millisecond, "expected at least %d successful writes", minWrites)
}

// waitForWriteProgress polls until new successful writes are observed, confirming
// that the cluster is serving and writes are flowing. This replaces fixed time.Sleep
// calls that could be too short under CI load or too long in the common case.
func waitForWriteProgress(t *testing.T, v *shardsetup.WriterValidator, timeout time.Duration) {
	t.Helper()
	baseline, _ := v.Stats()
	require.Eventually(t, func() bool {
		s, _ := v.Stats()
		return s > baseline+5
	}, timeout, 50*time.Millisecond, "writes should progress beyond baseline of %d", baseline)
}
