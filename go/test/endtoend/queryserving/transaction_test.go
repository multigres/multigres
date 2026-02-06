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

package queryserving

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// transactionTestCase defines a single transaction test scenario.
type transactionTestCase struct {
	name string
	// testFunc executes the test scenario and may return an error if expected
	testFunc func(ctx context.Context, t *testing.T, conn *client.Conn) error
	// verifyFunc verifies the final state after testFunc completes
	verifyFunc func(ctx context.Context, t *testing.T, conn *client.Conn)
}

// testTables lists all tables that may be created during transaction tests.
// This ensures cleanup happens even if tests fail mid-execution.
var testTables = []string{
	// multiStatementTestCases
	"users_ex1", "users_ex2", "users_ex3", "users_ex4", "users_ex5", "users_ex6",
	// autocommitTestCases
	"users_autocommit",
	// ddlTransactionTestCases
	"temp_ddl_test", "t1_ddl_test", "t2_ddl_test",
	// explicitTransactionTestCases
	"txn_explicit_test", "txn_rollback_test", "txn_savepoint_test", "txn_abort_test",
}

// cleanupTestTables drops all test tables to ensure a clean state.
func cleanupTestTables(ctx context.Context, conn *client.Conn) {
	for _, table := range testTables {
		_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS "+table)
	}
}

// runTransactionTests executes a slice of transaction test cases.
func runTransactionTests(t *testing.T, setup *shardsetup.ShardSetup, testCases []transactionTestCase) {
	ctx := utils.WithTimeout(t, 60*time.Second)

	primary := setup.GetMultipoolerInstance(setup.PrimaryName)
	require.NotNil(t, primary, "Primary instance should exist")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := client.Connect(ctx, &client.Config{
				Host:        "localhost",
				Port:        primary.Pgctld.PgPort,
				User:        "postgres",
				Password:    shardsetup.TestPostgresPassword,
				Database:    "postgres",
				DialTimeout: 5 * time.Second,
			})
			require.NoError(t, err)
			defer conn.Close()

			// Register cleanup to ensure test tables are dropped even if test fails.
			// This runs after the test completes (success or failure).
			// We use a fresh context with timeout since the test context may be cancelled.
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cleanupConn, cleanupErr := client.Connect(cleanupCtx, &client.Config{
					Host:        "localhost",
					Port:        primary.Pgctld.PgPort,
					User:        "postgres",
					Password:    shardsetup.TestPostgresPassword,
					Database:    "postgres",
					DialTimeout: 5 * time.Second,
				})
				if cleanupErr != nil {
					return
				}
				defer cleanupConn.Close()
				cleanupTestTables(cleanupCtx, cleanupConn)
			})

			// Run the test
			_ = tc.testFunc(ctx, t, conn)

			// Run verification
			if tc.verifyFunc != nil {
				tc.verifyFunc(ctx, t, conn)
			}
		})
	}
}

// TestTransactionScenarios tests PostgreSQL transaction behavior
func TestTransactionScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedSetup(t)

	t.Run("MultiStatement", func(t *testing.T) {
		runTransactionTests(t, setup, multiStatementTestCases())
	})

	t.Run("Autocommit", func(t *testing.T) {
		runTransactionTests(t, setup, autocommitTestCases())
	})

	t.Run("DDLTransactions", func(t *testing.T) {
		runTransactionTests(t, setup, ddlTransactionTestCases())
	})

	t.Run("ExplicitTransactions", func(t *testing.T) {
		runTransactionTests(t, setup, explicitTransactionTestCases())
	})
}

// multiStatementTestCases returns test cases for multi-statement behavior (Section 1).
func multiStatementTestCases() []transactionTestCase {
	return []transactionTestCase{
		{
			// Example 1: Successful Multi-Statement (Implicit Transaction)
			// All statements execute within TBLOCK_IMPLICIT_INPROGRESS and commit together.
			name: "Example1_SuccessfulMultiStatement",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex1")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex1 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send multiple statements as a single query string
				results, err := conn.Query(ctx, `
					INSERT INTO users_ex1 VALUES (1, 'Alice');
					INSERT INTO users_ex1 VALUES (2, 'Bob');
					INSERT INTO users_ex1 VALUES (3, 'Charlie')
				`)
				require.NoError(t, err)
				require.Len(t, results, 3, "Expected 3 result sets for 3 statements")
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM users_ex1")
				require.NoError(t, err)
				assert.Equal(t, "3", string(results[0].Rows[0].Values[0]))
			},
		},
		{
			// Example 2: Failure in Multi-Statement (Implicit Transaction Rollback)
			// If one statement fails in an implicit transaction, ALL statements are rolled back.
			name: "Example2_FailureRollsBackAll",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex2")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex2 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send multi-statement with a duplicate key error
				_, err = conn.Query(ctx, `
					INSERT INTO users_ex2 VALUES (1, 'Alice');
					INSERT INTO users_ex2 VALUES (2, 'Bob');
					INSERT INTO users_ex2 VALUES (1, 'Duplicate');
					INSERT INTO users_ex2 VALUES (3, 'Charlie')
				`)
				require.Error(t, err, "Expected error from duplicate key")

				var diag *sqltypes.PgDiagnostic
				require.True(t, errors.As(err, &diag), "Expected PostgreSQL error")
				assert.Equal(t, "23505", diag.Code, "Expected unique_violation error code")
				return err
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Table should be EMPTY - all INSERTs were rolled back
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM users_ex2")
				require.NoError(t, err)
				assert.Equal(t, "0", string(results[0].Rows[0].Values[0]),
					"Table should be empty - all statements rolled back")
			},
		},
		{
			// Example 3: Multi-Statement with BEGIN (Implicit to Explicit Transition)
			name: "Example3_BeginCommitsImplicit",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex3")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex3 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send multi-statement with BEGIN in the middle
				results, err := conn.Query(ctx, `
					INSERT INTO users_ex3 VALUES (1, 'Alice');
					BEGIN;
					INSERT INTO users_ex3 VALUES (2, 'Bob');
					INSERT INTO users_ex3 VALUES (3, 'Charlie');
					COMMIT
				`)
				require.NoError(t, err)
				require.Len(t, results, 5, "Expected 5 result sets")
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM users_ex3")
				require.NoError(t, err)
				assert.Equal(t, "3", string(results[0].Rows[0].Values[0]))
			},
		},
		{
			// Example 4: Failure AFTER BEGIN with no prior COMMIT
			// When there's no explicit COMMIT before the error, everything is rolled back.
			// BEGIN does NOT commit the preceding implicit transaction - it adopts it
			// into the explicit transaction block.
			name: "Example4_FailureAfterBeginNoCommit",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex4")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex4 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send as a single multi-statement query:
				// Alice is inserted, BEGIN adopts her into explicit transaction,
				// then error causes everything to roll back (no COMMIT to save anything)
				_, err = conn.Query(ctx, `
					INSERT INTO users_ex4 VALUES (1, 'Alice');
					BEGIN;
					INSERT INTO users_ex4 VALUES (2, 'Bob');
					INSERT INTO users_ex4 VALUES (2, 'Duplicate');
					INSERT INTO users_ex4 VALUES (3, 'Charlie');
					COMMIT
				`)
				require.Error(t, err, "Expected error from duplicate key")

				// Clear the aborted transaction state
				_, _ = conn.Query(ctx, "ROLLBACK")
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Nothing survives - no explicit COMMIT before the error
				results, err := conn.Query(ctx, "SELECT id, name FROM users_ex4 ORDER BY id")
				require.NoError(t, err)
				assert.Empty(t, results[0].Rows,
					"Table should be empty - no COMMIT before error means everything rolled back")
			},
		},
		{
			// Example 5: Failure BEFORE BEGIN (Nothing Committed)
			// If failure occurs in implicit transaction before BEGIN, nothing is committed.
			name: "Example5_FailureBeforeBegin",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex5")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex5 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send multi-statement with failure before BEGIN
				_, err = conn.Query(ctx, `
					INSERT INTO users_ex5 VALUES (1, 'Alice');
					INSERT INTO users_ex5 VALUES (1, 'Duplicate');
					BEGIN;
					INSERT INTO users_ex5 VALUES (2, 'Bob');
					COMMIT
				`)
				require.Error(t, err, "Expected error from duplicate key")
				return err
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Table should be EMPTY - failure before BEGIN rolls back everything
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM users_ex5")
				require.NoError(t, err)
				assert.Equal(t, "0", string(results[0].Rows[0].Values[0]),
					"Table should be empty - failure before BEGIN rolls back everything")
			},
		},
		{
			// Example 6: Multiple BEGIN/COMMIT Blocks - Only Committed Data Survives
			// Tests that only data saved by explicit COMMIT survives a later error.
			// Alice and Bob are committed by first COMMIT, Charlie is lost (inserted after
			// COMMIT but before error, never committed), David is lost (error before COMMIT).
			name: "Example6_MultipleBeginCommitBlocks",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_ex6")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_ex6 (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Send as a single multi-statement query to test multiple transaction blocks
				_, err = conn.Query(ctx, `
					INSERT INTO users_ex6 VALUES (1, 'Alice');
					BEGIN;
					INSERT INTO users_ex6 VALUES (2, 'Bob');
					COMMIT;
					INSERT INTO users_ex6 VALUES (3, 'Charlie');
					BEGIN;
					INSERT INTO users_ex6 VALUES (4, 'David');
					INSERT INTO users_ex6 VALUES (4, 'Duplicate');
					COMMIT
				`)
				require.Error(t, err, "Expected error from duplicate key")

				// Clear the aborted transaction state
				_, _ = conn.Query(ctx, "ROLLBACK")
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Only Alice and Bob survive - they were committed before the error.
				// Charlie is lost because she was inserted after COMMIT but before the error,
				// with no subsequent COMMIT to save her.
				results, err := conn.Query(ctx, "SELECT id, name FROM users_ex6 ORDER BY id")
				require.NoError(t, err)
				require.Len(t, results[0].Rows, 2, "Only Alice and Bob should survive (committed before error)")
				assert.Equal(t, "Alice", string(results[0].Rows[0].Values[1]))
				assert.Equal(t, "Bob", string(results[0].Rows[1].Values[1]))
			},
		},
	}
}

// autocommitTestCases returns test cases for autocommit behavior (Section 1.5).
func autocommitTestCases() []transactionTestCase {
	return []transactionTestCase{
		{
			// Single statements without BEGIN auto-commit independently.
			name: "SingleStatementsAutoCommit",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS users_autocommit")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE users_autocommit (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Each statement auto-commits
				_, err = conn.Query(ctx, "INSERT INTO users_autocommit VALUES (1, 'Alice')")
				require.NoError(t, err)

				// This will fail but shouldn't affect Alice
				_, err = conn.Query(ctx, "INSERT INTO users_autocommit VALUES (1, 'Duplicate')")
				require.Error(t, err, "Expected duplicate key error")

				// This should succeed
				_, err = conn.Query(ctx, "INSERT INTO users_autocommit VALUES (2, 'Bob')")
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Alice and Bob exist (Alice survived the subsequent error)
				results, err := conn.Query(ctx, "SELECT id, name FROM users_autocommit ORDER BY id")
				require.NoError(t, err)
				require.Len(t, results[0].Rows, 2, "Alice and Bob should both exist")
				assert.Equal(t, "Alice", string(results[0].Rows[0].Values[1]))
				assert.Equal(t, "Bob", string(results[0].Rows[1].Values[1]))
			},
		},
	}
}

// ddlTransactionTestCases returns test cases for DDL transaction behavior (Section 1.7).
func ddlTransactionTestCases() []transactionTestCase {
	return []transactionTestCase{
		{
			// PostgreSQL DDL is fully transactional - can be rolled back.
			name: "DDLIsTransactional",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS temp_ddl_test")

				// Create table, insert data, alter table, then ROLLBACK
				_, err := conn.Query(ctx, `
					BEGIN;
					CREATE TABLE temp_ddl_test (id INT, name TEXT);
					INSERT INTO temp_ddl_test VALUES (1, 'Alice');
					ALTER TABLE temp_ddl_test ADD COLUMN email TEXT;
					ROLLBACK
				`)
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Table should NOT exist (CREATE TABLE was rolled back)
				_, err := conn.Query(ctx, "SELECT * FROM temp_ddl_test")
				require.Error(t, err, "Table should not exist after rollback")

				var diag *sqltypes.PgDiagnostic
				require.True(t, errors.As(err, &diag), "Expected PostgreSQL error")
				assert.Equal(t, "42P01", diag.Code, "Expected undefined_table error code")
			},
		},
		{
			// DDL + DML failure in implicit transaction rolls back DDL too.
			name: "DDLRollbackOnDMLFailure",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS t1_ddl_test")

				// Create table with PRIMARY KEY, insert, then insert duplicate
				_, err := conn.Query(ctx, `
					CREATE TABLE t1_ddl_test (id INT PRIMARY KEY);
					INSERT INTO t1_ddl_test VALUES (1);
					INSERT INTO t1_ddl_test VALUES (1)
				`)
				require.Error(t, err, "Expected duplicate key error")
				return err
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Table should NOT exist (CREATE TABLE was rolled back)
				_, err := conn.Query(ctx, "SELECT * FROM t1_ddl_test")
				require.Error(t, err, "Table should not exist after rollback")

				var diag *sqltypes.PgDiagnostic
				require.True(t, errors.As(err, &diag), "Expected PostgreSQL error")
				assert.Equal(t, "42P01", diag.Code, "Expected undefined_table error code")
			},
		},
		{
			// DDL in explicit transaction survives if transaction commits.
			name: "DDLInExplicitTransactionWithRecovery",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, _ = conn.Query(ctx, "DROP TABLE IF EXISTS t2_ddl_test")

				// First transaction: create table and commit
				_, err := conn.Query(ctx, `
					BEGIN;
					CREATE TABLE t2_ddl_test (id INT PRIMARY KEY);
					INSERT INTO t2_ddl_test VALUES (1);
					COMMIT
				`)
				require.NoError(t, err)

				// Second transaction: try to drop and fail - should rollback DROP
				_, err = conn.Query(ctx, `
					BEGIN;
					DROP TABLE t2_ddl_test;
					INSERT INTO nonexistent_table VALUES (1)
				`)
				require.Error(t, err, "Expected error from nonexistent table")

				_, _ = conn.Query(ctx, "ROLLBACK")
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Table should still exist (DROP was rolled back)
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM t2_ddl_test")
				require.NoError(t, err, "Table should still exist after failed transaction")
				assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
			},
		},
		{
			// Certain DDL statements cannot run inside a transaction block.
			name: "DDLCannotRunInTransaction",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				// CREATE DATABASE cannot run inside a transaction
				_, err := conn.Query(ctx, `
					BEGIN;
					CREATE DATABASE testdb_forbidden
				`)
				require.Error(t, err, "CREATE DATABASE should fail inside transaction")

				var diag *sqltypes.PgDiagnostic
				require.True(t, errors.As(err, &diag), "Expected PostgreSQL error")
				assert.Equal(t, "25001", diag.Code,
					"Expected active_sql_transaction error for CREATE DATABASE in transaction")

				_, _ = conn.Query(ctx, "ROLLBACK")
				return err
			},
			verifyFunc: nil,
		},
	}
}

// explicitTransactionTestCases returns test cases for explicit transaction behavior.
func explicitTransactionTestCases() []transactionTestCase {
	return []transactionTestCase{
		{
			name: "BeginCommit",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS txn_explicit_test")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE txn_explicit_test (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				_, err = conn.Query(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_explicit_test VALUES (1, 'Alice')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_explicit_test VALUES (2, 'Bob')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "COMMIT")
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM txn_explicit_test")
				require.NoError(t, err)
				assert.Equal(t, "2", string(results[0].Rows[0].Values[0]))
			},
		},
		{
			name: "BeginRollback",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS txn_rollback_test")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE txn_rollback_test (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				// Insert initial data (committed)
				_, err = conn.Query(ctx, "INSERT INTO txn_rollback_test VALUES (1, 'Alice')")
				require.NoError(t, err)

				_, err = conn.Query(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_rollback_test VALUES (2, 'Bob')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_rollback_test VALUES (3, 'Charlie')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "ROLLBACK")
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Only initial data exists
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM txn_rollback_test")
				require.NoError(t, err)
				assert.Equal(t, "1", string(results[0].Rows[0].Values[0]),
					"Only Alice should exist after rollback")
			},
		},
		{
			name: "Savepoint",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS txn_savepoint_test")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE txn_savepoint_test (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				_, err = conn.Query(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_savepoint_test VALUES (1, 'Alice')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "SAVEPOINT sp1")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_savepoint_test VALUES (2, 'Bob')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "ROLLBACK TO SAVEPOINT sp1")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_savepoint_test VALUES (3, 'Charlie')")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "COMMIT")
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				// Alice and Charlie exist, but not Bob
				results, err := conn.Query(ctx, "SELECT id, name FROM txn_savepoint_test ORDER BY id")
				require.NoError(t, err)
				require.Len(t, results[0].Rows, 2, "Alice and Charlie should exist")
				assert.Equal(t, "Alice", string(results[0].Rows[0].Values[1]))
				assert.Equal(t, "Charlie", string(results[0].Rows[1].Values[1]))
			},
		},
		{
			// Error causes transaction to enter aborted state
			name: "AbortedTransactionState",
			testFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) error {
				_, err := conn.Query(ctx, "DROP TABLE IF EXISTS txn_abort_test")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "CREATE TABLE txn_abort_test (id INT PRIMARY KEY, name TEXT)")
				require.NoError(t, err)

				_, err = conn.Query(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Query(ctx, "INSERT INTO txn_abort_test VALUES (1, 'Alice')")
				require.NoError(t, err)

				// Cause an error
				_, err = conn.Query(ctx, "INSERT INTO txn_abort_test VALUES (1, 'Duplicate')")
				require.Error(t, err, "Expected duplicate key error")

				// Transaction is now in aborted state - further commands should fail
				_, err = conn.Query(ctx, "INSERT INTO txn_abort_test VALUES (2, 'Bob')")
				require.Error(t, err, "Should fail in aborted transaction")

				var diag *sqltypes.PgDiagnostic
				require.True(t, errors.As(err, &diag), "Expected PostgreSQL error")
				assert.Equal(t, "25P02", diag.Code, "Expected in_failed_sql_transaction error")

				_, err = conn.Query(ctx, "ROLLBACK")
				require.NoError(t, err)
				return nil
			},
			verifyFunc: func(ctx context.Context, t *testing.T, conn *client.Conn) {
				results, err := conn.Query(ctx, "SELECT COUNT(*) FROM txn_abort_test")
				require.NoError(t, err)
				assert.Equal(t, "0", string(results[0].Rows[0].Values[0]))
			},
		},
	}
}
