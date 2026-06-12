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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestErrorFormat_UndefinedColumnPosition tests that undefined column errors preserve the Position field,
// which enables psql to show LINE indicator and ^ marker pointing to the error location.
// Note: Pure syntax errors (like "SELECT * FORM users") are caught by multigateway's own parser
// before reaching PostgreSQL. This test uses queries that parse correctly but fail at PostgreSQL
// with Position information.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_UndefinedColumnPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			// Create a temp table and query a non-existent column
			// This parses correctly but PostgreSQL will return an error with Position
			tableName := fmt.Sprintf("pos_test_%d", time.Now().UnixNano())
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int, name text)", tableName))
			require.NoError(t, err)
			defer func() {
				_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableName)
			}()

			// Query a non-existent column - PostgreSQL will report Position
			_, err = conn.Exec(ctx, "SELECT nonexistent_column FROM "+tableName)
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

			// Verify error fields - 42703 = undefined_column
			assert.Equal(t, "ERROR", pgErr.Severity)
			assert.Equal(t, "42703", pgErr.Code)
			assert.Contains(t, pgErr.Message, "nonexistent_column")
			assert.Greater(t, pgErr.Position, int32(0), "Position should be set for undefined column errors")
			t.Logf("Undefined column error at position %d: %s", pgErr.Position, pgErr.Message)
		})
	}
}

// TestErrorFormat_TypeErrorSQLState tests that type conversion errors preserve the SQLSTATE code.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_TypeErrorSQLState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			// Invalid boolean literal
			_, err = conn.Exec(ctx, "SELECT bool 'invalid_bool'")
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

			// 22P02 = invalid_text_representation
			assert.Equal(t, "ERROR", pgErr.Severity)
			assert.Equal(t, "22P02", pgErr.Code, "SQLSTATE should be 22P02 for invalid boolean")
			assert.Contains(t, pgErr.Message, "invalid input syntax for type boolean")
			t.Logf("Type error: Code=%s Message=%s", pgErr.Code, pgErr.Message)
		})
	}
}

// TestErrorFormat_ConstraintViolation tests that constraint violation errors include
// Schema, Table, and Constraint fields.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_ConstraintViolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			tableName := fmt.Sprintf("error_test_%d", time.Now().UnixNano())

			// Create a table with constraints
			_, err = conn.Exec(ctx, fmt.Sprintf(`
				CREATE TABLE %s (
					id INT PRIMARY KEY,
					email TEXT UNIQUE,
					value INT CHECK (value > 0)
				)
			`, tableName))
			require.NoError(t, err)
			defer func() {
				_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableName)
			}()

			t.Run("unique_constraint_violation", func(t *testing.T) {
				// Insert a row
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, email, value) VALUES (1, 'test@example.com', 10)", tableName))
				require.NoError(t, err)

				// Try to insert duplicate email (unique constraint violation)
				_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, email, value) VALUES (2, 'test@example.com', 20)", tableName))
				require.Error(t, err)

				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

				// 23505 = unique_violation
				assert.Equal(t, "ERROR", pgErr.Severity)
				assert.Equal(t, "23505", pgErr.Code, "SQLSTATE should be 23505 for unique violation")
				assert.Equal(t, "public", pgErr.SchemaName, "Schema should be 'public'")
				assert.Equal(t, tableName, pgErr.TableName, "Table name should match")
				assert.NotEmpty(t, pgErr.ConstraintName, "Constraint name should be set")
				t.Logf("Constraint violation: Code=%s Schema=%s Table=%s Constraint=%s",
					pgErr.Code, pgErr.SchemaName, pgErr.TableName, pgErr.ConstraintName)
			})

			t.Run("check_constraint_violation", func(t *testing.T) {
				// Try to insert with invalid value (check constraint violation)
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, email, value) VALUES (3, 'other@example.com', -5)", tableName))
				require.Error(t, err)

				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

				// 23514 = check_violation
				assert.Equal(t, "ERROR", pgErr.Severity)
				assert.Equal(t, "23514", pgErr.Code, "SQLSTATE should be 23514 for check violation")
				assert.Equal(t, "public", pgErr.SchemaName, "Schema should be 'public'")
				assert.Equal(t, tableName, pgErr.TableName, "Table name should match")
				assert.NotEmpty(t, pgErr.ConstraintName, "Constraint name should be set")
				t.Logf("Check constraint violation: Code=%s Schema=%s Table=%s Constraint=%s",
					pgErr.Code, pgErr.SchemaName, pgErr.TableName, pgErr.ConstraintName)
			})

			t.Run("primary_key_violation", func(t *testing.T) {
				// Try to insert duplicate primary key
				_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, email, value) VALUES (1, 'another@example.com', 30)", tableName))
				require.Error(t, err)

				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

				// 23505 = unique_violation (primary key is implemented as unique constraint)
				assert.Equal(t, "ERROR", pgErr.Severity)
				assert.Equal(t, "23505", pgErr.Code)
				assert.Equal(t, "public", pgErr.SchemaName)
				assert.Equal(t, tableName, pgErr.TableName)
			})
		})
	}
}

// TestErrorFormat_PLpgSQLWhereField tests that PL/pgSQL errors include the Where field
// showing the call stack.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_PLpgSQLWhereField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			// Create nested functions that raise an error
			innerFunc := fmt.Sprintf("error_inner_%d", time.Now().UnixNano())
			outerFunc := fmt.Sprintf("error_outer_%d", time.Now().UnixNano())

			_, err = conn.Exec(ctx, fmt.Sprintf(`
				CREATE OR REPLACE FUNCTION %s() RETURNS void AS $$
				BEGIN
					RAISE EXCEPTION 'error from inner function';
				END;
				$$ LANGUAGE plpgsql;
			`, innerFunc))
			require.NoError(t, err)
			defer func() {
				_, _ = conn.Exec(context.Background(), "DROP FUNCTION IF EXISTS "+innerFunc+"()")
			}()

			_, err = conn.Exec(ctx, fmt.Sprintf(`
				CREATE OR REPLACE FUNCTION %s() RETURNS void AS $$
				BEGIN
					PERFORM %s();
				END;
				$$ LANGUAGE plpgsql;
			`, outerFunc, innerFunc))
			require.NoError(t, err)
			defer func() {
				_, _ = conn.Exec(context.Background(), "DROP FUNCTION IF EXISTS "+outerFunc+"()")
			}()

			// Call the outer function which triggers error in inner function
			_, err = conn.Exec(ctx, fmt.Sprintf("SELECT %s()", outerFunc))
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

			// P0001 = raise_exception
			assert.Equal(t, "ERROR", pgErr.Severity)
			assert.Equal(t, "P0001", pgErr.Code, "SQLSTATE should be P0001 for RAISE EXCEPTION")
			assert.Contains(t, pgErr.Message, "error from inner function")

			// The Where field should contain the PL/pgSQL call stack
			assert.NotEmpty(t, pgErr.Where, "Where field should contain call stack")
			assert.Contains(t, pgErr.Where, innerFunc, "Where should contain inner function name")
			assert.Contains(t, pgErr.Where, outerFunc, "Where should contain outer function name")
			t.Logf("PL/pgSQL error: Code=%s Message=%s\nWhere:\n%s",
				pgErr.Code, pgErr.Message, pgErr.Where)
		})
	}
}

// TestErrorFormat_HintAndDetail tests that errors preserve Hint and Detail fields.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_HintAndDetail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			// Use DO block to raise an error with DETAIL and HINT
			_, err = conn.Exec(ctx, `
				DO $$
				BEGIN
					RAISE EXCEPTION 'custom error message'
						USING DETAIL = 'This is the detailed explanation',
						      HINT = 'Try doing something else';
				END
				$$
			`)
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

			assert.Equal(t, "ERROR", pgErr.Severity)
			assert.Equal(t, "P0001", pgErr.Code) // raise_exception
			assert.Equal(t, "custom error message", pgErr.Message)
			assert.Equal(t, "This is the detailed explanation", pgErr.Detail)
			assert.Equal(t, "Try doing something else", pgErr.Hint)
			t.Logf("Error with Hint/Detail: Message=%s, Detail=%s, Hint=%s",
				pgErr.Message, pgErr.Detail, pgErr.Hint)
		})
	}
}

// TestErrorFormat_UndefinedTable tests that undefined object errors work correctly.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestErrorFormat_UndefinedTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping error format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping error format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")
			conn, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, "SELECT * FROM nonexistent_table_xyz")
			require.Error(t, err)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "expected pgconn.PgError, got %T", err)

			// 42P01 = undefined_table
			assert.Equal(t, "ERROR", pgErr.Severity)
			assert.Equal(t, "42P01", pgErr.Code, "SQLSTATE should be 42P01 for undefined table")
			assert.Contains(t, pgErr.Message, "nonexistent_table_xyz")
			t.Logf("Undefined table error: Code=%s Message=%s", pgErr.Code, pgErr.Message)
		})
	}
}
