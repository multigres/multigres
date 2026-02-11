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

package multipooler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgProtocolClientNotices tests that PostgreSQL NOTICE messages are forwarded to the client.
// This addresses issue #573 where NOTICE messages (e.g., from table inheritance merging)
// were silently dropped instead of being forwarded to the client.
func TestPgProtocolClientNotices(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	// Test 1: Table inheritance with column merge produces NOTICE
	t.Run("table_inheritance_column_merge_notice", func(t *testing.T) {
		// Create parent table
		_, err := conn.Query(ctx, "CREATE TEMP TABLE parent_notice_test (id int, name text)")
		require.NoError(t, err)

		// Create child table with same column name - PostgreSQL merges the columns and emits a NOTICE
		// The NOTICE message is: "merging column "name" with inherited definition"
		results, err := conn.Query(ctx, "CREATE TEMP TABLE child_notice_test (name text) INHERITS (parent_notice_test)")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.NotEmpty(t, result.Notices, "expected NOTICE from table inheritance column merge")

		// Verify the notice has the expected fields
		notice := result.Notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Equal(t, "00000", notice.Code) // successful_completion SQLSTATE (informational notice)
		assert.Contains(t, notice.Message, "merging column")
		assert.Contains(t, notice.Message, "name")
		t.Logf("Received NOTICE: %s - %s", notice.Code, notice.Message)
	})

	// Test 2: Simple query that produces no NOTICE
	t.Run("no_notice_on_simple_select", func(t *testing.T) {
		results, err := conn.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		assert.Empty(t, result.Notices, "expected no NOTICE from simple SELECT")
	})

	// Test 3: RAISE NOTICE in DO block
	t.Run("raise_notice_in_do_block", func(t *testing.T) {
		results, err := conn.Query(ctx, "DO $$ BEGIN RAISE NOTICE 'test notice message'; END $$")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.NotEmpty(t, result.Notices, "expected NOTICE from RAISE NOTICE")

		notice := result.Notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Equal(t, "00000", notice.Code) // successful_completion SQLSTATE (default for RAISE NOTICE)
		assert.Contains(t, notice.Message, "test notice message")
		t.Logf("Received NOTICE: %s - %s", notice.Code, notice.Message)
	})

	// Test 4: Multiple NOTICEs in single statement
	t.Run("multiple_notices", func(t *testing.T) {
		results, err := conn.Query(ctx, `DO $$
		BEGIN
			RAISE NOTICE 'first notice';
			RAISE NOTICE 'second notice';
			RAISE NOTICE 'third notice';
		END
		$$`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.Len(t, result.Notices, 3, "expected 3 NOTICEs from DO block")

		assert.Contains(t, result.Notices[0].Message, "first notice")
		assert.Contains(t, result.Notices[1].Message, "second notice")
		assert.Contains(t, result.Notices[2].Message, "third notice")
	})

	// Test 5: NOTICE with DETAIL and HINT
	t.Run("notice_with_detail_and_hint", func(t *testing.T) {
		results, err := conn.Query(ctx, `DO $$
		BEGIN
			RAISE NOTICE 'main message'
				USING DETAIL = 'detailed info',
				      HINT = 'helpful hint';
		END
		$$`)
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.NotEmpty(t, result.Notices, "expected NOTICE with DETAIL and HINT")

		notice := result.Notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Contains(t, notice.Message, "main message")
		assert.Equal(t, "detailed info", notice.Detail)
		assert.Equal(t, "helpful hint", notice.Hint)
		t.Logf("Received NOTICE: %s - %s (Detail: %s, Hint: %s)",
			notice.Code, notice.Message, notice.Detail, notice.Hint)
	})

	// Test 6: Streaming query with NOTICEs
	t.Run("streaming_query_with_notices", func(t *testing.T) {
		var results []*sqltypes.Result
		var totalNotices int

		err := conn.QueryStreaming(ctx, `DO $$
		BEGIN
			RAISE NOTICE 'streaming notice';
		END
		$$`,
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				totalNotices += len(result.Notices)
				return nil
			})
		require.NoError(t, err)

		assert.Equal(t, 1, totalNotices, "expected 1 NOTICE from streaming query")
	})

	// Test 7: NOTICE with Where field showing PL/pgSQL call stack
	t.Run("notice_with_where_field_plpgsql_stack", func(t *testing.T) {
		// Create a PL/pgSQL function that calls another function which raises a NOTICE
		// This tests that the Where field contains the call stack information
		_, err := conn.Query(ctx, `
			CREATE OR REPLACE FUNCTION notice_test_inner() RETURNS void AS $$
			BEGIN
				RAISE NOTICE 'notice from inner function';
			END;
			$$ LANGUAGE plpgsql;
		`)
		require.NoError(t, err)

		_, err = conn.Query(ctx, `
			CREATE OR REPLACE FUNCTION notice_test_outer() RETURNS void AS $$
			BEGIN
				PERFORM notice_test_inner();
			END;
			$$ LANGUAGE plpgsql;
		`)
		require.NoError(t, err)

		// Call the outer function which triggers the notice in the inner function
		results, err := conn.Query(ctx, "SELECT notice_test_outer()")
		require.NoError(t, err)
		require.Len(t, results, 1)

		result := results[0]
		require.NotEmpty(t, result.Notices, "expected NOTICE from nested function call")

		notice := result.Notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Contains(t, notice.Message, "notice from inner function")

		// The Where field should contain the PL/pgSQL call stack
		// It will look something like:
		// "PL/pgSQL function notice_test_inner() line 3 at RAISE
		// PL/pgSQL function notice_test_outer() line 3 at PERFORM"
		assert.NotEmpty(t, notice.Where, "expected Where field to contain call stack")
		assert.Contains(t, notice.Where, "notice_test_inner", "Where should contain inner function name")
		assert.Contains(t, notice.Where, "notice_test_outer", "Where should contain outer function name")
		t.Logf("Received NOTICE with Where field:\n  Message: %s\n  Where: %s", notice.Message, notice.Where)

		// Cleanup
		_, err = conn.Query(ctx, "DROP FUNCTION IF EXISTS notice_test_outer()")
		require.NoError(t, err)
		_, err = conn.Query(ctx, "DROP FUNCTION IF EXISTS notice_test_inner()")
		require.NoError(t, err)
	})
}

// TestPgProtocolClientNoticesExtendedQuery tests that NOTICE messages are forwarded
// through the extended query protocol.
func TestPgProtocolClientNoticesExtendedQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.PrimaryPgctld.PgPort,
		User:        "postgres",
		Password:    testPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer conn.Close()

	// Test extended query protocol with PrepareAndExecute
	t.Run("prepare_and_execute_with_notice", func(t *testing.T) {
		var results []*sqltypes.Result

		// Use a DO block that raises a NOTICE
		err := conn.PrepareAndExecute(ctx, "", `DO $$
		BEGIN
			RAISE NOTICE 'prepared statement notice';
		END
		$$`,
			nil, // no parameters
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				return nil
			})
		require.NoError(t, err)
		require.NotEmpty(t, results)

		// Find the result with the notice
		var foundNotice bool
		for _, result := range results {
			if len(result.Notices) > 0 {
				foundNotice = true
				notice := result.Notices[0]
				assert.Equal(t, "NOTICE", notice.Severity)
				assert.Contains(t, notice.Message, "prepared statement notice")
				t.Logf("Received NOTICE via extended query: %s - %s", notice.Code, notice.Message)
				break
			}
		}
		assert.True(t, foundNotice, "expected NOTICE from extended query protocol")
	})

	// Test BindAndExecute with NOTICE
	t.Run("bind_and_execute_with_notice", func(t *testing.T) {
		// Parse a DO block that raises a NOTICE
		err := conn.Parse(ctx, "notice_stmt", `DO $$
		BEGIN
			RAISE NOTICE 'bind and execute notice';
		END
		$$`, nil)
		require.NoError(t, err)

		var results []*sqltypes.Result
		completed, err := conn.BindAndExecute(ctx, "notice_stmt", nil, nil, nil, 0,
			func(ctx context.Context, result *sqltypes.Result) error {
				results = append(results, result)
				return nil
			})
		require.NoError(t, err)
		assert.True(t, completed)

		// Find the result with the notice
		var foundNotice bool
		for _, result := range results {
			if len(result.Notices) > 0 {
				foundNotice = true
				notice := result.Notices[0]
				assert.Equal(t, "NOTICE", notice.Severity)
				assert.Contains(t, notice.Message, "bind and execute notice")
				break
			}
		}
		assert.True(t, foundNotice, "expected NOTICE from BindAndExecute")

		// Cleanup
		err = conn.CloseStatement(ctx, "notice_stmt")
		require.NoError(t, err)
	})
}
