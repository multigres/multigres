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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// noticeCollector collects notices received via pgx's OnNotice callback.
type noticeCollector struct {
	mu      sync.Mutex
	notices []*pgconn.Notice
}

func (nc *noticeCollector) handler(_ *pgconn.PgConn, n *pgconn.Notice) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.notices = append(nc.notices, n)
}

func (nc *noticeCollector) collect() []*pgconn.Notice {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	out := make([]*pgconn.Notice, len(nc.notices))
	copy(out, nc.notices)
	return out
}

func (nc *noticeCollector) reset() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.notices = nil
}

// connectWithNotices creates a pgx connection with an OnNotice handler that collects notices.
func connectWithNotices(ctx context.Context, t *testing.T, host string, port int, password string) (*pgx.Conn, *noticeCollector) {
	t.Helper()
	connStr := fmt.Sprintf("host=%s port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		host, port, password)
	config, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)

	collector := &noticeCollector{}
	config.OnNotice = collector.handler

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	return conn, collector
}

// TestNoticeFormat_TableInheritanceColumnMerge tests that PostgreSQL NOTICE messages from
// table inheritance column merging are forwarded through multigateway to the client.
func TestNoticeFormat_TableInheritanceColumnMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping notice format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping notice format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	t.Run("simple_query_inheritance_notice", func(t *testing.T) {
		conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer conn.Close(ctx)

		parentTable := fmt.Sprintf("parent_notice_%d", time.Now().UnixNano())
		childTable := fmt.Sprintf("child_notice_%d", time.Now().UnixNano())

		// Create parent table
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int, name text)", parentTable))
		require.NoError(t, err)
		defer func() {
			_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+childTable)
			_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+parentTable)
		}()

		collector.reset()

		// Create child table with same column name - PostgreSQL merges columns and emits a NOTICE:
		// NOTICE: merging column "name" with inherited definition
		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (name text) INHERITS (%s)", childTable, parentTable))
		require.NoError(t, err)

		notices := collector.collect()
		require.NotEmpty(t, notices, "expected NOTICE from table inheritance column merge")

		notice := notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Equal(t, "00000", notice.Code)
		assert.Contains(t, notice.Message, "merging column")
		assert.Contains(t, notice.Message, "name")
		t.Logf("Received NOTICE: Severity=%s Code=%s Message=%s", notice.Severity, notice.Code, notice.Message)
	})

	t.Run("compare_with_direct_postgres", func(t *testing.T) {
		directConn, directCollector := connectWithNotices(ctx, t, "localhost", setup.GetPrimary(t).Pgctld.PgPort, shardsetup.TestPostgresPassword)
		defer directConn.Close(ctx)

		mgConn, mgCollector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer mgConn.Close(ctx)

		suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

		// Setup on both connections
		for name, conn := range map[string]*pgx.Conn{"direct": directConn, "multigateway": mgConn} {
			_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE parent_cmp_%s_%s (id int, name text)", name, suffix))
			require.NoError(t, err)
		}
		defer func() {
			for _, name := range []string{"direct", "multigateway"} {
				_, _ = directConn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS child_cmp_%s_%s", name, suffix))
				_, _ = directConn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS parent_cmp_%s_%s", name, suffix))
				_, _ = mgConn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS child_cmp_%s_%s", name, suffix))
				_, _ = mgConn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS parent_cmp_%s_%s", name, suffix))
			}
		}()

		directCollector.reset()
		mgCollector.reset()

		// Trigger notice on both
		_, err := directConn.Exec(ctx, fmt.Sprintf("CREATE TABLE child_cmp_direct_%s (name text) INHERITS (parent_cmp_direct_%s)", suffix, suffix))
		require.NoError(t, err)
		_, err = mgConn.Exec(ctx, fmt.Sprintf("CREATE TABLE child_cmp_multigateway_%s (name text) INHERITS (parent_cmp_multigateway_%s)", suffix, suffix))
		require.NoError(t, err)

		directNotices := directCollector.collect()
		mgNotices := mgCollector.collect()

		require.NotEmpty(t, directNotices, "direct postgres should produce NOTICE")
		require.NotEmpty(t, mgNotices, "multigateway should forward NOTICE")

		assert.Equal(t, directNotices[0].Severity, mgNotices[0].Severity, "Severity should match")
		assert.Equal(t, directNotices[0].Code, mgNotices[0].Code, "Code should match")
		assert.Equal(t, directNotices[0].Message, mgNotices[0].Message, "Message should match")
	})
}

// TestNoticeFormat_RaiseNotice tests that RAISE NOTICE in PL/pgSQL DO blocks is forwarded
// through multigateway.
func TestNoticeFormat_RaiseNotice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping notice format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping notice format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	t.Run("single_raise_notice", func(t *testing.T) {
		conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "DO $$ BEGIN RAISE NOTICE 'test notice message'; END $$")
		require.NoError(t, err)

		notices := collector.collect()
		require.NotEmpty(t, notices, "expected NOTICE from RAISE NOTICE")

		notice := notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Equal(t, "00000", notice.Code)
		assert.Contains(t, notice.Message, "test notice message")
		t.Logf("Received NOTICE: Severity=%s Code=%s Message=%s", notice.Severity, notice.Code, notice.Message)
	})

	t.Run("multiple_notices", func(t *testing.T) {
		conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, `DO $$
		BEGIN
			RAISE NOTICE 'first notice';
			RAISE NOTICE 'second notice';
			RAISE NOTICE 'third notice';
		END
		$$`)
		require.NoError(t, err)

		notices := collector.collect()
		require.Len(t, notices, 3, "expected 3 NOTICEs from DO block")

		assert.Contains(t, notices[0].Message, "first notice")
		assert.Contains(t, notices[1].Message, "second notice")
		assert.Contains(t, notices[2].Message, "third notice")
	})

	t.Run("no_notice_on_simple_select", func(t *testing.T) {
		conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, "SELECT 1")
		require.NoError(t, err)

		notices := collector.collect()
		assert.Empty(t, notices, "expected no NOTICE from simple SELECT")
	})

	t.Run("compare_with_direct_postgres", func(t *testing.T) {
		directConn, directCollector := connectWithNotices(ctx, t, "localhost", setup.GetPrimary(t).Pgctld.PgPort, shardsetup.TestPostgresPassword)
		defer directConn.Close(ctx)

		mgConn, mgCollector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer mgConn.Close(ctx)

		query := "DO $$ BEGIN RAISE NOTICE 'comparison notice'; END $$"

		_, err := directConn.Exec(ctx, query)
		require.NoError(t, err)
		_, err = mgConn.Exec(ctx, query)
		require.NoError(t, err)

		directNotices := directCollector.collect()
		mgNotices := mgCollector.collect()

		require.NotEmpty(t, directNotices, "direct postgres should produce NOTICE")
		require.NotEmpty(t, mgNotices, "multigateway should forward NOTICE")

		assert.Equal(t, directNotices[0].Severity, mgNotices[0].Severity, "Severity should match")
		assert.Equal(t, directNotices[0].Code, mgNotices[0].Code, "Code should match")
		assert.Equal(t, directNotices[0].Message, mgNotices[0].Message, "Message should match")
	})
}

// TestNoticeFormat_DetailAndHint tests that NOTICE messages with DETAIL and HINT fields
// are forwarded through multigateway with all fields preserved.
func TestNoticeFormat_DetailAndHint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping notice format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping notice format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	t.Run("notice_with_detail_and_hint", func(t *testing.T) {
		conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer conn.Close(ctx)

		_, err := conn.Exec(ctx, `DO $$
		BEGIN
			RAISE NOTICE 'main message'
				USING DETAIL = 'detailed info',
				      HINT = 'helpful hint';
		END
		$$`)
		require.NoError(t, err)

		notices := collector.collect()
		require.NotEmpty(t, notices, "expected NOTICE with DETAIL and HINT")

		notice := notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Contains(t, notice.Message, "main message")
		assert.Equal(t, "detailed info", notice.Detail)
		assert.Equal(t, "helpful hint", notice.Hint)
		t.Logf("Received NOTICE: Severity=%s Code=%s Message=%s Detail=%s Hint=%s",
			notice.Severity, notice.Code, notice.Message, notice.Detail, notice.Hint)
	})

	t.Run("compare_with_direct_postgres", func(t *testing.T) {
		directConn, directCollector := connectWithNotices(ctx, t, "localhost", setup.GetPrimary(t).Pgctld.PgPort, shardsetup.TestPostgresPassword)
		defer directConn.Close(ctx)

		mgConn, mgCollector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		defer mgConn.Close(ctx)

		query := `DO $$
		BEGIN
			RAISE NOTICE 'test notice'
				USING DETAIL = 'test detail',
				      HINT = 'test hint';
		END
		$$`

		_, err := directConn.Exec(ctx, query)
		require.NoError(t, err)
		_, err = mgConn.Exec(ctx, query)
		require.NoError(t, err)

		directNotices := directCollector.collect()
		mgNotices := mgCollector.collect()

		require.NotEmpty(t, directNotices, "direct postgres should produce NOTICE")
		require.NotEmpty(t, mgNotices, "multigateway should forward NOTICE")

		assert.Equal(t, directNotices[0].Severity, mgNotices[0].Severity, "Severity should match")
		assert.Equal(t, directNotices[0].Code, mgNotices[0].Code, "Code should match")
		assert.Equal(t, directNotices[0].Message, mgNotices[0].Message, "Message should match")
		assert.Equal(t, directNotices[0].Detail, mgNotices[0].Detail, "Detail should match")
		assert.Equal(t, directNotices[0].Hint, mgNotices[0].Hint, "Hint should match")
	})
}

// TestNoticeFormat_PLpgSQLWhereField tests that NOTICE messages from nested PL/pgSQL functions
// include the Where field with the call stack, forwarded through multigateway.
func TestNoticeFormat_PLpgSQLWhereField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping notice format test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping notice format tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	conn, collector := connectWithNotices(ctx, t, "localhost", setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	defer conn.Close(ctx)

	innerFunc := fmt.Sprintf("notice_inner_%d", time.Now().UnixNano())
	outerFunc := fmt.Sprintf("notice_outer_%d", time.Now().UnixNano())

	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s() RETURNS void AS $$
		BEGIN
			RAISE NOTICE 'notice from inner function';
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

	t.Run("notice_with_where_field", func(t *testing.T) {
		collector.reset()

		_, err := conn.Exec(ctx, fmt.Sprintf("SELECT %s()", outerFunc))
		require.NoError(t, err)

		notices := collector.collect()
		require.NotEmpty(t, notices, "expected NOTICE from nested function call")

		notice := notices[0]
		assert.Equal(t, "NOTICE", notice.Severity)
		assert.Contains(t, notice.Message, "notice from inner function")
		assert.NotEmpty(t, notice.Where, "expected Where field to contain call stack")
		assert.Contains(t, notice.Where, innerFunc, "Where should contain inner function name")
		assert.Contains(t, notice.Where, outerFunc, "Where should contain outer function name")
		t.Logf("Received NOTICE with Where field:\n  Message: %s\n  Where: %s", notice.Message, notice.Where)
	})

	t.Run("compare_with_direct_postgres", func(t *testing.T) {
		directConn, directCollector := connectWithNotices(ctx, t, "localhost", setup.GetPrimary(t).Pgctld.PgPort, shardsetup.TestPostgresPassword)
		defer directConn.Close(ctx)

		// Create the same functions on direct connection
		_, err := directConn.Exec(ctx, fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s() RETURNS void AS $$
			BEGIN
				RAISE NOTICE 'notice from inner function';
			END;
			$$ LANGUAGE plpgsql;
		`, innerFunc))
		require.NoError(t, err)

		_, err = directConn.Exec(ctx, fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %s() RETURNS void AS $$
			BEGIN
				PERFORM %s();
			END;
			$$ LANGUAGE plpgsql;
		`, outerFunc, innerFunc))
		require.NoError(t, err)

		directCollector.reset()
		collector.reset()

		query := fmt.Sprintf("SELECT %s()", outerFunc)
		_, err = directConn.Exec(ctx, query)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, query)
		require.NoError(t, err)

		directNotices := directCollector.collect()
		mgNotices := collector.collect()

		require.NotEmpty(t, directNotices, "direct postgres should produce NOTICE")
		require.NotEmpty(t, mgNotices, "multigateway should forward NOTICE")

		assert.Equal(t, directNotices[0].Severity, mgNotices[0].Severity, "Severity should match")
		assert.Equal(t, directNotices[0].Code, mgNotices[0].Code, "Code should match")
		assert.Equal(t, directNotices[0].Message, mgNotices[0].Message, "Message should match")
		assert.Equal(t, directNotices[0].Where, mgNotices[0].Where, "Where (call stack) should match")
	})
}
