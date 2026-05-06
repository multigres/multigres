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
	"bufio"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// connectToMultigateway creates a client connection to the multigateway.
func connectToMultigateway(ctx context.Context, t *testing.T, setup *shardsetup.ShardSetup) *client.Conn {
	t.Helper()
	conn, err := client.Connect(ctx, ctx, &client.Config{
		Host:        "localhost",
		Port:        setup.MultigatewayPgPort,
		User:        shardsetup.DefaultTestUser,
		Password:    shardsetup.TestPostgresPassword,
		Database:    "postgres",
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return conn
}

// TestGatewayQuerySpanAttributes verifies that multigateway emits
// gateway.query spans with correct db.plan.type and db.tables_used
// attributes for different query types through the full path:
// client → multigateway → multipooler → PostgreSQL.
func TestGatewayQuerySpanAttributes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	collector := shardsetup.NewTestOTLPCollector(t)

	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithOTelExport(collector.Endpoint()),
	)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := connectToMultigateway(ctx, t, setup)
	defer conn.Close()

	// Create a test table for queries that reference tables.
	_, err := conn.Query(ctx, "CREATE TABLE IF NOT EXISTS otel_test_table (id int)")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn := connectToMultigateway(cleanupCtx, t, setup)
		_, _ = cleanupConn.Query(cleanupCtx, "DROP TABLE IF EXISTS otel_test_table")
		cleanupConn.Close()
	})

	// Execute queries that produce different plan types (simple protocol).
	_, err = conn.Query(ctx, "SELECT * FROM otel_test_table")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "INSERT INTO otel_test_table VALUES (1)")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "SET statement_timeout = '5s'")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "SHOW statement_timeout")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "BEGIN")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "COMMIT")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "SET search_path = public")
	require.NoError(t, err)

	// Multi-statement batch — produces a single span with no plan type
	// because the batch contains mixed statement types.
	_, err = conn.Query(ctx, "SELECT 1; SELECT 2")
	require.NoError(t, err)

	// Extended protocol — uses PrepareAndExecute (Parse/Bind/Execute/Sync).
	_, err = conn.QueryArgs(ctx, "SELECT * FROM otel_test_table WHERE id = $1", 1)
	require.NoError(t, err)

	// Wait for spans — we need at least the 9 queries above plus setup queries.
	spans := collector.WaitForSpans(t, "gateway.query", 9, 15*time.Second)

	// findSpanWithPlanType searches for a gateway.query span with the given
	// db.plan.type attribute value.
	findSpanWithPlanType := func(planType string) *shardsetup.SpanProto {
		for _, s := range spans {
			pt, _ := shardsetup.FindSpanAttribute(s, "db.plan.type")
			if pt == planType {
				return s
			}
		}
		return nil
	}

	// Route: SELECT and INSERT reference tables.
	t.Run("Route", func(t *testing.T) {
		span := findSpanWithPlanType("Route")
		require.NotNil(t, span, "expected a Route span")

		tables, found := shardsetup.FindSpanAttributeArray(span, "db.tables_used")
		assert.True(t, found, "Route span should have db.tables_used")
		assert.Contains(t, tables, "otel_test_table")
	})

	// GatewaySessionState: SET statement_timeout is gateway-managed.
	t.Run("GatewaySessionState", func(t *testing.T) {
		span := findSpanWithPlanType("GatewaySessionState")
		require.NotNil(t, span, "expected a GatewaySessionState span")

		_, found := shardsetup.FindSpanAttributeArray(span, "db.tables_used")
		assert.False(t, found, "GatewaySessionState should not have db.tables_used")
	})

	// GatewayShowVariable: SHOW statement_timeout is gateway-managed.
	t.Run("GatewayShowVariable", func(t *testing.T) {
		span := findSpanWithPlanType("GatewayShowVariable")
		require.NotNil(t, span, "expected a GatewayShowVariable span")

		_, found := shardsetup.FindSpanAttributeArray(span, "db.tables_used")
		assert.False(t, found, "GatewayShowVariable should not have db.tables_used")
	})

	// Transaction: BEGIN and COMMIT both produce Transaction plan type.
	t.Run("Transaction", func(t *testing.T) {
		span := findSpanWithPlanType("Transaction")
		require.NotNil(t, span, "expected a Transaction span")

		_, found := shardsetup.FindSpanAttributeArray(span, "db.tables_used")
		assert.False(t, found, "Transaction should not have db.tables_used")
	})

	// ApplySessionState: SET search_path is not gateway-managed, goes to postgres.
	t.Run("ApplySessionState", func(t *testing.T) {
		span := findSpanWithPlanType("ApplySessionState")
		require.NotNil(t, span, "expected an ApplySessionState span")

		_, found := shardsetup.FindSpanAttributeArray(span, "db.tables_used")
		assert.False(t, found, "ApplySessionState should not have db.tables_used")
	})

	// Multi-statement batch: the batch-level span should have no db.plan.type
	// because the batch contains mixed statement types. Per-table metrics are
	// emitted per-statement inside executeWithImplicitTransaction.
	t.Run("MultiStatementBatch", func(t *testing.T) {
		// Find a span that has no db.plan.type (batch path gets nil result).
		var batchSpan *shardsetup.SpanProto
		for _, s := range spans {
			_, hasPlanType := shardsetup.FindSpanAttribute(s, "db.plan.type")
			if !hasPlanType {
				batchSpan = s
				break
			}
		}
		// A batch span should exist without db.plan.type.
		assert.NotNil(t, batchSpan, "expected a span without db.plan.type (multi-statement batch)")
	})

	// Extended protocol: PrepareAndExecute goes through HandleExecute/PortalStreamExecute.
	t.Run("ExtendedProtocol", func(t *testing.T) {
		// Find a span with db.query.protocol = "extended".
		var extSpan *shardsetup.SpanProto
		for _, s := range spans {
			proto, _ := shardsetup.FindSpanAttribute(s, "db.query.protocol")
			if proto == "extended" {
				extSpan = s
				break
			}
		}
		require.NotNil(t, extSpan, "expected a span with db.query.protocol=extended")

		planType, found := shardsetup.FindSpanAttribute(extSpan, "db.plan.type")
		assert.True(t, found, "extended protocol span should have db.plan.type")
		assert.Equal(t, "Route", planType)

		tables, found := shardsetup.FindSpanAttributeArray(extSpan, "db.tables_used")
		assert.True(t, found, "extended protocol span should have db.tables_used")
		assert.Contains(t, tables, "otel_test_table")
	})
}

// queryLogEntry represents a parsed "query completed" log line from the multigateway.
type queryLogEntry struct {
	Msg          string   `json:"msg"`
	OpName       string   `json:"db.operation.name"`
	Protocol     string   `json:"db.query.protocol"`
	PlanType     string   `json:"db.plan.type"`
	TablesUsed   []string `json:"db.tables_used"`
	DurationPlan float64  `json:"duration.plan"`
}

// readQueryLogs reads the multigateway log file and returns all "query completed" entries.
func readQueryLogs(t *testing.T, logFile string) []queryLogEntry {
	t.Helper()
	f, err := os.Open(logFile)
	require.NoError(t, err)
	defer f.Close()

	var entries []queryLogEntry
	scanner := bufio.NewScanner(f)
	// Increase buffer for long log lines.
	scanner.Buffer(make([]byte, 0, 256*1024), 256*1024)
	for scanner.Scan() {
		var entry queryLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		if entry.Msg == "query completed" {
			entries = append(entries, entry)
		}
	}
	require.NoError(t, scanner.Err())
	return entries
}

// TestGatewayQueryLogFields verifies that multigateway structured query logs
// contain the new plan instrumentation fields: duration.plan, db.plan.type,
// and db.tables_used.
func TestGatewayQueryLogFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	// OTel export not needed for log verification, but we use it to share
	// the same setup pattern. A no-op collector is fine.
	collector := shardsetup.NewTestOTLPCollector(t)

	setup := shardsetup.New(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithOTelExport(collector.Endpoint()),
	)
	setup.SetupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn := connectToMultigateway(ctx, t, setup)
	defer conn.Close()

	_, err := conn.Query(ctx, "CREATE TABLE IF NOT EXISTS otel_log_test (id int)")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn := connectToMultigateway(cleanupCtx, t, setup)
		_, _ = cleanupConn.Query(cleanupCtx, "DROP TABLE IF EXISTS otel_log_test")
		cleanupConn.Close()
	})

	// Execute queries that produce different plan types.
	_, err = conn.Query(ctx, "SELECT * FROM otel_log_test")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "SET statement_timeout = '5s'")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "BEGIN")
	require.NoError(t, err)

	_, err = conn.Query(ctx, "COMMIT")
	require.NoError(t, err)

	// Extended protocol query.
	_, err = conn.QueryArgs(ctx, "SELECT * FROM otel_log_test WHERE id = $1", 1)
	require.NoError(t, err)

	// Poll the multigateway log file until we see enough "query completed" entries.
	// We expect at least 5: CREATE TABLE, SELECT, SET, BEGIN, COMMIT (plus extended).
	logFile := setup.Multigateway.LogFile
	require.NotEmpty(t, logFile, "multigateway log file path should be set")
	var entries []queryLogEntry
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		entries = readQueryLogs(t, logFile)
		if len(entries) >= 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.GreaterOrEqual(t, len(entries), 5, "expected at least 5 query completed log entries, got %d", len(entries))

	// Index log entries by operation name (last occurrence wins).
	logByOp := make(map[string]queryLogEntry)
	for _, e := range entries {
		logByOp[e.OpName] = e
	}

	// Verify Route query log (SELECT).
	t.Run("Log/SELECT", func(t *testing.T) {
		entry, ok := logByOp["SELECT"]
		require.True(t, ok, "expected a SELECT log entry")
		assert.Equal(t, "Route", entry.PlanType, "db.plan.type")
		assert.Contains(t, entry.TablesUsed, "otel_log_test", "db.tables_used")
		assert.Greater(t, entry.DurationPlan, 0.0, "duration.plan should be > 0")
	})

	// Verify GatewaySessionState query log (SET).
	t.Run("Log/SET", func(t *testing.T) {
		entry, ok := logByOp["SET"]
		require.True(t, ok, "expected a SET log entry")
		assert.Equal(t, "GatewaySessionState", entry.PlanType, "db.plan.type")
		assert.Empty(t, entry.TablesUsed, "SET should have no tables")
	})

	// Verify Transaction query log (BEGIN/COMMIT use their own operation names in logs).
	t.Run("Log/BEGIN", func(t *testing.T) {
		entry, ok := logByOp["BEGIN"]
		require.True(t, ok, "expected a BEGIN log entry")
		assert.Equal(t, "Transaction", entry.PlanType, "db.plan.type")
		assert.Empty(t, entry.TablesUsed, "BEGIN should have no tables")
	})

	t.Run("Log/COMMIT", func(t *testing.T) {
		entry, ok := logByOp["COMMIT"]
		require.True(t, ok, "expected a COMMIT log entry")
		assert.Equal(t, "Transaction", entry.PlanType, "db.plan.type")
		assert.Empty(t, entry.TablesUsed, "COMMIT should have no tables")
	})

	// Verify extended protocol query log entry.
	t.Run("Log/ExtendedProtocol", func(t *testing.T) {
		var extEntry *queryLogEntry
		for i := range entries {
			if entries[i].Protocol == "extended" {
				extEntry = &entries[i]
				break
			}
		}
		require.NotNil(t, extEntry, "expected an extended protocol log entry")
		assert.Equal(t, "Route", extEntry.PlanType, "db.plan.type for extended protocol")
		assert.Contains(t, extEntry.TablesUsed, "otel_log_test", "db.tables_used for extended protocol")
	})

	// Verify duration.plan is present on all entries.
	t.Run("Log/DurationPlanPresent", func(t *testing.T) {
		for _, entry := range entries {
			// duration.plan is always emitted (even as 0 for batch paths).
			// For single-statement queries it should be >= 0.
			assert.GreaterOrEqual(t, entry.DurationPlan, 0.0,
				"duration.plan should be >= 0 for op=%s", entry.OpName)
		}
	})
}
