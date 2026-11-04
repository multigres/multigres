// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package minigressetup

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

// Exercises actual protocol handlers, in-process gRPC, pool acquisition and
// PostgreSQL together. The captured query/parameter strings must never escape.
func TestMinigres_LatencyDiagnostics(t *testing.T) {
	skipIfShort(t)
	setup, cleanup := NewIsolated(t, WithExtraArgs("--latency-diagnostics"))
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("postgres", clustersetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var captureBody []byte
	var captureErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://localhost:%d/debug/latency?seconds=3&sample_every=1&trace=1&sockets=1&id=e2e", setup.Minigres.HttpPort), nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			captureErr = err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			captureErr = fmt.Errorf("capture HTTP %d", resp.StatusCode)
			return
		}
		captureBody, captureErr = io.ReadAll(resp.Body)
	}()
	// Repeat operations throughout the capture window: no assumption that the
	// collector goroutine has started before the first SQL request is sent.
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		var result string
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT 'diagnostic_sql_sentinel'").Scan(&result))
		exerciseDiagnosticPrepared(t, ctx, tx)
		require.NoError(t, tx.Commit())
		_, err = db.ExecContext(ctx, "SELECT pg_sleep(0.025)")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SELECT 1/0")
		require.Error(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	db.Close()
	<-done
	require.NoError(t, captureErr)
	if path := os.Getenv("MULTIGRES_LATENCY_TEST_ARTIFACT"); path != "" {
		require.NoError(t, os.WriteFile(path, captureBody, 0o600))
	}
	archive, err := zip.NewReader(bytes.NewReader(captureBody), int64(len(captureBody)))
	require.NoError(t, err)
	var report latencydiag.Report
	for _, file := range archive.File {
		reader, err := file.Open()
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		reader.Close()
		require.NoError(t, err)
		require.NotContains(t, string(data), "diagnostic_sql_sentinel")
		require.NotContains(t, string(data), "diagnostic_parameter_sentinel")
		if file.Name == "events.json" {
			require.NoError(t, json.Unmarshal(data, &report))
		}
	}
	require.NotEmpty(t, report.Events)
	require.Zero(t, report.Dropped)
	phases := map[string]bool{}
	var linked, failed, delayedResponses int
	for _, e := range report.Events {
		phases[e.Phase] = true
		if e.Phase == "postgres.response" && e.DurationNS >= int64(20*time.Millisecond) {
			delayedResponses++
		}
		if e.RemoteParent != "" {
			linked++
		}
		if e.Failed {
			failed++
		}
	}
	t.Logf("captured phases: %v; linked RPCs: %d", phases, linked)
	for _, phase := range []string{"frontend.read_type", "frontend.read_length", "frontend.read_body", "frontend.dispatch", "frontend.encode_result", "frontend.flush_release", "frontend.transport_read", "frontend.transport_write", "postgres.response", "postgres.result_callback", "pool.acquire", "transaction.commit"} {
		require.True(t, phases[phase], "missing phase %s", phase)
	}
	require.Positive(t, delayedResponses, "injected PostgreSQL sleeps must appear in backend response timing")
	require.Positive(t, linked, "gateway and pooler must have correlated RPCs")
	require.Positive(t, failed, "division-by-zero errors must appear without query text")
	t.Logf("validated %d events, %d correlated RPCs, %d failed spans, %d unfinished at window boundary", len(report.Events), linked, failed, report.Unfinished)
}

func exerciseDiagnosticPrepared(t *testing.T, ctx context.Context, tx *sql.Tx) {
	t.Helper()
	stmt, err := tx.PrepareContext(ctx, "SELECT $1::text")
	require.NoError(t, err)
	defer func() { require.NoError(t, stmt.Close()) }()
	var result string
	require.NoError(t, stmt.QueryRowContext(ctx, "diagnostic_parameter_sentinel").Scan(&result))
	require.Equal(t, "diagnostic_parameter_sentinel", result)
}
