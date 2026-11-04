// Copyright 2026 Supabase, Inc.
// SPDX-License-Identifier: Apache-2.0

package executor

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/tools/latencydiag"
)

type diagnosticWaitingPool struct {
	stubPoolManager
	waited  time.Duration
	failure error
}

func (p *diagnosticWaitingPool) GetRegularConnWithSettings(ctx context.Context, _ map[string]string, _ string, _, _ []byte) (regular.PooledConn, error) {
	before := time.Now()
	<-ctx.Done()
	p.waited = time.Since(before)
	p.failure = ctx.Err()
	return nil, p.failure
}

func TestDiagnosticPoolWaitAttribution(t *testing.T) {
	capture, err := latencydiag.Begin(latencydiag.Options{ID: "pool", SampleEvery: 1, MaxEvents: 100})
	require.NoError(t, err)
	defer capture.Stop()
	root := latencydiag.Message(t.Context(), 1, 1)
	ctx, cancel := context.WithTimeout(root.Context(), 20*time.Millisecond)
	defer cancel()
	pool := &diagnosticWaitingPool{}
	executor := NewExecutor(slog.New(slog.DiscardHandler), pool, &clustermetadata.ID{}, false)
	_, _, err = executor.ExecuteQuery(ctx, nil, "SELECT 1", nil)
	require.Error(t, err)
	require.True(t, errors.Is(pool.failure, context.DeadlineExceeded))
	root.End(err, 0)
	report := capture.Stop()
	require.Len(t, report.Events, 2)
	wait := report.Events[0]
	require.Equal(t, "pool.acquire", wait.Phase)
	require.GreaterOrEqual(t, wait.DurationNS, pool.waited.Nanoseconds())
	require.True(t, wait.Failed)
	require.Equal(t, report.Events[1].ID, wait.Parent)
	require.Zero(t, report.Unfinished)
}
