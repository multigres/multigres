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

package manager

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/tools/pgutil"
)

const heartbeatWritePattern = "(?s)INSERT INTO multigres\\.heartbeat.*"

func newHeartbeatProofManager(t *testing.T, queryService *mock.QueryService) *MultipoolerManager {
	t.Helper()
	pm := newGracefulShutdownTestManager(t, nil)
	pm.replTracker = heartbeat.NewReplTracker(queryService, pm.logger, []byte("0"), "pooler-1", 250)
	return pm
}

func TestWriteHeartbeatForAckedProposal(t *testing.T) {
	startedAt := time.Now()
	lsn, err := pgutil.ParseLSN("0/100")
	require.NoError(t, err)

	t.Run("records the proof and writes the heartbeat row", func(t *testing.T) {
		queryService := mock.NewQueryService()
		writes := 0
		queryService.AddQueryPatternWithCallback(heartbeatWritePattern, mock.MakeQueryResult(
			[]string{"pg_current_wal_lsn"}, [][]any{{"0/900"}},
		), func(string) { writes++ })
		pm := newHeartbeatProofManager(t, queryService)

		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, startedAt)

		got, ts, have := pm.replTracker.HeartbeatWriter().LastProven()
		require.True(t, have)
		assert.Equal(t, "0/100", got.String())
		assert.Equal(t, startedAt, ts)
		assert.Equal(t, 1, writes)
	})

	t.Run("a failed write still records the proof and does not fail the caller", func(t *testing.T) {
		queryService := mock.NewQueryService()
		queryService.AddQueryPatternWithError(heartbeatWritePattern, errors.New("boom"))
		pm := newHeartbeatProofManager(t, queryService)

		pm.writeHeartbeatForAckedProposal(t.Context(), lsn, startedAt)

		got, _, have := pm.replTracker.HeartbeatWriter().LastProven()
		require.True(t, have)
		assert.Equal(t, "0/100", got.String())
	})

	t.Run("no heartbeat tracker is a no-op", func(t *testing.T) {
		pm := newGracefulShutdownTestManager(t, nil)
		pm.replTracker = nil
		assert.NotPanics(t, func() { pm.writeHeartbeatForAckedProposal(t.Context(), lsn, startedAt) })
	})
}
