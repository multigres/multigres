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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

// managerWithMockQuery builds a minimal manager whose pm.query routes to the
// given mock query service — enough to unit-test the backup-health query
// helpers (archiverStats / backupSettings) without standing up a full manager.
func managerWithMockQuery(qs *mock.QueryService) *MultipoolerManager {
	return &MultipoolerManager{qsc: &mockPoolerController{queryService: qs}}
}

func TestArchiverStats(t *testing.T) {
	t.Run("maps epoch seconds to time and count", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern("pg_stat_archiver", mock.MakeQueryResult(
			[]string{"last_archived", "last_failed", "failed_count"},
			[][]any{{int64(1735984800), int64(1735984900), int64(3)}}))

		stats, err := managerWithMockQuery(qs).archiverStats(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(1735984800), stats.LastArchived.Unix())
		assert.Equal(t, int64(1735984900), stats.LastFailed.Unix())
		assert.Equal(t, int64(3), stats.FailedCount)
	})

	t.Run("zero (NULL→COALESCE 0) maps to zero time", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern("pg_stat_archiver", mock.MakeQueryResult(
			[]string{"last_archived", "last_failed", "failed_count"},
			[][]any{{int64(0), int64(0), int64(0)}}))

		stats, err := managerWithMockQuery(qs).archiverStats(t.Context())
		require.NoError(t, err)
		assert.True(t, stats.LastArchived.IsZero(), "epoch 0 must map to zero time, not 1970")
		assert.True(t, stats.LastFailed.IsZero())
		assert.Zero(t, stats.FailedCount)
	})

	t.Run("query error is wrapped", func(t *testing.T) {
		qs := mock.NewQueryService() // no pattern registered → query errors
		_, err := managerWithMockQuery(qs).archiverStats(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pg_stat_archiver")
	})

	t.Run("scan error on unexpected row shape", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern("pg_stat_archiver", mock.MakeQueryResult(
			[]string{"only_one"}, [][]any{{int64(1)}})) // 1 column, scanner needs 3
		_, err := managerWithMockQuery(qs).archiverStats(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scan")
	})
}

func TestBackupSettings(t *testing.T) {
	t.Run("maps the three settings", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern("current_setting", mock.MakeQueryResult(
			[]string{"archive_command", "archive_mode", "restore_command", "server_version"},
			[][]any{{"pgbackrest archive-push %p", "on", "pgbackrest archive-get %f %p", "16.2"}}))

		s, err := managerWithMockQuery(qs).backupSettings(t.Context())
		require.NoError(t, err)
		assert.Equal(t, "pgbackrest archive-push %p", s.ArchiveCommand)
		assert.Equal(t, "on", s.ArchiveMode)
		assert.Equal(t, "pgbackrest archive-get %f %p", s.RestoreCommand)
		assert.Equal(t, "16.2", s.ServerVersion)
	})

	t.Run("empty settings pass through", func(t *testing.T) {
		qs := mock.NewQueryService()
		qs.AddQueryPattern("current_setting", mock.MakeQueryResult(
			[]string{"archive_command", "archive_mode", "restore_command", "server_version"},
			[][]any{{"", "off", "", ""}}))

		s, err := managerWithMockQuery(qs).backupSettings(t.Context())
		require.NoError(t, err)
		assert.Empty(t, s.ArchiveCommand)
		assert.Equal(t, "off", s.ArchiveMode)
		assert.Empty(t, s.RestoreCommand)
		assert.Empty(t, s.ServerVersion)
	})

	t.Run("query error is wrapped", func(t *testing.T) {
		qs := mock.NewQueryService()
		_, err := managerWithMockQuery(qs).backupSettings(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backup settings")
	})
}
