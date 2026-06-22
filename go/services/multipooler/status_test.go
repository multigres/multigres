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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/web"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

func renderPoolerIndex(t *testing.T, status *Status) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, web.Templates.ExecuteTemplate(&buf, "pooler_index.html", status))
	return buf.String()
}

func TestBuildBackupStatusView_WithBackup(t *testing.T) {
	stop := time.Now().Add(-2 * time.Hour)
	failAt := time.Now().Add(-3 * time.Hour)
	snap := backupengine.Snapshot{
		LastSuccessStop:      stop,
		CompleteCount:        5,
		Ready:                true,
		Reason:               backupengine.ReadyReasonOK,
		LastArchived:         time.Now().Add(-15 * time.Second),
		FailuresSinceSuccess: 2,
		InProgressStart:      time.Now().Add(-30 * time.Second),
		LeaseHeld:            true,
		LastFailErr:          "boom",
		LastFailAt:           failAt,
		LastRefresh:          time.Now().Add(-10 * time.Second),
	}

	view := buildBackupStatusView(snap)
	assert.True(t, view.HasBackup)
	assert.Equal(t, stop.Format(time.RFC3339), view.LastBackupAt)
	assert.NotEmpty(t, view.LastBackupAge)
	assert.Equal(t, int64(5), view.CompleteCount)
	assert.Equal(t, int64(2), view.FailuresSince)
	assert.True(t, view.Ready)
	assert.Equal(t, "ok", view.ReadyReason)
	assert.NotEmpty(t, view.WALArchiveLag, "WAL lag should render when last-archived is set")
	assert.True(t, view.InProgress)
	assert.NotEmpty(t, view.InProgressFor)
	assert.True(t, view.LeaseHeld)
	assert.Contains(t, view.LastFailure, "boom")
	assert.Contains(t, view.LastFailure, failAt.Format(time.RFC3339))
	assert.Contains(t, view.LastRefreshed, "ago", "should render the last-refreshed time with an age")
}

func TestBuildBackupStatusView_NoBackup(t *testing.T) {
	// Zero snapshot: no backup, nothing in progress, no lag, no failure.
	view := buildBackupStatusView(backupengine.Snapshot{Reason: backupengine.ReadyReasonStanzaMissing})
	assert.False(t, view.HasBackup)
	assert.Empty(t, view.LastRefreshed, "no refresh time before the first poll")
	assert.Empty(t, view.LastBackupAt)
	assert.Empty(t, view.LastBackupAge)
	assert.False(t, view.InProgress)
	assert.Empty(t, view.InProgressFor)
	assert.Empty(t, view.WALArchiveLag, "no WAL lag when last-archived is zero (standby/unknown)")
	assert.Empty(t, view.LastFailure)
	assert.Equal(t, "stanza_missing", view.ReadyReason)
}

func TestBackupStatusView_NilManagerDuringStartup(t *testing.T) {
	// Before the pooler manager is constructed (early startup), the view must
	// render an "unknown" state rather than panicking on a nil manager.
	mp := &MultiPooler{}
	view := mp.backupStatusView()
	assert.Equal(t, "unknown", view.ReadyReason)
	assert.False(t, view.HasBackup)
	assert.False(t, view.Ready)
}

func TestPoolerIndex_BackupsSection_NoBackup(t *testing.T) {
	status := &Status{
		Title: "pooler",
		Backups: BackupStatusView{
			HasBackup:   false,
			Ready:       false,
			ReadyReason: "stanza_missing",
		},
	}
	html := renderPoolerIndex(t, status)

	assert.Contains(t, html, "<h4>Backups</h4>")
	assert.Contains(t, html, "never", "should show 'never' when there is no backup")
	assert.Contains(t, html, "stanza_missing", "should show the not-ready reason")
}

func TestPoolerIndex_BackupsSection_WithBackup(t *testing.T) {
	status := &Status{
		Title: "pooler",
		Backups: BackupStatusView{
			HasBackup:     true,
			LastBackupAt:  "2026-06-10T12:00:00Z",
			LastBackupAge: "2h13m0s",
			CompleteCount: 4,
			FailuresSince: 1,
			Ready:         true,
			ReadyReason:   "ok",
			WALArchiveLag: "15s",
			LeaseHeld:     true,
			LastFailure:   "boom (2026-06-10T11:00:00Z)",
			LastRefreshed: "2026-06-10T14:00:00Z (12s ago)",
		},
	}
	html := renderPoolerIndex(t, status)

	assert.Contains(t, html, "2026-06-10T12:00:00Z")
	assert.Contains(t, html, "2h13m0s ago")
	assert.Contains(t, html, "15s", "should show WAL archive lag")
	assert.Contains(t, html, "boom (2026-06-10T11:00:00Z)", "should show last failure")
	assert.NotContains(t, html, "never")
	assert.Contains(t, html, "Last refreshed 2026-06-10T14:00:00Z (12s ago)", "should show the last-refreshed note")
}
