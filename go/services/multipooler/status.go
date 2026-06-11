// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package multipooler provides multipooler functionality.
package multipooler

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/web"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

// Link represents a link on the status page.
type Link struct {
	Title       string
	Description string
	Link        string
}

// Status represents the response from the temporary status endpoint
type Status struct {
	mu sync.Mutex

	Title string `json:"title"`

	InitError  string            `json:"init_error"`
	TopoStatus map[string]string `json:"topo_status"`

	Cell           string `json:"cell"`
	ServiceID      string `json:"service_id"`
	Database       string `json:"database"`
	TableGroup     string `json:"table_group"`
	PgctldAddr     string `json:"pgctld_addr"`
	SocketFilePath string `json:"socket_file_path"`

	Backups BackupStatusView `json:"backups"`

	Links []Link `json:"links"`
}

// BackupStatusView is the formatted, template-ready view of the backup-health
// snapshot. Durations are pre-formatted here (not in the template).
type BackupStatusView struct {
	HasBackup     bool   `json:"has_backup"`
	LastBackupAt  string `json:"last_backup_at"`  // formatted; empty if none
	LastBackupAge string `json:"last_backup_age"` // e.g. "2h13m"; empty if none
	CompleteCount int64  `json:"complete_count"`
	FailuresSince int64  `json:"failures_since"`
	InProgress    bool   `json:"in_progress"`
	InProgressFor string `json:"in_progress_for"`
	Ready         bool   `json:"ready"`
	ReadyReason   string `json:"ready_reason"`
	WALArchiveLag string `json:"wal_archive_lag"` // empty when unknown / standby
	LeaseHeld     bool   `json:"lease_held"`
	LastFailure   string `json:"last_failure"` // err + timestamp; empty if none
	// LastRefreshed is when the poller last refreshed this snapshot, formatted
	// as "<timestamp> (<age> ago)"; empty before the first poll. Rendered as a
	// freshness note on the status page.
	LastRefreshed string `json:"last_refreshed"`
}

// formatAge renders the time elapsed since t, rounded to the second, or "" if t
// is the zero time.
func formatAge(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return time.Since(t).Round(time.Second).String()
}

// handleIndex serves the index page
func (mp *MultiPooler) handleIndex(w http.ResponseWriter, r *http.Request) {
	mp.serverStatus.mu.Lock()
	defer mp.serverStatus.mu.Unlock()

	mp.serverStatus.Cell = mp.cell.Get()
	mp.serverStatus.ServiceID = mp.serviceID.Get()
	mp.serverStatus.Database = mp.database.Get()
	mp.serverStatus.TableGroup = mp.tableGroup.Get()
	mp.serverStatus.PgctldAddr = mp.pgctldAddr.Get()
	mp.serverStatus.SocketFilePath = mp.socketFilePath.Get()
	mp.serverStatus.TopoStatus = mp.ts.Status()
	mp.serverStatus.Backups = mp.backupStatusView()
	err := web.Templates.ExecuteTemplate(w, "pooler_index.html", &mp.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// backupStatusView builds the template-ready backup view from the manager's
// health snapshot. During early startup the manager may not exist yet; in that
// case it returns an "unknown" view rather than panicking.
func (mp *MultiPooler) backupStatusView() BackupStatusView {
	if mp.poolerManager == nil {
		return BackupStatusView{ReadyReason: "unknown"}
	}
	return buildBackupStatusView(mp.poolerManager.BackupStatusSnapshot())
}

// buildBackupStatusView maps a backup-health snapshot into the template-ready
// view, pre-formatting durations/timestamps. Pure (no manager) so it is
// directly unit-testable.
func buildBackupStatusView(snap backupengine.Snapshot) BackupStatusView {
	view := BackupStatusView{
		HasBackup:     !snap.LastSuccessStop.IsZero(),
		CompleteCount: snap.CompleteCount,
		FailuresSince: snap.FailuresSinceSuccess,
		InProgress:    !snap.InProgressStart.IsZero(),
		InProgressFor: formatAge(snap.InProgressStart),
		Ready:         snap.Ready,
		ReadyReason:   snap.Reason,
		WALArchiveLag: formatAge(snap.LastArchived),
		LeaseHeld:     snap.LeaseHeld,
	}
	if view.HasBackup {
		view.LastBackupAt = snap.LastSuccessStop.Format(time.RFC3339)
		view.LastBackupAge = formatAge(snap.LastSuccessStop)
	}
	if snap.LastFailErr != "" {
		view.LastFailure = fmt.Sprintf("%s (%s)", snap.LastFailErr, snap.LastFailAt.Format(time.RFC3339))
	}
	if !snap.LastRefresh.IsZero() {
		view.LastRefreshed = fmt.Sprintf("%s (%s ago)", snap.LastRefresh.Format(time.RFC3339), formatAge(snap.LastRefresh))
	}
	return view
}
