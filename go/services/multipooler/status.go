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
	"github.com/multigres/multigres/go/services/multipooler/internal/replicationstats"
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

	Backups          BackupStatusView     `json:"backups"`
	ReplicationStats ReplicationStatsView `json:"replication_stats"`

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

// ReplicationStatsView is the formatted, template-ready view of the
// replicationstats poller's health and latest polled connections. This is
// the status-page surface for the guidelines' "who am I connected to, and
// what are the stats for each" — it does not depend on a Prometheus scrape,
// so it stays useful when debugging a pooler whose /metrics endpoint isn't
// reachable.
type ReplicationStatsView struct {
	// Open is true while this pooler is the writable leader and the poller
	// is actively running. Always false on a standby.
	Open       bool  `json:"open"`
	Polls      int64 `json:"polls"`
	PollErrors int64 `json:"poll_errors"`

	Connections []ReplicationConnView `json:"connections"`
}

// ReplicationConnView is the formatted view of one polled logical-replication
// connection.
type ReplicationConnView struct {
	ConnID      string `json:"conn_id"`
	User        string `json:"user"`
	ReplayLag   string `json:"replay_lag"`   // formatted duration; "—" if unset
	LastAckAge  string `json:"last_ack_age"` // formatted duration; "—" if unset
	LastMsgAge  string `json:"last_msg_age"` // formatted duration; "—" if unset
	SlotName    string `json:"slot_name"`    // "—" if no matching slot
	RetainedWAL string `json:"retained_wal"` // bytes; "—" if no matching slot
}

// unknownValue is rendered for a value that wasn't available this poll
// tick (e.g. no ack yet, no matching slot) — distinct from a real zero
// duration.
const unknownValue = "—"

// formatSeconds renders secs as a duration string, or unknownValue if have
// is false.
func formatSeconds(secs float64, have bool) string {
	if !have {
		return unknownValue
	}
	return time.Duration(secs * float64(time.Second)).Round(time.Millisecond).String()
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
func (mp *Multipooler) handleIndex(w http.ResponseWriter, r *http.Request) {
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
	mp.serverStatus.ReplicationStats = mp.replicationStatsView()
	err := web.Templates.ExecuteTemplate(w, "pooler_index.html", &mp.serverStatus)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to execute template: %v", err), http.StatusInternalServerError)
		return
	}
}

// backupStatusView builds the template-ready backup view from the manager's
// health snapshot. During early startup the manager may not exist yet; in that
// case it returns an "unknown" view rather than panicking.
func (mp *Multipooler) backupStatusView() BackupStatusView {
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

// replicationStatsView builds the template-ready replicationstats view from
// the manager's poller status. During early startup the manager may not
// exist yet; in that case it returns the zero view (closed, no connections)
// rather than panicking — the same fallback the manager's own accessor uses
// when the poller hasn't started (see MultipoolerManager.ReplicationStatsStatus).
func (mp *Multipooler) replicationStatsView() ReplicationStatsView {
	if mp.poolerManager == nil {
		return ReplicationStatsView{}
	}
	return buildReplicationStatsView(mp.poolerManager.ReplicationStatsStatus())
}

// buildReplicationStatsView maps a poller status into the template-ready
// view, pre-formatting durations. Pure (no manager) so it is directly
// unit-testable.
func buildReplicationStatsView(status replicationstats.PollerStatus) ReplicationStatsView {
	view := ReplicationStatsView{
		Open:        status.Open,
		Polls:       status.Polls,
		PollErrors:  status.PollErrors,
		Connections: make([]ReplicationConnView, 0, len(status.Connections)),
	}
	for _, c := range status.Connections {
		connView := ReplicationConnView{
			ConnID:     c.ConnID,
			User:       c.User,
			ReplayLag:  formatSeconds(c.ReplayLag, c.HaveReplayLag),
			LastAckAge: formatSeconds(c.LastAckAge, c.HaveAck),
			LastMsgAge: formatSeconds(c.LastMsgAge, c.HaveMsgAge),
		}
		if c.HaveSlot {
			connView.SlotName = c.SlotName
			connView.RetainedWAL = fmt.Sprintf("%d bytes", c.RetainedWAL)
		} else {
			connView.SlotName = unknownValue
			connView.RetainedWAL = unknownValue
		}
		view.Connections = append(view.Connections, connView)
	}
	return view
}
