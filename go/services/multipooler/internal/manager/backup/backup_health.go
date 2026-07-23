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

package backup

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

// Bounded readiness reasons reported by the readiness gauge and status page.
// Keeping this set small keeps metric cardinality bounded.
const (
	// ReadyReasonUnknown is the reason before the first poll completes.
	ReadyReasonUnknown = "unknown"
	// ReadyReasonOK means backups can run: repo reachable, stanza present,
	// archiving configured and not failing, restore configured.
	ReadyReasonOK = "ok"
	// ReadyReasonStanzaMissing means the stanza has not been created yet.
	ReadyReasonStanzaMissing = "stanza_missing"
	// ReadyReasonRepoUnreachable means the repo could not be reached.
	ReadyReasonRepoUnreachable = "repo_unreachable"
	// ReadyReasonArchivingFailing means WAL archiving is failing.
	ReadyReasonArchivingFailing = "archiving_failing"
	// ReadyReasonArchiveCommandUnset means archive_command is empty (primary).
	ReadyReasonArchiveCommandUnset = "archive_command_unset"
	// ReadyReasonArchiveModeOff means archive_mode is not on/always (primary).
	ReadyReasonArchiveModeOff = "archive_mode_off"
	// ReadyReasonRestoreCommandUnset means restore_command is empty.
	ReadyReasonRestoreCommandUnset = "restore_command_unset"
	// ReadyReasonDisabled means backup config has not been loaded.
	ReadyReasonDisabled = "disabled"
)

// HealthTracker holds the latest backup-health snapshot, refreshed by the
// poller and read by OTel gauge callbacks + the status page. All fields are
// guarded by mu; the poller does its pgbackrest/SQL I/O OUTSIDE the lock and
// only takes mu to copy results in, so callbacks never block on I/O.
//
// A single mutex (rather than per-field atomics, à la pgctld) is used
// deliberately: this is a cold path, and one lock gives consistent group
// snapshots (independent atomics can be read torn — e.g. completeCount updated
// but lastSuccessStop not yet) and naturally holds the string fail fields.
type HealthTracker struct {
	mu sync.Mutex

	// Repo-derived (set by the poller from `pgbackrest info`).
	lastSuccessStop time.Time // stop time of newest COMPLETE backup; zero if none
	completeCount   int64

	// Readiness, derived passively each poll from `pgbackrest info`
	// (repo/stanza) + pg_settings (archive/restore config). No active probe.
	ready  bool
	reason string // one of the ReadyReason* constants

	// WAL archive lag (primary only; set from pg_stat_archiver).
	lastArchived time.Time // zero if unknown / not primary

	// Updated inline by the Backup() RPC.
	failuresSinceSuccess int64
	inProgressStart      time.Time // zero when no backup running
	leaseHeld            bool

	// Last failure detail for the status page.
	lastFailErr string
	lastFailAt  time.Time

	// lastRefresh is when the poller last completed a refresh cycle (the
	// freshness of everything above). Zero before the first poll.
	lastRefresh time.Time
}

// NewHealthTracker constructs a tracker with sane pre-poll defaults.
func NewHealthTracker() *HealthTracker {
	return &HealthTracker{reason: ReadyReasonUnknown}
}

// Snapshot is a plain-value, lock-free view of the tracker state for the gauge
// callback and the status page. Ages are derived from the timestamps at read
// time so they stay accurate between polls.
type Snapshot struct {
	LastSuccessStop      time.Time // zero if no COMPLETE backup
	CompleteCount        int64
	Ready                bool
	Reason               string
	LastArchived         time.Time // zero if unknown / not primary
	FailuresSinceSuccess int64
	InProgressStart      time.Time // zero when no backup running
	LeaseHeld            bool
	LastFailErr          string
	LastFailAt           time.Time
	LastRefresh          time.Time // when the poller last refreshed; zero before first poll
}

// Snapshot reads all tracker state into a plain value under a single lock — one
// consistent read for both the gauge callback and the status page.
func (t *HealthTracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	reason := t.reason
	if reason == "" {
		reason = ReadyReasonUnknown
	}
	return Snapshot{
		LastSuccessStop:      t.lastSuccessStop,
		CompleteCount:        t.completeCount,
		Ready:                t.ready,
		Reason:               reason,
		LastArchived:         t.lastArchived,
		FailuresSinceSuccess: t.failuresSinceSuccess,
		InProgressStart:      t.inProgressStart,
		LeaseHeld:            t.leaseHeld,
		LastFailErr:          t.lastFailErr,
		LastFailAt:           t.lastFailAt,
		LastRefresh:          t.lastRefresh,
	}
}

// applyRepoInfo stores the repo-derived state computed by the poller.
func (t *HealthTracker) applyRepoInfo(lastSuccessStop time.Time, completeCount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSuccessStop = lastSuccessStop
	t.completeCount = completeCount
}

// applyReadiness stores the readiness state computed by the poller.
func (t *HealthTracker) applyReadiness(ready bool, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ready = ready
	t.reason = reason
}

// applyArchiver stores the WAL archive state computed by the poller.
func (t *HealthTracker) applyArchiver(lastArchived time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastArchived = lastArchived
}

// SetLeaseHeld records whether this pooler currently holds the backup lease.
func (t *HealthTracker) SetLeaseHeld(held bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.leaseHeld = held
}

// markRefreshed records that the poller just completed a refresh cycle.
func (t *HealthTracker) markRefreshed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastRefresh = time.Now()
}

// BackupStarted records that a backup is now in progress. Called inline by the
// Backup() RPC alongside the attempts counter.
func (t *HealthTracker) BackupStarted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inProgressStart = time.Now()
}

// BackupSucceeded records a successful backup: clears the in-progress timer and
// resets the failure streak.
func (t *HealthTracker) BackupSucceeded() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inProgressStart = time.Time{}
	t.failuresSinceSuccess = 0
}

// BackupFailed records a failed backup: clears the in-progress timer, increments
// the failure streak, and records the failure detail for the status page.
func (t *HealthTracker) BackupFailed(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inProgressStart = time.Time{}
	t.failuresSinceSuccess++
	if err != nil {
		t.lastFailErr = err.Error()
		t.lastFailAt = time.Now()
	}
}

// RunHealthPoller refreshes the backup-health snapshot on a ticker until ctx is
// cancelled. It is meant to be launched as a goroutine by the manager; it
// returns when ctx is done, so cancelling the manager context stops the poller
// (no leak). An initial refresh runs immediately so the snapshot is populated
// without waiting a full interval.
func (e *Engine) RunHealthPoller(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = constants.BackupHealthPollInterval
	}
	e.refreshHealth(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.refreshHealth(ctx)
		}
	}
}

// RefreshHealthNow performs a full one-off backup-health refresh, including the
// readiness checks that issue pg queries (role, pg_settings, pg_stat_archiver).
// The manager calls it from the service layer once bootstrap completes so the
// gauges/status page reflect post-bootstrap state without waiting a poll tick.
//
// It is deliberately NOT used in the backup path — see RefreshRepoNow.
func (e *Engine) RefreshHealthNow(ctx context.Context) {
	e.refreshHealth(ctx)
}

// RefreshRepoNow refreshes only the repo-derived gauges (last-backup age +
// complete count) from `pgbackrest info`. The manager calls it right after a
// backup completes so the just-created backup is reflected immediately on the
// pooler that took it, rather than waiting for the next poll tick.
//
// It is intentionally kept separate from (and narrower than) RefreshHealthNow —
// it must NOT issue the readiness pg queries — for two reasons:
//
//  1. It runs inside the Backup() path's defer while the action lock AND the
//     distributed backup lease are still held; limiting it to a single
//     `pgbackrest info` keeps that critical section short.
//  2. The cluster's first backup is created through this same path
//     (backupLocked), which in unit tests runs against a strict mock query
//     service with one-shot expectations. Issuing pg_is_in_recovery /
//     pg_settings / pg_stat_archiver here would race with and consume those
//     mocks, breaking unrelated consensus tests.
//
// A backup changes only repo state (a new backup), not role/config/archiver
// readiness, so skipping the readiness refresh here loses nothing — the next
// poll re-derives it.
func (e *Engine) RefreshRepoNow(ctx context.Context) {
	e.refreshFromRepo(ctx)
	e.health.markRefreshed()
}

// refreshHealth recomputes the whole health snapshot for one tick. It derives
// readiness passively — `pgbackrest info` (repo/stanza, which it also uses for
// the age/count gauges) plus cheap pg_settings reads — so it never runs an
// active probe like `pgbackrest check` that would force WAL/checkpoint I/O on
// the primary.
func (e *Engine) refreshHealth(ctx context.Context) {
	// Record refresh completion on every poll, whatever path it takes.
	defer e.health.markRefreshed()

	// Disabled: backup config not loaded from topology yet.
	if _, err := e.requireBackupConfig(); err != nil {
		e.health.applyReadiness(false, ReadyReasonDisabled)
		return
	}

	// pgMode is the physical recovery mode: it gates the archive_command /
	// archive_mode readiness checks and the WAL-archive-lag gauge, since only a
	// primary (out of recovery) archives WAL.
	//
	// TODO: consider whether these checks should instead key off the routing role
	// (writability). Physical mode is correct today — a deposed primary still out
	// of recovery keeps archiving until it is demoted — but revisit if backup
	// readiness should track routing writability instead.
	pgMode := e.resolveRole(ctx)
	reachable, repoReason := e.refreshFromRepo(ctx)
	configReason := e.checkBackupSettings(ctx, pgMode)
	archivingFailing := e.refreshArchiver(ctx, pgMode)

	reason := resolveReadiness(reachable, repoReason, configReason, archivingFailing)
	e.health.applyReadiness(reason == ReadyReasonOK, reason)
}

// resolveRole returns the local postgres recovery mode, defaulting to primary
// when the role is unknown so a real archiving issue on an actual primary is
// never suppressed.
func (e *Engine) resolveRole(ctx context.Context) pgmode.Mode {
	fn := e.roleProvider()
	if fn == nil {
		return pgmode.Primary
	}
	mode, err := fn(ctx)
	if err != nil {
		e.logger.DebugContext(ctx, "backup health: role check failed; assuming primary", "error", err)
		return pgmode.Primary
	}
	return mode
}

// refreshFromRepo runs `pgbackrest info` once: it updates the age/count gauges
// and returns the repo reachability classification for readiness. On a
// transient repo failure it leaves the cached age/count untouched (so the
// gauges don't thrash to zero) and reports repo_unreachable.
func (e *Engine) refreshFromRepo(ctx context.Context) (reachable bool, reason string) {
	configPath, err := e.requireConfigPath()
	if err != nil {
		// pgbackrest.conf not generated yet (topology not loaded).
		e.logger.DebugContext(ctx, "backup health: config path not set; repo unreachable", "error", err)
		return false, ReadyReasonRepoUnreachable
	}

	output, err := e.runInfoCmd(ctx, configPath)
	if err != nil {
		if isStanzaMissing(output) {
			// Stanza not created yet: there are genuinely no backups.
			e.health.applyRepoInfo(time.Time{}, 0)
			return false, ReadyReasonStanzaMissing
		}
		e.logger.DebugContext(ctx, "backup health: pgbackrest info failed; keeping cached age/count", "error", err)
		return false, ReadyReasonRepoUnreachable
	}

	infoData, err := decodeInfo(output)
	if err != nil {
		e.logger.DebugContext(ctx, "backup health: failed to parse pgbackrest info; keeping cached age/count", "error", err)
		return false, ReadyReasonRepoUnreachable
	}
	// `info` always exits 0 and always attaches the correct status code, even
	// when a repository can't be evaluated (e.g. a cipher-key mismatch) — but
	// getting here means the JSON above happened to decode (see decodeInfo for
	// when it doesn't). The failure still shows up as backup:[] with no decode
	// error, so it must not be read as "zero backups" — that would report
	// readiness as reachable for a repo we actually can't read.
	if err := repoStatusError(infoData); err != nil {
		e.logger.DebugContext(ctx, "backup health: repo status error; keeping cached age/count", "error", err)
		return false, ReadyReasonRepoUnreachable
	}

	var completeCount int64
	var lastSuccessStop time.Time
	for _, b := range e.parseBackups(ctx, infoData) {
		if b.GetStatus() != multipoolermanagerdata.BackupMetadata_COMPLETE {
			continue
		}
		completeCount++
		if ts := b.GetStopTimestamp(); ts != nil {
			if stop := ts.AsTime(); stop.After(lastSuccessStop) {
				lastSuccessStop = stop
			}
		}
	}
	e.health.applyRepoInfo(lastSuccessStop, completeCount)
	return true, ""
}

// checkBackupSettings reads the PostgreSQL settings relevant to backup
// readiness (cheap pg_settings reads, no forced I/O) and classifies them. It
// returns "" when the settings are fine or cannot be read (we don't block
// readiness on a transient query failure).
func (e *Engine) checkBackupSettings(ctx context.Context, pgMode pgmode.Mode) string {
	fn := e.settingsProvider()
	if fn == nil {
		return ""
	}
	settings, err := fn(ctx)
	if err != nil {
		e.logger.DebugContext(ctx, "backup health: settings query failed; skipping config readiness check", "error", err)
		return ""
	}
	return classifyBackupSettings(settings, pgMode)
}

// classifyBackupSettings maps PostgreSQL backup-related settings to a bounded
// readiness reason, or "" when they are fine. It is role-aware: archive_command
// and archive_mode are only meaningful on a primary (only it archives WAL),
// while restore_command is required on any node that may recover/restore.
func classifyBackupSettings(s PGSettings, pgMode pgmode.Mode) string {
	if pgMode.OutOfRecovery() {
		if strings.TrimSpace(s.ArchiveCommand) == "" {
			return ReadyReasonArchiveCommandUnset
		}
		if !archiveModeEnabled(s.ArchiveMode) {
			return ReadyReasonArchiveModeOff
		}
	}
	if strings.TrimSpace(s.RestoreCommand) == "" {
		return ReadyReasonRestoreCommandUnset
	}
	return ""
}

// archiveModeEnabled reports whether archive_mode permits archiving.
func archiveModeEnabled(mode string) bool {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case "on", "always":
		return true
	default:
		return false
	}
}

// resolveReadiness combines the repo, config, and archiving signals into a
// single bounded reason, worst-first: an unreachable repo or missing stanza
// dominates a config problem, which dominates an archiving failure.
func resolveReadiness(reachable bool, repoReason, configReason string, archivingFailing bool) string {
	if !reachable {
		return repoReason
	}
	if configReason != "" {
		return configReason
	}
	if archivingFailing {
		return ReadyReasonArchivingFailing
	}
	return ReadyReasonOK
}

// refreshArchiver refreshes the WAL archive lag gauge from pg_stat_archiver and
// reports whether archiving is currently failing (the most recent archive
// attempt failed). Only a primary archives WAL, so on a standby it clears the
// cached value (the gauge stops emitting) and reports not-failing. This is a
// passive, continuous signal — a cheap system-view read, no forced I/O.
func (e *Engine) refreshArchiver(ctx context.Context, pgMode pgmode.Mode) (archivingFailing bool) {
	if !pgMode.OutOfRecovery() {
		e.health.applyArchiver(time.Time{})
		return false
	}

	fn := e.archiverStatsProvider()
	if fn == nil {
		return false
	}
	stats, err := fn(ctx)
	if err != nil {
		e.logger.DebugContext(ctx, "backup health: pg_stat_archiver query failed; keeping cached lag", "error", err)
		return false
	}

	e.health.applyArchiver(stats.LastArchived)
	return stats.FailedCount > 0 && stats.LastFailed.After(stats.LastArchived)
}
