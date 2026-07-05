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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
)

func TestBackupTracker_SetLeaseHeld(t *testing.T) {
	tr := NewHealthTracker()
	assert.False(t, tr.Snapshot().LeaseHeld)

	tr.SetLeaseHeld(true)
	assert.True(t, tr.Snapshot().LeaseHeld)

	tr.SetLeaseHeld(false)
	assert.False(t, tr.Snapshot().LeaseHeld)
}

func TestBackupTracker_FailuresAndInProgress(t *testing.T) {
	tr := NewHealthTracker()

	tr.BackupStarted()
	assert.False(t, tr.Snapshot().InProgressStart.IsZero(), "in-progress should be set after start")

	tr.BackupFailed(errors.New("boom"))
	s := tr.Snapshot()
	assert.Equal(t, int64(1), s.FailuresSinceSuccess)
	assert.True(t, s.InProgressStart.IsZero(), "in-progress cleared on failure")
	assert.Equal(t, "boom", s.LastFailErr)
	assert.False(t, s.LastFailAt.IsZero())

	tr.BackupStarted()
	tr.BackupFailed(errors.New("boom2"))
	assert.Equal(t, int64(2), tr.Snapshot().FailuresSinceSuccess, "streak accumulates")

	tr.BackupStarted()
	tr.BackupSucceeded()
	s = tr.Snapshot()
	assert.Zero(t, s.FailuresSinceSuccess, "streak resets on success")
	assert.True(t, s.InProgressStart.IsZero(), "in-progress cleared on success")
}

func TestResolveReadiness(t *testing.T) {
	tests := []struct {
		name             string
		reachable        bool
		repoReason       string
		configReason     string
		archivingFailing bool
		want             string
	}{
		{name: "all good", reachable: true, want: ReadyReasonOK},
		{name: "repo unreachable dominates", reachable: false, repoReason: ReadyReasonRepoUnreachable, configReason: ReadyReasonArchiveCommandUnset, want: ReadyReasonRepoUnreachable},
		{name: "stanza missing", reachable: false, repoReason: ReadyReasonStanzaMissing, want: ReadyReasonStanzaMissing},
		{name: "config dominates archiving", reachable: true, configReason: ReadyReasonArchiveModeOff, archivingFailing: true, want: ReadyReasonArchiveModeOff},
		{name: "restore_command unset config reason", reachable: true, configReason: ReadyReasonRestoreCommandUnset, want: ReadyReasonRestoreCommandUnset},
		{name: "archiving failing", reachable: true, archivingFailing: true, want: ReadyReasonArchivingFailing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveReadiness(tt.reachable, tt.repoReason, tt.configReason, tt.archivingFailing))
		})
	}
}

func TestClassifyBackupSettings(t *testing.T) {
	tests := []struct {
		name     string
		settings PGSettings
		pgMode   pgmode.Mode
		want     string
	}{
		{
			name:     "primary fully configured",
			settings: PGSettings{ArchiveCommand: "pgbackrest archive-push %p", ArchiveMode: "on", RestoreCommand: "pgbackrest archive-get %f %p"},
			pgMode:   pgmode.Primary,
			want:     "",
		},
		{
			name:     "primary archive_command unset",
			settings: PGSettings{ArchiveCommand: "  ", ArchiveMode: "on", RestoreCommand: "x"},
			pgMode:   pgmode.Primary,
			want:     ReadyReasonArchiveCommandUnset,
		},
		{
			name:     "primary archive_mode off",
			settings: PGSettings{ArchiveCommand: "x", ArchiveMode: "off", RestoreCommand: "x"},
			pgMode:   pgmode.Primary,
			want:     ReadyReasonArchiveModeOff,
		},
		{
			name:     "archive_mode always is enabled",
			settings: PGSettings{ArchiveCommand: "x", ArchiveMode: "always", RestoreCommand: "x"},
			pgMode:   pgmode.Primary,
			want:     "",
		},
		{
			name:     "restore_command unset flagged on both roles",
			settings: PGSettings{ArchiveCommand: "x", ArchiveMode: "on", RestoreCommand: ""},
			pgMode:   pgmode.Primary,
			want:     ReadyReasonRestoreCommandUnset,
		},
		{
			name:     "standby ignores archive settings",
			settings: PGSettings{ArchiveCommand: "", ArchiveMode: "off", RestoreCommand: "x"},
			pgMode:   pgmode.InRecovery,
			want:     "",
		},
		{
			name:     "standby still needs restore_command",
			settings: PGSettings{ArchiveCommand: "", ArchiveMode: "off", RestoreCommand: ""},
			pgMode:   pgmode.InRecovery,
			want:     ReadyReasonRestoreCommandUnset,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyBackupSettings(tt.settings, tt.pgMode))
		})
	}
}

func TestRefreshHealth_DisabledWhenNoBackupConfig(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "") // no backup location → config not set
	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonDisabled, snap.Reason)
}

func TestRefreshHealth_OKWhenRepoReachableAndConfigured(t *testing.T) {
	stubPgbackrest(t, pgbackrestInfoStub(`[{"backup":[]}]`))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Primary, nil })
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{ArchiveCommand: "x", ArchiveMode: "on", RestoreCommand: "x"}, nil
	})

	assert.True(t, e.Health().Snapshot().LastRefresh.IsZero(), "no refresh time before the first poll")
	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.True(t, snap.Ready)
	assert.Equal(t, ReadyReasonOK, snap.Reason)
	assert.False(t, snap.LastRefresh.IsZero(), "refreshHealth should record the refresh time")
}

func TestRefreshHealth_StanzaMissing(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\necho \"ERROR: [055]: stanza 'multigres' does not exist\" >&2\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonStanzaMissing, snap.Reason)
}

func TestRefreshHealth_ArchiveCommandUnset(t *testing.T) {
	stubPgbackrest(t, pgbackrestInfoStub(`[{"backup":[]}]`))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Primary, nil })
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{ArchiveCommand: "", ArchiveMode: "on", RestoreCommand: "x"}, nil
	})

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonArchiveCommandUnset, snap.Reason)
}

func TestRefreshHealth_ArchiveModeOff(t *testing.T) {
	stubPgbackrest(t, pgbackrestInfoStub(`[{"backup":[]}]`))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Primary, nil })
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{ArchiveCommand: "x", ArchiveMode: "off", RestoreCommand: "x"}, nil
	})

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonArchiveModeOff, snap.Reason)
}

func TestRefreshHealth_RestoreCommandUnset(t *testing.T) {
	stubPgbackrest(t, pgbackrestInfoStub(`[{"backup":[]}]`))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Primary, nil })
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{ArchiveCommand: "x", ArchiveMode: "on", RestoreCommand: ""}, nil
	})

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonRestoreCommandUnset, snap.Reason)
}

func TestRefreshHealth_ArchivingFailing(t *testing.T) {
	stubPgbackrest(t, pgbackrestInfoStub(`[{"backup":[]}]`))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Primary, nil })
	// Config is healthy, so archiving failure is the dominant reason.
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{ArchiveCommand: "x", ArchiveMode: "on", RestoreCommand: "x"}, nil
	})
	e.SetArchiverStatsProvider(func(context.Context) (ArchiverStats, error) {
		return ArchiverStats{
			LastArchived: time.Unix(1735984000, 0),
			LastFailed:   time.Unix(1735984900, 0), // newer failure than last success
			FailedCount:  2,
		}, nil
	})

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonArchivingFailing, snap.Reason)
}

func TestRefreshHealth_RepoUnreachable(t *testing.T) {
	// info fails with a non-stanza-missing error → the whole poll lands on
	// repo_unreachable.
	stubPgbackrest(t, "#!/bin/bash\necho \"ERROR: [049]: unable to connect to repository host\" >&2\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	e.refreshHealth(t.Context())
	snap := e.Health().Snapshot()
	assert.False(t, snap.Ready)
	assert.Equal(t, ReadyReasonRepoUnreachable, snap.Reason)
}

func TestBackupHealthSnapshot_Defaults(t *testing.T) {
	tr := NewHealthTracker()
	snap := tr.Snapshot()

	assert.Equal(t, ReadyReasonUnknown, snap.Reason, "reason should default to unknown before first poll")
	assert.False(t, snap.Ready)
	assert.True(t, snap.LastSuccessStop.IsZero())
	assert.Zero(t, snap.CompleteCount)
	assert.True(t, snap.LastArchived.IsZero())
	assert.Zero(t, snap.FailuresSinceSuccess)
	assert.True(t, snap.InProgressStart.IsZero())
	assert.False(t, snap.LeaseHeld)
	assert.Empty(t, snap.LastFailErr)
}

func TestRefreshFromRepo(t *testing.T) {
	// Two COMPLETE backups + one errored; newest stop is 1735984860.
	json := `[{"backup":[
		{"label":"20250104-100000F","type":"full","timestamp":{"start":1735984000,"stop":1735984060},"annotation":{"table_group":"tg1","shard":"0","job_id":"j1"}},
		{"label":"20250104-110000F","type":"full","timestamp":{"start":1735984800,"stop":1735984860},"annotation":{"table_group":"tg1","shard":"0","job_id":"j2"}},
		{"label":"20250104-120000F","type":"full","error":true,"timestamp":{"start":1735985000,"stop":1735985060},"annotation":{"table_group":"tg1","shard":"0","job_id":"j3"}}
	]}]`
	stubPgbackrest(t, pgbackrestInfoStub(json))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	e.refreshFromRepo(t.Context())
	snap := e.Health().Snapshot()
	assert.Equal(t, int64(2), snap.CompleteCount, "errored backup should not be counted")
	assert.Equal(t, int64(1735984860), snap.LastSuccessStop.Unix(), "should be the newest COMPLETE stop time")
}

func TestRefreshFromRepo_RetainsValuesOnError(t *testing.T) {
	// Prime the tracker with values, then point at a config-less engine so
	// ListBackups errors; the cached values must be retained.
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	// No SetConfigPath → ListBackups returns an error.
	primed := time.Unix(1735984860, 0)
	e.Health().applyRepoInfo(primed, 5)

	e.refreshFromRepo(t.Context())
	snap := e.Health().Snapshot()
	assert.Equal(t, int64(5), snap.CompleteCount, "values should be retained on error")
	assert.Equal(t, primed.Unix(), snap.LastSuccessStop.Unix())
}

func TestRunHealthPoller_StopsOnContextCancel(t *testing.T) {
	json := `[{"backup":[]}]`
	stubPgbackrest(t, pgbackrestInfoStub(json))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		e.RunHealthPoller(ctx, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "poller did not stop after context cancel")
	}
}

func TestRefreshArchiver_ComputesLagOnPrimary(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	archived := time.Unix(1735984900, 0)
	e.SetArchiverStatsProvider(func(context.Context) (ArchiverStats, error) {
		return ArchiverStats{LastArchived: archived}, nil
	})

	failing := e.refreshArchiver(t.Context(), pgmode.Primary)
	assert.False(t, failing)
	snap := e.Health().Snapshot()
	assert.Equal(t, archived, snap.LastArchived)
}

func TestRefreshArchiver_SkipsOnStandby(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	// Prime a stale value to ensure standby clears it.
	e.Health().applyArchiver(time.Unix(1735984900, 0))
	called := false
	e.SetArchiverStatsProvider(func(context.Context) (ArchiverStats, error) {
		called = true
		return ArchiverStats{}, nil
	})

	failing := e.refreshArchiver(t.Context(), pgmode.InRecovery)
	assert.False(t, failing)
	snap := e.Health().Snapshot()
	assert.True(t, snap.LastArchived.IsZero(), "standby should report no archive lag")
	assert.False(t, called, "archiver stats should not be queried on a standby")
}

func TestRefreshArchiver_ReportsFailingWhenLastFailedNewer(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	archived := time.Unix(1735984000, 0)
	failed := time.Unix(1735984900, 0) // newer failure than last success
	e.SetArchiverStatsProvider(func(context.Context) (ArchiverStats, error) {
		return ArchiverStats{LastArchived: archived, LastFailed: failed, FailedCount: 3}, nil
	})

	failing := e.refreshArchiver(t.Context(), pgmode.Primary)
	assert.True(t, failing, "a failure newer than the last success means archiving is failing")
	snap := e.Health().Snapshot()
	assert.Equal(t, archived, snap.LastArchived)
}

func TestBackupHealthSnapshot_ReflectsState(t *testing.T) {
	tr := NewHealthTracker()
	stop := time.Unix(1735984800, 0)
	archived := time.Unix(1735984900, 0)
	inProgress := time.Unix(1735985000, 0)
	failAt := time.Unix(1735984860, 0)

	tr.mu.Lock()
	tr.lastSuccessStop = stop
	tr.completeCount = 3
	tr.ready = true
	tr.reason = ReadyReasonOK
	tr.lastArchived = archived
	tr.failuresSinceSuccess = 2
	tr.inProgressStart = inProgress
	tr.leaseHeld = true
	tr.lastFailErr = "boom"
	tr.lastFailAt = failAt
	tr.mu.Unlock()

	snap := tr.Snapshot()

	assert.Equal(t, stop, snap.LastSuccessStop)
	assert.Equal(t, int64(3), snap.CompleteCount)
	assert.True(t, snap.Ready)
	assert.Equal(t, ReadyReasonOK, snap.Reason)
	assert.Equal(t, archived, snap.LastArchived)
	assert.Equal(t, int64(2), snap.FailuresSinceSuccess)
	assert.Equal(t, inProgress, snap.InProgressStart)
	assert.True(t, snap.LeaseHeld)
	assert.Equal(t, "boom", snap.LastFailErr)
	assert.Equal(t, failAt, snap.LastFailAt)
}

func TestRefreshFromRepo_RepoUnreachableRetainsCache(t *testing.T) {
	// `pgbackrest info` fails with a non-stanza-missing error → repo_unreachable,
	// and the previously cached age/count must be retained (not thrashed to 0).
	stubPgbackrest(t, "#!/bin/bash\necho \"ERROR: [049]: unable to connect to repository host\" >&2\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	primed := time.Unix(1735984860, 0)
	e.Health().applyRepoInfo(primed, 7)

	reachable, reason := e.refreshFromRepo(t.Context())
	assert.False(t, reachable)
	assert.Equal(t, ReadyReasonRepoUnreachable, reason)
	snap := e.Health().Snapshot()
	assert.Equal(t, int64(7), snap.CompleteCount, "cached count retained on repo error")
	assert.Equal(t, primed.Unix(), snap.LastSuccessStop.Unix())
}

func TestRefreshFromRepo_MalformedJSONRetainsCache(t *testing.T) {
	// info returns non-JSON (exit 0) → decode fails → repo_unreachable, retained.
	stubPgbackrest(t, "#!/bin/bash\nif [[ \"$*\" == *info* ]]; then echo \"not json\"; exit 0; fi\nexit 0\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "tg1", "0", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	primed := time.Unix(1735984860, 0)
	e.Health().applyRepoInfo(primed, 7)

	reachable, reason := e.refreshFromRepo(t.Context())
	assert.False(t, reachable)
	assert.Equal(t, ReadyReasonRepoUnreachable, reason)
	assert.Equal(t, int64(7), e.Health().Snapshot().CompleteCount, "cached count retained on parse error")
}

func TestResolveRole(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	assert.Equal(t, pgmode.Primary, e.resolveRole(t.Context()), "nil provider defaults to primary")

	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.Unknown, errors.New("boom") })
	assert.Equal(t, pgmode.Primary, e.resolveRole(t.Context()), "role error must assume primary, not suppress archiving issues")

	e.SetRoleProvider(func(context.Context) (pgmode.Mode, error) { return pgmode.InRecovery, nil })
	assert.Equal(t, pgmode.InRecovery, e.resolveRole(t.Context()))
}

func TestCheckBackupSettings_DoesNotBlockOnNilOrError(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	// nil provider → cannot check → don't block readiness.
	assert.Equal(t, "", e.checkBackupSettings(t.Context(), pgmode.Primary))

	// provider error → don't block readiness.
	e.SetPGSettingsProvider(func(context.Context) (PGSettings, error) {
		return PGSettings{}, errors.New("query failed")
	})
	assert.Equal(t, "", e.checkBackupSettings(t.Context(), pgmode.Primary))
}

func TestRefreshArchiver_QueryErrorKeepsCache(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	primed := time.Unix(1735984900, 0)
	e.Health().applyArchiver(primed)
	e.SetArchiverStatsProvider(func(context.Context) (ArchiverStats, error) {
		return ArchiverStats{}, errors.New("query failed")
	})

	failing := e.refreshArchiver(t.Context(), pgmode.Primary)
	assert.False(t, failing, "a query error is not an archiving failure")
	assert.Equal(t, primed.Unix(), e.Health().Snapshot().LastArchived.Unix(), "cached lag retained on query error")
}

func TestRefreshArchiver_NilProvider(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "tg1", "0", "/tmp/backups")
	assert.False(t, e.refreshArchiver(t.Context(), pgmode.Primary), "no provider → cannot be failing")
}
