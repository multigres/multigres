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

// Package backup is the multipooler's pgBackRest engine: it owns all pgBackRest
// interaction for a single pooler — backup, restore, expire, verify, list,
// check, and archive-command configuration. It is a leaf package: the manager
// orchestrates lifecycle and owns the gRPC handlers, delegating the pgBackRest
// steps here. Construct via NewEngine.
package backup

import (
	"context"
	"log/slog"
	"sync"
	"time"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multipooler/internal/pgmode"
	"github.com/multigres/multigres/go/tools/executil"
)

// stanzaName is the fixed pgBackRest stanza name used by multigres.
const stanzaName = "multigres"

// Identity exposes the pooler identity the backup engine needs.
type Identity interface {
	Id() *clustermetadatapb.ID
	ShardKey() *clustermetadatapb.ShardKey
}

// Settings are the static settings fixed at construction (from the pooler's
// static config / environment).
type Settings struct {
	// PgpassPath, when set, is exported as PGPASSFILE for pgbackrest commands.
	PgpassPath string
	// PgDataDir is the PostgreSQL data directory (for archive-command config in
	// postgresql.auto.conf).
	PgDataDir string
}

// RunFunc executes a (possibly long-running) command with progress logging. It
// is injected (the manager's runLongCommand) so the engine doesn't own command
// lifecycle/logging policy.
type RunFunc func(ctx context.Context, cmd *executil.Cmd, operationName string) ([]byte, error)

// RoleFunc reports the local PostgreSQL's physical recovery mode. It is injected
// by the manager (which owns the pg connection) so the health poller can be
// role-aware — e.g. only a primary (out of recovery) archives WAL — without the
// engine owning a connection. See refreshHealth for how the mode is applied and
// whether it should instead track the routing role.
type RoleFunc func(ctx context.Context) (pgmode.Mode, error)

// ArchiverStats is a snapshot of pg_stat_archiver relevant to WAL archive lag.
// Zero time fields mean the corresponding timestamp was NULL (never archived /
// never failed).
type ArchiverStats struct {
	LastArchived time.Time
	LastFailed   time.Time
	FailedCount  int64
}

// ArchiverStatsFunc returns pg_stat_archiver stats for the local primary. It is
// injected by the manager (which owns the pg connection + result scanning) so
// the health poller can compute WAL archive lag without coupling the engine to
// the SQL result types or owning a connection.
type ArchiverStatsFunc func(ctx context.Context) (ArchiverStats, error)

// PGSettings holds the PostgreSQL settings relevant to backup readiness. Empty
// strings mean the setting is unset.
type PGSettings struct {
	ArchiveCommand string
	ArchiveMode    string // "on" / "off" / "always"
	RestoreCommand string
	ServerVersion  string // full server_version (e.g. "16.2"), build suffix stripped
}

// PGSettingsFunc returns the backup-relevant PostgreSQL settings. It is injected
// by the manager (which owns the pg connection) so the health poller can
// validate configuration via cheap pg_settings reads without owning a
// connection.
type PGSettingsFunc func(ctx context.Context) (PGSettings, error)

// Engine owns all pgBackRest interaction for a single multipooler.
type Engine struct {
	logger   *slog.Logger
	run      RunFunc
	metrics  *Metrics
	health   *HealthTracker
	id       Identity
	settings Settings

	// mu guards the config resolved at runtime: the pgbackrest.conf path and
	// pgpass file (resolved when topology loads), the repo config (resolved
	// at DB-open), and the role provider. All may be re-set on reopen.
	mu         sync.Mutex
	configPath string
	pgpassPath string
	backupCfg  *commonbackup.Config
	roleFn     RoleFunc
	archiverFn ArchiverStatsFunc
	settingsFn PGSettingsFunc
}

// NewEngine constructs a backup engine and its metrics from the given fixed
// dependencies. The pgbackrest.conf path and repo config are supplied later via
// SetConfigPath / SetBackupConfig.
//
// Metric registration is best-effort: NewMetrics always returns usable (no-op on
// failure) counters, so a registration error is logged and a working engine is
// still returned rather than failing construction (which previously left the
// manager with a nil engine that panicked on first use).
func NewEngine(logger *slog.Logger, run RunFunc, id Identity, settings Settings) *Engine {
	health := NewHealthTracker()
	metrics, err := NewMetrics()
	if err != nil {
		logger.Warn("pgBackRest metric registration partially failed; backup metrics may be incomplete", "error", err)
	}
	if err := metrics.RegisterHealthCallback(health); err != nil {
		logger.Warn("pgBackRest health gauge registration failed; backup health metrics may be missing", "error", err)
	}
	return &Engine{logger: logger, run: run, metrics: metrics, health: health, id: id, settings: settings, pgpassPath: settings.PgpassPath}
}

// SetConfigPath sets the path to this pooler's pgbackrest.conf, resolved once
// topology is loaded (and again on reopen).
func (e *Engine) SetConfigPath(path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.configPath = path
}

// SetBackupConfig sets (or replaces) the resolved pgBackRest repo config. The
// manager calls this once the database's backup location is known.
func (e *Engine) SetBackupConfig(cfg *commonbackup.Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.backupCfg = cfg
}

// SetPgpassPath sets (or replaces) the path to the libpq password file exported
// as PGPASSFILE for pgbackrest commands. It is resolved alongside the config
// path when topology loads (and again on reopen).
func (e *Engine) SetPgpassPath(path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pgpassPath = path
}

// SetRoleProvider injects the function the health poller uses to learn the local
// postgres recovery mode. The manager wires this to its own recovery-mode probe
// (which owns the pg connection).
func (e *Engine) SetRoleProvider(fn RoleFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.roleFn = fn
}

// roleProvider returns the currently-set role provider, or nil.
func (e *Engine) roleProvider() RoleFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.roleFn
}

// SetArchiverStatsProvider injects the function the health poller uses to read
// pg_stat_archiver. The manager wires this to its own query path.
func (e *Engine) SetArchiverStatsProvider(fn ArchiverStatsFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.archiverFn = fn
}

// archiverStatsProvider returns the currently-set archiver stats provider, or nil.
func (e *Engine) archiverStatsProvider() ArchiverStatsFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.archiverFn
}

// SetPGSettingsProvider injects the function the health poller uses to read the
// backup-relevant PostgreSQL settings. The manager wires this to its own query
// path.
func (e *Engine) SetPGSettingsProvider(fn PGSettingsFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.settingsFn = fn
}

// settingsProvider returns the currently-set PG settings provider, or nil.
func (e *Engine) settingsProvider() PGSettingsFunc {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.settingsFn
}

// requireConfigPath returns the pgbackrest.conf path or an error if topology
// has not been loaded yet.
func (e *Engine) requireConfigPath() (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.configPath == "" {
		return "", mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgbackrest config not yet generated (topology not loaded)")
	}
	return e.configPath, nil
}

// requireBackupConfig returns the repo config or an error if it has not been
// resolved yet.
func (e *Engine) requireBackupConfig() (*commonbackup.Config, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.backupCfg == nil {
		return nil, mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "backup config not set: database backup location has not been resolved")
	}
	return e.backupCfg, nil
}

// pgbackrestCmd builds a pgbackrest command, exporting PGPASSFILE when configured.
func (e *Engine) pgbackrestCmd(ctx context.Context, args ...string) *executil.Cmd {
	e.mu.Lock()
	pgpassPath := e.pgpassPath
	e.mu.Unlock()

	cmd := executil.Command(ctx, "pgbackrest", args...)
	if pgpassPath != "" {
		cmd.AddEnv("PGPASSFILE=" + pgpassPath)
	}
	return cmd
}

// shardID returns this pooler's shard identifier.
func (e *Engine) shardID() string {
	return e.id.ShardKey().GetShard()
}

// Metrics returns the engine's pgBackRest metrics. The manager uses this to
// record metrics for the orchestration it still owns (backup lock-wait,
// restore lifecycle).
func (e *Engine) Metrics() *Metrics {
	return e.metrics
}

// Health returns the engine's backup-health tracker. The manager uses this to
// expose a snapshot on the status page and to update inline counters from the
// Backup() path.
func (e *Engine) Health() *HealthTracker {
	return e.health
}
