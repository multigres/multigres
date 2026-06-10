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

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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

// Engine owns all pgBackRest interaction for a single multipooler.
type Engine struct {
	logger   *slog.Logger
	run      RunFunc
	metrics  *Metrics
	id       Identity
	settings Settings

	// mu guards the config resolved at runtime: the pgbackrest.conf path and
	// pgpass file (resolved when topology loads) and the repo config (resolved
	// at DB-open). All may be re-set on reopen.
	mu         sync.Mutex
	configPath string
	pgpassPath string
	backupCfg  *commonbackup.Config
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
	metrics, err := NewMetrics()
	if err != nil {
		logger.Warn("pgBackRest metric registration partially failed; backup metrics may be incomplete", "error", err)
	}
	return &Engine{logger: logger, run: run, metrics: metrics, id: id, settings: settings, pgpassPath: settings.PgpassPath}
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
