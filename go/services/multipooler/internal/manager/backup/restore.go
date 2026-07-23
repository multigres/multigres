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
	"fmt"
	"path/filepath"
	"strings"

	commonbackup "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// shellQuoteSingle quotes s for safe embedding as a single argument in a
// command line executed by /bin/sh (as postgres does for restore_command).
// Not the same as SQL/postgres-config quoting (ast.QuoteStringLiteral): in
// shell, a doubled ” is two adjacent empty strings, not an escaped quote —
// an embedded quote must close the quoted section, escape a literal quote,
// then reopen it.
func shellQuoteSingle(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// Restore runs pgbackrest restore to recreate PGDATA from the given backup.
// The manager orchestrates the surrounding PG lifecycle (archive config,
// starting postgres, reopening the pooler).
func (e *Engine) Restore(ctx context.Context, backupID, poolerDir string) error {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "pgbackrest config not found")
	}

	restoreCtx, cancel := context.WithTimeout(ctx, commonbackup.RestoreTimeout)
	defer cancel()

	rawRestoreCommand := fmt.Sprintf(`pgbackrest --stanza=%s --config=%s archive-get %%f "%%p"`, shellQuoteSingle(stanzaName), shellQuoteSingle(configPath))
	pidFile := filepath.Join(poolerDir, constants.RestoreCommandPIDFile)
	// The wrapper stores the PID to a file on disk so later on consensus operations
	// for any cohort member or recruited cohort candidate can be sure that they're never
	// pulling WAL from the archive, only the consensus leader.
	wrappedRestoreCommand := fmt.Sprintf("pgctld restore-wrapper %s -- %s", shellQuoteSingle(pidFile), rawRestoreCommand)

	// pgbackrest writes --recovery-option values verbatim between single quotes
	// in postgresql.auto.conf, without escaping the embedded single quotes
	// shellQuoteSingle just added around pidFile — so those must be escaped
	// here the same way postgres itself escapes a quote inside a quoted
	// config value (' -> ''), or the resulting line fails to parse.
	escapedRestoreCommand := strings.ReplaceAll(wrappedRestoreCommand, "'", "''")

	args := []string{
		"--stanza=" + stanzaName,
		"--config=" + configPath,
		"--type=standby",
		"--recovery-option=restore_command=" + escapedRestoreCommand,
	}

	if backupID != "" {
		args = append(args, "--set="+backupID)
	}

	args = append(args, "restore")

	cmd := e.pgbackrestCmd(restoreCtx, args...)
	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, output))
	}

	return nil
}
