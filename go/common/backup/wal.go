// Copyright 2026 Supabase, Inc.
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

package backup

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/multigres/multigres/go/common/constants"
)

// WAL command strings are an on-disk contract across image upgrades and rollbacks.
// Resolve the standalone executables only in the directory approved for this
// launch by pgctld. An older pgctld may exit successfully on an unknown
// subcommand, so never fall back to its CLI or an executable elsewhere on PATH.
// Both names include pgbackrest because its check command rejects an
// archive_command that does not contain the literal "pgbackrest".
const (
	ArchiveWALExecutable = "pgbackrest-archive-wal"
	RestoreWALExecutable = "pgbackrest-restore-wal"
	WALWrapperVersion    = 1
	WALBinDirEnv         = "MULTIGRES_WAL_BIN_DIR"
	walCommandPrefix     = `"${` + WALBinDirEnv + `:?}"/`
	ArchiveWALCommand    = walCommandPrefix + ArchiveWALExecutable + ` "%p"`
	RestoreWALCommand    = walCommandPrefix + RestoreWALExecutable + ` "%f" "%p"`
	WALStanza            = "multigres"
	WALPoolerDirEnv      = "MULTIGRES_WAL_POOLER_DIR"
)

func ClientConfigPath(poolerDir string) string {
	return filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")
}

// WALCommands selects the persisted command ABI supported by the postgres image.
func WALCommands(wrappers bool, config, dir string) (string, string) {
	if wrappers {
		return ArchiveWALCommand, RestoreWALCommand
	}
	return LegacyWALCommands(config, dir)
}

// LegacyWALCommands reproduces the commands used before standalone executables.
func LegacyWALCommands(config, dir string) (archive, restore string) {
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	return fmt.Sprintf("pgbackrest --stanza=%s --config=%s archive-push %%p", WALStanza, config), fmt.Sprintf(`pgctld restore-wrapper %s -- pgbackrest --stanza=%s --config=%s archive-get %%f "%%p"`, quote(filepath.Join(dir, constants.RestoreCommandPIDFile)), quote(WALStanza), quote(config))
}
