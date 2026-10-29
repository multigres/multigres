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
	"fmt"
	"path/filepath"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// RestoreCommand returns the archive restore_command, run through
// `pgctld restore-wrapper` so that consensus operations can stop archive replay
// by killing the PID the wrapper records in the pooler directory.
func (e *Engine) RestoreCommand(poolerDir string) string {
	return e.settings.RestoreCommand(filepath.Join(poolerDir, constants.RestoreCommandPIDFile))
}

// Restore runs pgbackrest restore to recreate PGDATA from the given backup.
// The manager orchestrates the surrounding PG lifecycle (archive config,
// starting postgres, reopening the pooler).
func (e *Engine) Restore(ctx context.Context, backupID, poolerDir string) error {
	defer e.listCache.Invalidate()
	// The sidecar's restore child gets RestoreTimeout only after it has drained
	// and quiesced, so this side must outlast that or a slow restore always
	// fails here first while the child is still writing.
	restoreCtx, cancel := context.WithTimeout(ctx, sidecarRestoreDeadline())
	defer cancel()

	args := []string{
		"--stanza=" + stanzaName,
		"--type=standby",
	}
	if backupID != "" {
		args = append(args, "--set="+backupID)
	}

	args = append(args, "restore")

	cmd := e.pgbackrestCmd(args...)
	output, err := e.output(restoreCtx, cmd)
	if err != nil && !errors.Is(err, ErrRunNotDelivered) {
		// The restore runs on in the sidecar after the call that began it has
		// failed, so the caller may only clean up once it has ended.
		err = e.settleSidecarRestore(ctx, err, output)
	}
	if err != nil {
		if errors.Is(err, ErrRestoreUnsettled) || errors.Is(err, ErrRunNotDelivered) {
			return err
		}
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest restore failed: %v\nOutput: %s", err, output))
	}
	e.awaitRestoreStart(ctx)

	return nil
}
