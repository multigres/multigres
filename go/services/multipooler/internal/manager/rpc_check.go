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

package manager

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// runPgBackRestCheck runs `pgbackrest check`, which forces a WAL switch and
// confirms the segment reaches the repo, validating archive_command and
// archive_mode end-to-end. On a standby pgBackRest skips the WAL switch, so
// this is safe to call regardless of role. On failure the combined command
// output is folded into the returned error.
func (pm *MultiPoolerManager) runPgBackRestCheck(ctx context.Context) error {
	configPath, err := pm.pgBackRestConfig()
	if err != nil {
		return mterrors.Wrap(err, "pgbackrest config not found")
	}

	ctx, cancel := context.WithTimeout(ctx, backup.CheckTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + pm.stanzaName(),
		"--config=" + configPath,
		"check",
	}
	cmd := pm.pgbackrestCmd(ctx, args...)

	var output []byte
	err = telemetry.WithSpan(ctx, "pgbackrest-check", func(ctx context.Context) error {
		var runErr error
		output, runErr = pm.runLongCommand(ctx, cmd, "pgbackrest check")
		return runErr
	})
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest check failed for stanza %s: %v\nOutput: %s",
				pm.stanzaName(), err, string(output)))
	}
	return nil
}
