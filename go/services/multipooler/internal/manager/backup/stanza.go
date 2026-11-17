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
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/executil"
)

// stanzaDeleteTimeout bounds each command StanzaDelete runs.
const stanzaDeleteTimeout = 30 * time.Second

// StanzaCreate runs pgbackrest stanza-create. The operation is idempotent
// and safe to run concurrently — no backup lease required.
func (e *Engine) StanzaCreate(ctx context.Context) error {
	defer e.listCache.Invalidate()

	if err := e.runPgBackRestCmd(ctx, 5*time.Second, e.pgbackrestIOTimeoutCmd, "stanza-create"); err != nil {
		return err
	}

	e.logger.InfoContext(ctx, "pgbackrest stanza initialized", "stanza", stanzaName)
	return nil
}

// StanzaDelete removes the stanza and its archived WAL. pgBackRest requires the
// stanza to be stopped, so it runs `stop`, `stanza-delete --force` and `start`;
// `start` always runs so the stop file never blocks later commands. Callers must
// hold the backup lease and have checked that the repository holds no backups.
func (e *Engine) StanzaDelete(ctx context.Context) (retErr error) {
	defer e.listCache.Invalidate()
	if err := e.runPgBackRestCmd(ctx, stanzaDeleteTimeout, e.pgbackrestCmd, "stop"); err != nil {
		return err
	}
	defer func() {
		err := e.runPgBackRestCmd(context.WithoutCancel(ctx), stanzaDeleteTimeout, e.pgbackrestCmd, "start")
		if err == nil {
			return
		}
		e.logger.ErrorContext(ctx, "failed to start the pgbackrest stanza again after stanza-delete", "stanza", stanzaName, "error", err)
		if retErr == nil {
			retErr = err
		}
	}()

	if err := e.runPgBackRestCmd(ctx, stanzaDeleteTimeout, e.pgbackrestIOTimeoutCmd, "--force", "stanza-delete"); err != nil {
		return err
	}

	e.logger.InfoContext(ctx, "pgbackrest stanza removed", "stanza", stanzaName)
	return nil
}

// StanzaStart runs `pgbackrest start`, clearing a stop file left by an
// interrupted StanzaDelete. It only warns when there is none.
func (e *Engine) StanzaStart(ctx context.Context) error {
	return e.runPgBackRestCmd(ctx, stanzaDeleteTimeout, e.pgbackrestCmd, "start")
}

// IsStaleStanzaError reports whether err from StanzaCreate is error 028 (info
// files do not match the database) or 055 (an info file is missing), as left by
// another cluster's stanza or an interrupted create or delete. Act on it only
// when the repository holds no backups.
func IsStaleStanzaError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "[028]") || strings.Contains(msg, "[055]")
}

// runPgBackRestCmd runs a pgBackRest command for the stanza within timeout.
// newCmd is pgbackrestIOTimeoutCmd for repository commands and pgbackrestCmd
// for start and stop, which reject --io-timeout.
func (e *Engine) runPgBackRestCmd(ctx context.Context, timeout time.Duration, newCmd func(context.Context, ...string) *executil.Cmd, args ...string) error {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "failed to get pgbackrest config")
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := newCmd(runCtx, append([]string{"--stanza=" + stanzaName, "--config=" + configPath}, args...)...)
	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest %s failed for stanza %s: %v\nOutput: %s",
				args[len(args)-1], stanzaName, err, output))
	}
	return nil
}
