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
	"github.com/multigres/multigres/go/common/topoclient"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// StanzaCreate runs pgbackrest stanza-create. The operation is idempotent
// and safe to run concurrently — no backup lease required.
func (e *Engine) StanzaCreate(ctx context.Context) error {
	defer e.listCache.Invalidate()
	stanzaCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := e.pgbackrestCmd(
		"--stanza="+stanzaName,
		"stanza-create")

	output, err := e.output(stanzaCtx, cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest stanza-create failed for stanza %s: %v\nOutput: %s",
				stanzaName, err, output))
	}

	e.logger.InfoContext(ctx, "pgbackrest stanza initialized", "stanza", stanzaName)
	return nil
}

// StanzaDelete removes the stanza and its archived WAL through the sidecar,
// which stops archiving, deletes the stanza and starts archiving again, and
// fails when it cannot start it again. The sidecar also refuses unless the
// repository holds no backup. Callers must hold the backup lease and have
// checked that the repository holds no backups.
func (e *Engine) StanzaDelete(ctx context.Context) error {
	defer e.listCache.Invalidate()
	runCtx, cancel := context.WithTimeout(ctx, sidecarResetDeadline())
	defer cancel()
	output, err := e.output(runCtx, &pgCmd{command: pb.Command_COMMAND_STANZA_DELETE})
	if err != nil {
		failure := mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest stanza-delete failed for stanza %s: %v\nOutput: %s",
				stanzaName, err, output))
		if topoclient.IsLeaseAbandon(err) {
			// Rebuilt as text above, so the request not to unlock the lease is kept.
			return topoclient.AbandonLease(failure)
		}
		return failure
	}

	e.logger.InfoContext(ctx, "pgbackrest stanza removed", "stanza", stanzaName)
	return nil
}

// ResetArchiveSpool has the sidecar clear the asynchronous archive-push state
// of earlier attempts and lift any stop file an interrupted stanza reset left.
// A new initdb restarts WAL numbering, so a stale acknowledgement would make
// archive-push skip the new cluster's first segment and the archive check time
// out. It needs no repository, so it can run before the first Configure.
func (e *Engine) ResetArchiveSpool(ctx context.Context) error {
	runCtx, cancel := context.WithTimeout(ctx, sidecarResetDeadline())
	defer cancel()
	output, err := e.output(runCtx, &pgCmd{command: pb.Command_COMMAND_ARCHIVE_SPOOL_RESET})
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest archive spool reset failed for stanza %s: %v\nOutput: %s",
				stanzaName, err, output))
	}
	return nil
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
