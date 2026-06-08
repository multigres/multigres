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
	"regexp"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// verifyStanzaErrorRe matches the per-stanza status line pgBackRest prints when
// `verify` finds problems (a corrupt/partial backup or a missing/invalid WAL
// segment). pgBackRest exits 0 even when it reports these — the failure is only
// visible in the output — so we scan for it explicitly. A healthy repo prints
// "status: ok".
var verifyStanzaErrorRe = regexp.MustCompile(`(?m)^\s*status: error`)

// VerifyResult is the in-process return type for VerifyBackups. The gRPC
// service layer maps this onto the proto VerifyBackupsResponse.
type VerifyResult struct {
	Duration  time.Duration
	RawOutput string
}

// VerifyBackups runs `pgbackrest verify` against the full stanza (no --set),
// validating every backup file and WAL segment in the repository.
//
// pgBackRest exits 0 even when verify finds a corrupt/partial backup or an
// invalid WAL segment — it reports the problem in its output and as a stanza
// "status: error" line. We scan for that and return an error, so a corrupt
// repository surfaces as a failed RPC rather than a success carrying buried
// error text.
//
// Concurrency: verify is read-only against the S3 repo and takes no action
// lock and no backup lease. The one benign edge case is verify running while
// expire is deleting backups on the same stanza — verify may report a transient
// "missing file". This is not corruption; re-running verify clears it. Because
// it still trips the stanza "status: error" check, such a run returns an error
// and the caller should retry.
func (pm *MultiPoolerManager) VerifyBackups(ctx context.Context) (*VerifyResult, error) {
	if err := pm.checkReady(); err != nil {
		return nil, err
	}

	configPath, err := pm.pgBackRestConfig()
	if err != nil {
		return nil, mterrors.Wrap(err, "pgbackrest config not found")
	}

	ctx, cancel := context.WithTimeout(ctx, backup.StanzaVerifyTimeout)
	defer cancel()

	args := []string{
		"--stanza=" + pm.stanzaName(),
		"--config=" + configPath,
		"verify",
	}
	cmd := pm.pgbackrestCmd(ctx, args...)

	start := time.Now()
	var output []byte
	err = telemetry.WithSpan(ctx, "verify-backups", func(ctx context.Context) error {
		var runErr error
		output, runErr = pm.runLongCommand(ctx, cmd, "pgbackrest verify")
		return runErr
	})
	duration := time.Since(start)
	if err != nil {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest verify failed: %v\nOutput: %s", err, string(output)))
	}
	// pgBackRest exits 0 even when it finds corruption, so inspect the output
	// for the stanza-level error status it prints alongside invalid backups or
	// WAL segments.
	if verifyStanzaErrorRe.Match(output) {
		return nil, mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest verify found problems in stanza %s\nOutput: %s",
				pm.stanzaName(), string(output)))
	}
	return &VerifyResult{Duration: duration, RawOutput: string(output)}, nil
}
