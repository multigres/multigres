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
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// StanzaCreate runs pgbackrest stanza-create. The operation is idempotent
// and safe to run concurrently — no backup lease required.
func (e *Engine) StanzaCreate(ctx context.Context) error {
	configPath, err := e.requireConfigPath()
	if err != nil {
		return mterrors.Wrap(err, "failed to get pgbackrest config")
	}

	stanzaCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := e.pgbackrestCmd(stanzaCtx,
		"--stanza="+stanzaName,
		"--config="+configPath,
		"stanza-create")

	output, err := safeCombinedOutput(cmd)
	if err != nil {
		return mterrors.New(mtrpcpb.Code_INTERNAL,
			fmt.Sprintf("pgbackrest stanza-create failed for stanza %s: %v\nOutput: %s",
				stanzaName, err, output))
	}

	e.logger.InfoContext(ctx, "pgbackrest stanza initialized", "stanza", stanzaName)
	return nil
}
