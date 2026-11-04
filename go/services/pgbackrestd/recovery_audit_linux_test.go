//go:build linux

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

package pgbackrestd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRecoveryAuditPreservesRetiredWorkerEvidence(t *testing.T) {
	for _, obstacle := range []string{"none", "archive is file", "existing evidence", "later invalid marker"} {
		t.Run(obstacle, func(t *testing.T) {
			s := newTestServer(t, nil)
			// This unit fixture models the documented serialized PID-1 replacement
			// contract, not evidence of kernel namespace teardown. The real instance
			// lock remains held by s throughout recovery and audit persistence.
			current := s.workerOwner
			current.PID = 1
			s.workerOwner = current
			old := current
			old.Token = uuid.NewString()
			old.StartTicks++
			raw, err := json.Marshal(workerMarker{Version: 1, Owner: old})
			require.NoError(t, err)
			const name = "worker-a.pending"
			source := s.cfg.stateSub(name)
			require.NoError(t, os.WriteFile(source, raw, 0o600))
			archive := s.cfg.stateSub("recovered-workers")
			switch obstacle {
			case "archive is file":
				require.NoError(t, os.WriteFile(archive, []byte("keep"), 0o600))
			case "existing evidence":
				require.NoError(t, os.Mkdir(archive, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(archive, name), []byte("prior audit"), 0o600))
			case "later invalid marker":
				require.NoError(t, os.WriteFile(s.cfg.stateSub("worker-z.pending"), []byte("invalid"), 0o600))
			}
			err = s.recoverWorkerMarkers()
			if obstacle != "none" {
				require.Error(t, err)
				retained, readErr := os.ReadFile(source)
				require.NoError(t, readErr)
				require.Equal(t, raw, retained, "a refused recovery must retain active evidence")
				if obstacle == "existing evidence" {
					prior, readErr := os.ReadFile(filepath.Join(archive, name))
					require.NoError(t, readErr)
					require.Equal(t, "prior audit", string(prior))
				}
				return
			}
			require.NoError(t, err)
			_, err = os.Stat(source)
			require.ErrorIs(t, err, os.ErrNotExist)
			evidence, err := os.ReadFile(filepath.Join(archive, name))
			require.NoError(t, err)
			require.Equal(t, raw, evidence)
			info, err := os.Stat(archive)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
			require.NoError(t, s.recoverWorkerMarkers(), "a later pass must not treat audit records as active workers")
		})
	}
}
