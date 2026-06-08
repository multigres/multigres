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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestVerify_ExecPaths drives Verify end-to-end against a stubbed `pgbackrest`
// binary on PATH. It exercises the three outcomes the production code must
// distinguish, which the unit-level integration tests cannot reach in-process:
// a healthy stanza, pgBackRest reporting corruption while still exiting 0 (the
// case this method exists to catch), and pgBackRest exiting non-zero.
func TestVerify_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		stdout      string
		exitCode    int
		wantErr     bool
		errContains string
	}{
		{
			name:     "healthy stanza succeeds",
			stdout:   "P00   INFO: stanza: multigres\n    status: ok\n",
			exitCode: 0,
			wantErr:  false,
		},
		{
			// pgBackRest exits 0 even when verify finds problems, so the only
			// signal is the "status: error" line in its output.
			name:        "corrupt stanza surfaces as error",
			stdout:      "P00   INFO: invalid checksum 'x.zst'\nP00   INFO: stanza: multigres\n    status: error\n",
			exitCode:    0,
			wantErr:     true,
			errContains: "verify found problems",
		},
		{
			name:        "nonzero exit surfaces as error",
			stdout:      "P00  ERROR: unable to load info file\n",
			exitCode:    1,
			wantErr:     true,
			errContains: "pgbackrest verify failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binDir := t.TempDir()
			script := fmt.Sprintf("#!/bin/bash\ncat <<'PGBR_EOF'\n%sPGBR_EOF\nexit %d\n", tt.stdout, tt.exitCode)
			require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(script), 0o755))
			t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			resp, err := e.Verify(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Contains(t, resp.RawOutput, "status: ok")
			require.GreaterOrEqual(t, resp.Duration, time.Duration(0))
		})
	}
}

// TestVerifyStanzaErrorDetection covers the parser that turns pgBackRest's
// exit-0-but-reported-error output into a failed call. pgBackRest's verify
// command exits 0 even when it finds a corrupt backup or invalid WAL, so the
// stanza "status: error" line is the signal we key on.
func TestVerifyStanzaErrorDetection(t *testing.T) {
	tests := []struct {
		name      string
		output    string
		wantError bool
	}{
		{
			name:      "healthy stanza",
			output:    "INFO: stanza: multigres\n    status: ok\n",
			wantError: false,
		},
		{
			name: "corrupt backup",
			output: "INFO: invalid checksum '20260605-112558F/pg_data/base/1/2838.zst'\n" +
				"INFO: stanza: multigres\n    status: error\n" +
				"      backup: 20260605-112558F, status: invalid, total files checked: 999, total valid files: 998\n" +
				"        checksum invalid: 1\n",
			wantError: true,
		},
		{
			name:      "empty output",
			output:    "",
			wantError: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantError, verifyStanzaErrorRe.MatchString(tc.output))
		})
	}
}
