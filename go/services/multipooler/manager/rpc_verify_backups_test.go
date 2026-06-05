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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyBackups(t *testing.T) {
	// VerifyBackups requires a running pgbackrest with a valid stanza.
	// The actual invocation, including the corrupt-backup negative path, is
	// tested by integration tests (endtoend/multipooler). This test validates
	// the precondition checks.

	t.Run("fails when not ready", func(t *testing.T) {
		pm := &MultiPoolerManager{}
		_, err := pm.VerifyBackups(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "manager is in unknown state")
	})
}

// TestVerifyStanzaErrorDetection covers the parser that turns pgBackRest's
// exit-0-but-reported-error output into a failed RPC. pgBackRest's verify
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
