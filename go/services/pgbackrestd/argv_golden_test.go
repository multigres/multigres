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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// multipoolerArgvGolden is the argv the multipooler engine sends in sidecar
// mode, written by its own test (TestEngine_SidecarArgvGolden). Neither
// package imports the other; this file is the contract. Every entry must pass
// the real checkArgs.
const multipoolerArgvGolden = "../multipooler/internal/manager/backup/testdata/sidecar_argv.golden.json"

func TestAllowlist_AcceptsMultipoolerArgvGolden(t *testing.T) {
	raw, err := os.ReadFile(multipoolerArgvGolden)
	require.NoError(t, err)
	var entries []struct {
		Name    string   `json:"name"`
		Command string   `json:"command"`
		Args    []string `json:"args"`
	}
	require.NoError(t, json.Unmarshal(raw, &entries))
	require.NotEmpty(t, entries)

	seen := map[string]bool{}
	for _, e := range entries {
		cmd, ok := pb.Command_value[e.Command]
		require.True(t, ok, "%s: unknown command %q", e.Name, e.Command)
		seen[e.Command] = true
		assert.NoError(t, checkArgs(pb.Command(cmd), e.Args), "%s: %s %v", e.Name, e.Command, e.Args)
	}
	for _, c := range []pb.Command{
		pb.Command_COMMAND_BACKUP, pb.Command_COMMAND_RESTORE, pb.Command_COMMAND_EXPIRE,
		pb.Command_COMMAND_INFO, pb.Command_COMMAND_CHECK, pb.Command_COMMAND_STANZA_CREATE, pb.Command_COMMAND_VERIFY,
	} {
		assert.True(t, seen[c.String()], "the golden has no %s entry", c)
	}
}

// The multipooler pins the same hashes for the same inputs, so a field dropped
// from the fingerprint on either side fails a test on that side.
func TestLocationFingerprint_PinnedValues(t *testing.T) {
	s3 := Config{LocationType: LocationS3, S3Bucket: "b", S3Region: "r", S3Endpoint: "https://e", S3KeyPrefix: "p/"}
	assert.Equal(t, "3fd332ada31a0d7a11c515a65ea8edd6e030aad106be71742c70cc82dd777c58", s3.locationFingerprint())
	fs := Config{LocationType: LocationFilesystem, FSRepoPath: "/data/backups"}
	assert.Equal(t, "ac9d5d4edc31242dd8ebdb27151e528dc39568c30066b8104248bea9cf6d1a19", fs.locationFingerprint())
	other := s3
	other.S3KeyPrefix = "q/"
	assert.NotEqual(t, s3.locationFingerprint(), other.locationFingerprint())
}
