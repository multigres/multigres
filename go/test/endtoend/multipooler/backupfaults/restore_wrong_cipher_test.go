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

package backupfaults

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestJoin_FailsWithWrongCipherKey proves the security property the whole
// feature exists for: a pooler holding the wrong cipher key cannot bring up a
// database from an encrypted backup repository — and refuses to act rather
// than bootstrapping over a repo it cannot read.
//
// One pooler self-bootstraps an encrypted repository with the real key (no
// multiorch — the first backup is created by the pooler itself under the
// backup lease; leader election is unnecessary here). A second pooler then
// joins pointed at a *different* cipher key file. Its config renders (the key
// is self-consistent with its own fingerprint), but it cannot decrypt the
// repo's archive.info to list backups. The monitor treats that read failure as
// unknown state (not "no backups"): it surfaces the error and skips the tick
// instead of trying to create a fresh stanza over the unreadable repo. The
// second pooler is identical to a healthy replica except for the cipher key,
// so the failure isolates the cause to the key mismatch.
//
// (The check-for-complete-backups error text is deliberately not asserted: a
// wrong AES-CBC key surfaces nondeterministically as a pgBackRest CryptoError
// or a FormatError depending on whether the garbage plaintext happens to parse.
// The stable, meaningful observable is that the monitor refuses to bootstrap.)
func TestJoin_FailsWithWrongCipherKey(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}

	// Single deferred pooler with encryption; no multiorch. Starting the
	// multipooler triggers self-bootstrap of the encrypted repo.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithBackupEncryption(),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()
	require.NotEmpty(t, setup.BackupCipherKey, "encrypted setup must mint a cipher key")

	require.Len(t, setup.Multipoolers, 1)
	var bootstrapper *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		bootstrapper = v
	}
	require.NoError(t, bootstrapper.Multipooler.Start(t.Context(), t))

	// Wait for the encrypted first backup to land in the repo — that is the
	// artifact the wrong-key pooler will be unable to read.
	backupClient := createBackupClient(t, bootstrapper.Multipooler.GrpcPort)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := backupClient.GetBackups(ctx, &multipoolermanagerdata.GetBackupsRequest{Limit: 10})
		if err != nil {
			return false
		}
		for _, b := range resp.Backups {
			if b.Status == multipoolermanagerdata.BackupMetadata_COMPLETE {
				return true
			}
		}
		return false
	}, 90*time.Second, 2*time.Second, "encrypted first backup should be created")
	t.Log("Encrypted repository bootstrapped with a complete backup")

	// A cipher key file for the joining pooler holding a *different* passphrase.
	// It is internally consistent (the pooler's own fingerprint is computed
	// from it), so config rendering succeeds — the mismatch only surfaces when
	// pgBackRest tries to read the repo the real key created.
	wrongKeyFile := filepath.Join(t.TempDir(), "wrong-cipher-keys.json")
	require.NoError(t, os.WriteFile(wrongKeyFile,
		[]byte(`{"1": "this-is-not-the-real-passphrase"}`), 0o600))

	// Provision a second pooler pointed at the wrong key. CreateMultipoolerInstance
	// does not inherit the shared (real) key flag, so the wrong key is the only
	// --pgbackrest-cipher-key-file it sees.
	const joinerName = "pooler-2"
	joiner := setup.CreateMultipoolerInstance(t, joinerName,
		utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	joiner.Multipooler.ExtraArgs = append(joiner.Multipooler.ExtraArgs,
		"--pgbackrest-cipher-key-file", wrongKeyFile)

	require.NoError(t, joiner.Pgctld.Start(t.Context(), t))
	require.NoError(t, joiner.Multipooler.Start(t.Context(), t))

	// The monitor cannot read the encrypted repo with the wrong key, so it
	// surfaces a check-for-complete-backups error and skips the tick.
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(joiner.Multipooler.LogFile)
		if err != nil {
			return false
		}
		return strings.Contains(string(data), "check for complete backups")
	}, 60*time.Second, 2*time.Second,
		"monitor should surface an error when it cannot read the encrypted repo")

	// We must NOT bootstrap a fresh stanza over the unreadable repo.
	// An unreadable repository is unknown state, not an empty
	// one,  treating it as empty would create a conflicting stanza.
	data, err := os.ReadFile(joiner.Multipooler.LogFile)
	require.NoError(t, err)
	require.NotContains(t, string(data), "Creating first backup for shard",
		"must not bootstrap when the backup repository cannot be read")
	t.Log("Monitor refused to bootstrap over the unreadable encrypted repo, as expected")
}
