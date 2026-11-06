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
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStaticSessionTokenSnapshotRotationAndOmission(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, staticKeyIDFile), []byte("key"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, staticKeySecretFile), []byte("secret"), 0o600))
	s := &Server{cfg: Config{S3CredSource: CredStatic, S3StaticKeyDir: dir}}
	var previous RepoSnapshot
	for _, token := range []string{"", "temporary-one", "temporary-two", ""} {
		if token == "" {
			_ = os.Remove(filepath.Join(dir, staticKeyTokenFile))
		} else {
			require.NoError(t, os.WriteFile(filepath.Join(dir, staticKeyTokenFile), []byte(token+"\n"), 0o600))
		}
		snap, err := s.buildSnapshot(map[uint32]uint32{1: 0, 4: 0})
		require.NoError(t, err)
		for _, n := range []uint32{1, 4} {
			require.Equal(t, token, snap.StaticKeys[n].SessionToken)
		}
		if token == "temporary-two" {
			require.Equal(t, "temporary-one", previous.StaticKeys[1].SessionToken)
		}
		previous = snap
		secrets := Secrets{RepoSnapshot: snap}
		for _, kind := range []Kind{KindServer, KindLocalCommand, KindS3Command} {
			env := strings.Join(envFor(kind, testConfig(t), secrets), "\n")
			if kind == KindS3Command && token != "" {
				require.Contains(t, env, "PGBACKREST_REPO1_S3_TOKEN="+token)
				require.Contains(t, env, "PGBACKREST_REPO4_S3_TOKEN="+token)
			} else {
				require.NotContains(t, env, "S3_TOKEN=")
			}
		}
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, staticKeyTokenFile), nil, 0o600))
	_, err := loadStaticKey(dir)
	require.Error(t, err, "present but empty token is invalid, not permanent credentials")
}

func TestTemporaryTokenOnlyReachesRepositoryChildren(t *testing.T) {
	dir := t.TempDir()
	// Model authentication requiring all three fields; control-only children
	// fail if any S3 session token (including inherited AWS env) reaches them.
	script := `#!/bin/sh
test -z "$AWS_SESSION_TOKEN" || exit 9
case "$*" in
 *info*) test "$PGBACKREST_REPO1_S3_KEY" = key && test "$PGBACKREST_REPO1_S3_KEY_SECRET" = secret && test "$PGBACKREST_REPO1_S3_TOKEN" = temporary ;;
 *) test -z "$PGBACKREST_REPO1_S3_TOKEN" ;;
esac
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("AWS_SESSION_TOKEN", "must-not-inherit")
	s := newTestServer(t, nil)
	s.setRepoSnapshot(RepoSnapshot{StaticKeys: map[uint32]StaticKey{1: {ID: "key", Secret: "secret", SessionToken: "temporary"}}})
	for _, command := range []string{cmdInfo, cmdStop, cmdStart, cmdVersion} {
		code, err := s.runChild(t.Context(), KindS3Command, command, nil, io.Discard, io.Discard)
		require.NoError(t, err)
		require.Zero(t, code, command)
	}
	require.NotContains(t, strings.Join(envFor(KindServer, testConfig(t), s.secretsNow()), "\n"), "temporary")
}
