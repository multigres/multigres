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
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/tools/fileutil"
)

// This integration gate combines the safety wire with the separately owned
// local snapshot/env implementation, not a stub or a version-only assertion.
func TestLifecycleV1AtomicBundleContract(t *testing.T) {
	newFakeBackrest(t, nil, false)
	dir := t.TempDir()
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("legacy-must-not-win"), 0o600))
	}
	bundle := filepath.Join(dir, "credentials.json")
	require.NoError(t, fileutil.AtomicWriteFile(bundle, []byte(`{"access_key_id":"bundle-id","secret_access_key":"bundle-secret","session_token":"bundle-token"}`), 0o600))
	h := newControlHarness(t, func(c *Config) { c.LocationType = LocationS3; c.S3CredSource = CredStatic; c.S3StaticKeyDir = dir })
	status, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	require.Equal(t, uint32(1), status.Msg.LifecycleProtocolVersion)
	require.Equal(t, status.Msg.RestoreIncarnation, status.Msg.InstanceId)
	h.configure(t, 1, repos(1, 1))
	env := envFor(KindS3Command, h.s.cfg, h.s.secretsNow())
	require.Contains(t, env, "PGBACKREST_REPO1_S3_KEY=bundle-id")
	require.Contains(t, env, "PGBACKREST_REPO1_S3_KEY_SECRET=bundle-secret")
	require.Contains(t, env, "PGBACKREST_REPO1_S3_TOKEN=bundle-token")
	require.NoError(t, fileutil.AtomicWriteFile(bundle, []byte(`{"access_key_id":"rotated-id","secret_access_key":"rotated-secret"}`), 0o600))
	h.configure(t, 2, repos(1, 1))
	env = envFor(KindS3Command, h.s.cfg, h.s.secretsNow())
	require.Contains(t, env, "PGBACKREST_REPO1_S3_KEY=rotated-id")
	require.Contains(t, env, "PGBACKREST_REPO1_S3_KEY_SECRET=rotated-secret")
	require.NotContains(t, env, "PGBACKREST_REPO1_S3_TOKEN=bundle-token")
	require.NotContains(t, env, "PGBACKREST_REPO1_S3_TOKEN=legacy-must-not-win")
	require.NoError(t, fileutil.AtomicWriteFile(bundle, []byte(`{"access_key_id":"incomplete"}`), 0o600))
	_, err = h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{Revision: 3, Repos: repos(1, 1)}))
	require.Error(t, err, "malformed bundle must not fall back to legacy fields")
	require.Equal(t, env, envFor(KindS3Command, h.s.cfg, h.s.secretsNow()), "failed configure preserves prior complete snapshot")
}
