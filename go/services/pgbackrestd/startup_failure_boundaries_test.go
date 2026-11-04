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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartupFailureClosesEarlierListeners(t *testing.T) {
	for _, phase := range []string{"wal", "control", "credentials"} {
		t.Run(phase, func(t *testing.T) {
			s := newTestServer(t, nil)
			dir, err := os.MkdirTemp("/tmp", "startup-")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
			s.cfg.WALSocket = filepath.Join(dir, "wal.sock")
			s.cfg.ControlSocket = filepath.Join(dir, "control.sock")
			s.bin = "/bin/true"
			blocked := filepath.Join(dir, "not-a-directory")
			require.NoError(t, os.WriteFile(blocked, []byte("evidence"), 0o600))
			switch phase {
			case "wal":
				s.cfg.WALSocket = filepath.Join(blocked, "wal.sock")
			case "control":
				s.cfg.ControlSocket = filepath.Join(blocked, "control.sock")
			case "credentials":
				require.NoError(t, os.RemoveAll(s.cfg.stateSub(dirCreds)))
				require.NoError(t, os.WriteFile(s.cfg.stateSub(dirCreds), []byte("evidence"), 0o600))
			}
			require.Error(t, s.Run(t.Context()))
			// Dial before any unlink/rebind: a leaked open listener would still accept.
			for _, socket := range []string{s.cfg.WALSocket, s.cfg.ControlSocket} {
				conn, dialErr := net.DialTimeout("unix", socket, time.Second)
				if conn != nil {
					_ = conn.Close()
				}
				require.Error(t, dialErr, "failed startup must leave no accepting listener")
			}
			raw, err := os.ReadFile(blocked)
			require.NoError(t, err)
			require.Equal(t, "evidence", string(raw))
		})
	}
}

func TestStartupSocketAndStateIOFailures(t *testing.T) {
	cfg := testConfig(t)
	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, []byte("keep"), 0o600))
	cfg.StateDir = filepath.Join(blocker, "state")
	require.ErrorContains(t, prepareState(cfg), "create state dir")
	require.ErrorContains(t, removeEntry(blocker, "socket"), "open socket dir")
	_, err := listenUnix(filepath.Join(t.TempDir(), strings.Repeat("x", 150)))
	require.ErrorContains(t, err, "listen on")
	cfg = testConfig(t)
	require.NoError(t, os.MkdirAll(cfg.stateSub(dirConf), 0o700))
	require.NoError(t, os.Mkdir(cfg.stateSub(filePgpass), 0o700))
	require.ErrorContains(t, prepareState(cfg), "create pgpass")
	require.NoError(t, os.RemoveAll(cfg.stateSub(dirConf)))
	require.NoError(t, os.WriteFile(cfg.stateSub(dirConf), []byte("keep"), 0o600))
	require.ErrorContains(t, writeMinimalConf(cfg), "stat")
	require.Equal(t, "abcd...", truncate("abcdefgh", 4))
}

func TestCredentialSourceSelectionAndInitializationFailure(t *testing.T) {
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/fixture/token")
	t.Setenv("AWS_ROLE_ARN", "fixture-role")
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://169.254.170.23/credentials")
	t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/fixture/token")
	for _, source := range []string{CredWebIdentity, CredPodIdentity, CredEndpoint, CredStatic, CredNone} {
		cfg := testConfig(t)
		cfg.S3CredSource, cfg.S3CredEndpoint = source, "https://credentials.invalid/session"
		s := &Server{cfg: cfg}
		selected, err := s.credSourceFor()
		require.NoError(t, err)
		switch source {
		case CredWebIdentity:
			require.IsType(t, &webIdentitySource{}, selected)
		case CredPodIdentity:
			require.IsType(t, &podIdentitySource{}, selected)
		case CredEndpoint:
			require.IsType(t, &endpointSource{}, selected)
		default:
			require.IsType(t, unservedSource{}, selected)
		}
	}
	s := newTestServer(t, nil)
	s.cfg.LocationType, s.cfg.S3CredSource = LocationS3, CredEndpoint
	s.cfg.S3CredEndpoint = "https://credentials.invalid/session"
	s.cfg.S3CredCAFile = filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(s.cfg.S3CredCAFile, []byte("not a certificate"), 0o600))
	require.ErrorContains(t, s.startCreds(t.Context()), "holds no certificate")
	_, err := os.Stat(s.cfg.stateSub(dirCreds + "/creds.sock"))
	require.ErrorIs(t, err, os.ErrNotExist, "source failure must precede opening the credential socket")
}
