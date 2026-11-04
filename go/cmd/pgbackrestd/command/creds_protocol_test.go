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

package command

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func credentialSocket(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	// Keep AF_UNIX paths below the Darwin limit as well as Linux's.
	dir, err := os.MkdirTemp("/tmp", "creds-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	require.NoError(t, os.Mkdir(filepath.Join(dir, "creds"), 0o700))
	ln, err := net.Listen("unix", filepath.Join(dir, "creds", "creds.sock"))
	require.NoError(t, err)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	return dir
}

func TestCredsRootWritesOnlyCredentialJSON(t *testing.T) {
	const body = `{"access_key":"fixture-key","secret_key":"fixture-secret","token":"fixture-session"}`
	requests := make(chan *http.Request, 1)
	dir := credentialSocket(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(context.Background())
		_, _ = io.WriteString(w, body)
	})
	t.Setenv("PGBACKRESTD_CREDS_TOKEN", "fixture-capability")
	root, logger := GetRootCommand()
	require.NotNil(t, logger)
	var stdout, stderr bytes.Buffer
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetArgs([]string{"creds", "--state-dir", dir})
	require.NoError(t, root.ExecuteContext(t.Context()))
	require.False(t, root.SilenceUsage, "credential command must bypass the root logging hook")
	require.Equal(t, body, stdout.String(), "logging or Cobra output would corrupt pgBackRest's JSON")
	require.Empty(t, stderr.String())
	request := <-requests
	require.Equal(t, http.MethodGet, request.Method)
	require.Equal(t, "/creds", request.URL.Path)
	require.Equal(t, "fixture-capability", request.Header.Get("X-Pgbackrestd-Token"))
}

func TestFetchCredsFailsClosed(t *testing.T) {
	t.Run("missing token never dials", func(t *testing.T) {
		body, err := fetchCreds(t.Context(), t.TempDir(), "")
		require.ErrorContains(t, err, "PGBACKRESTD_CREDS_TOKEN is not set")
		require.Nil(t, body)
	})
	t.Run("missing socket", func(t *testing.T) {
		body, err := fetchCreds(t.Context(), t.TempDir(), "fixture")
		require.Error(t, err)
		require.Nil(t, body)
	})
	t.Run("non-success body is not forwarded", func(t *testing.T) {
		dir := credentialSocket(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "private diagnostic must not be propagated")
		})
		body, err := fetchCreds(t.Context(), dir, "fixture")
		require.ErrorContains(t, err, "403 Forbidden")
		require.NotContains(t, err.Error(), "private diagnostic")
		require.Nil(t, body)
	})
	t.Run("response is bounded", func(t *testing.T) {
		dir := credentialSocket(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat("x", (1<<20)+1))
		})
		body, err := fetchCreds(t.Context(), dir, "fixture")
		require.NoError(t, err)
		require.Len(t, body, 1<<20)
	})
	t.Run("cancelled request", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		body, err := fetchCreds(ctx, t.TempDir(), "fixture")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, body)
	})
}

func TestCredsFailureExitContract(t *testing.T) {
	if os.Getenv("PGBACKRESTD_TEST_CREDS_EXIT") == "1" {
		root, _ := GetRootCommand()
		root.SetArgs([]string{"creds"})
		_ = root.ExecuteContext(t.Context())
		return
	}
	t.Setenv("PGBACKRESTD_TEST_CREDS_EXIT", "1")
	t.Setenv("PGBACKRESTD_CREDS_TOKEN", "")
	child := executil.Command(t.Context(), os.Args[0], "-test.run=^TestCredsFailureExitContract$")
	var stdout, stderr bytes.Buffer
	child.Stdout, child.Stderr = &stdout, &stderr
	require.NoError(t, child.Start())
	err := child.Wait()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.ExitCode())
	require.Empty(t, stdout.String())
	require.Equal(t, "pgbackrestd creds: PGBACKRESTD_CREDS_TOKEN is not set\n", stderr.String())
}
