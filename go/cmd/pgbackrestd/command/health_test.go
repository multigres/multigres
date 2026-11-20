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
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
)

type statusStub struct {
	pgbackrestdserviceconnect.UnimplementedPgBackRestdHandler
	err error
}

func (s statusStub) Status(context.Context, *connect.Request[pgbackrestdservice.StatusRequest]) (*connect.Response[pgbackrestdservice.StatusResponse], error) {
	if s.err != nil {
		return nil, s.err
	}
	// Not configured, no server, S3 unreachable: none of it matters to health.
	return connect.NewResponse(&pgbackrestdservice.StatusResponse{}), nil
}

func serveStatus(t *testing.T, h statusStub) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "c.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	mux := http.NewServeMux()
	mux.Handle(pgbackrestdserviceconnect.NewPgBackRestdHandler(h))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(ln) }() // returns http.ErrServerClosed on cleanup
	t.Cleanup(func() { srv.Close() })
	return sock
}

func runHealth(t *testing.T, args ...string) error {
	t.Helper()
	cmd := newHealthCmd()
	cmd.SetArgs(args)
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	return cmd.ExecuteContext(t.Context())
}

func TestHealth_OKWhenStatusAnswers(t *testing.T) {
	sock := serveStatus(t, statusStub{})
	require.NoError(t, runHealth(t, "--control-socket", sock))
}

func TestHealth_FailsWhenControlSocketIsDown(t *testing.T) {
	require.Error(t, runHealth(t, "--control-socket", filepath.Join(t.TempDir(), "none.sock")))
}

func TestHealth_FailsWhenStatusFails(t *testing.T) {
	sock := serveStatus(t, statusStub{err: connect.NewError(connect.CodeInternal, assert.AnError)})
	require.Error(t, runHealth(t, "--control-socket", sock))
}

func TestHealth_RejectsTLSCertDirFlag(t *testing.T) {
	sock := serveStatus(t, statusStub{})
	require.Error(t, runHealth(t, "--control-socket", sock, "--tls-cert-dir", t.TempDir()))
}
