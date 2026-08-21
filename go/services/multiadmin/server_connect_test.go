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

package multiadmin

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/vanguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/servenv/servenvtest"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiadminconnect "github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
)

func newTestAdapter(t *testing.T) *connectAdapter {
	t.Helper()
	ts := memorytopo.NewServer(t.Context())
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	srv := NewMultiadminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return &connectAdapter{srv}
}

func TestConnectAdapterGetCellNames(t *testing.T) {
	adapter := newTestAdapter(t)
	resp, err := adapter.GetCellNames(t.Context(), connect.NewRequest(&multiadminpb.GetCellNamesRequest{}))
	require.NoError(t, err)
	assert.NotNil(t, resp.Msg)
	assert.Empty(t, resp.Msg.Names)
}

func TestConnectAdapterGetCellNotFound(t *testing.T) {
	adapter := newTestAdapter(t)
	_, err := adapter.GetCell(t.Context(), connect.NewRequest(&multiadminpb.GetCellRequest{Name: "missing"}))
	require.Error(t, err)
	// connect-go passes grpc status errors through transparently
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

// TestConnectHandlerPropagatesGRPCCode exercises the error path through the
// real Connect HTTP handler (not a direct adapter call). The backend returns a
// gRPC status error; connect-go does not understand those, so without explicit
// translation it serializes them as CodeUnknown (HTTP 500), masking NotFound,
// InvalidArgument, etc. This guards against that regression.
func TestConnectHandlerPropagatesGRPCCode(t *testing.T) {
	path, handler := newConnectHandler(newTestServer(t), func() servenv.Authenticator { return nil })
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	client := multiadminconnect.NewMultiadminServiceClient(httpSrv.Client(), httpSrv.URL)
	_, err := client.GetCell(t.Context(), connect.NewRequest(&multiadminpb.GetCellRequest{Name: "missing"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
		"gRPC NotFound must survive serialization through the Connect handler")
}

// authFailedMessage mirrors the unexported servenv.authFailedMessage - it
// can't be referenced directly across packages, so the literal is duplicated
// here. Every auth rejection, regardless of transport or reason, must
// produce exactly this message; see servenv.AuthenticateBearer.
const authFailedMessage = "authentication failed"

// assertGenericAuthFailedMessage checks that a rejection's message is the
// sanitized, generic string - never anything that would let a caller
// distinguish *why* a request was rejected (missing header vs. bad token vs.
// ...), which could otherwise be used as an oracle to probe valid credentials.
func assertGenericAuthFailedMessage(t *testing.T, err error) {
	t.Helper()
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Equal(t, authFailedMessage, connectErr.Message())
}

// TestConnectHandlerJWTAuth exercises newJWTConnectInterceptor through the
// real Connect HTTP handler, covering both the Connect protocol path and the
// REST (Vanguard-transcoded) path, since both are expected to share the same
// auth behavior (they wrap the same underlying handler).
func TestConnectHandlerJWTAuth(t *testing.T) {
	const validToken = "valid-token"

	newHandler := func(authPlugin func() servenv.Authenticator) (*httptest.Server, multiadminconnect.MultiadminServiceClient) {
		path, handler := newConnectHandler(newTestServer(t), authPlugin)
		mux := http.NewServeMux()
		mux.Handle(path, handler)

		transcoder, err := vanguard.NewTranscoder([]*vanguard.Service{vanguard.NewService(path, handler)})
		require.NoError(t, err)
		mux.Handle("/api/", transcoder)

		httpSrv := httptest.NewServer(mux)
		t.Cleanup(httpSrv.Close)
		client := multiadminconnect.NewMultiadminServiceClient(httpSrv.Client(), httpSrv.URL)
		return httpSrv, client
	}

	t.Run("no auth plugin active passes through unauthenticated", func(t *testing.T) {
		_, client := newHandler(func() servenv.Authenticator { return nil })
		_, err := client.GetCellNames(t.Context(), connect.NewRequest(&multiadminpb.GetCellNamesRequest{}))
		require.NoError(t, err)
	})

	t.Run("non-TokenVerifier plugin active (e.g. mtls) fails closed", func(t *testing.T) {
		_, client := newHandler(func() servenv.Authenticator { return &servenvtest.AuthenticatorWithoutVerify{} })
		_, err := client.GetCellNames(t.Context(), connect.NewRequest(&multiadminpb.GetCellNamesRequest{}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		assertGenericAuthFailedMessage(t, err)
	})

	t.Run("missing authorization header rejected", func(t *testing.T) {
		_, client := newHandler(func() servenv.Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		_, err := client.GetCellNames(t.Context(), connect.NewRequest(&multiadminpb.GetCellNamesRequest{}))
		require.Error(t, err)
		assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		// Must match the invalid-token case below exactly - see the comment
		// there for why.
		assertGenericAuthFailedMessage(t, err)
	})

	t.Run("invalid token rejected", func(t *testing.T) {
		_, client := newHandler(func() servenv.Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		req := connect.NewRequest(&multiadminpb.GetCellNamesRequest{})
		req.Header().Set("Authorization", "Bearer wrong-token")
		_, err := client.GetCellNames(t.Context(), req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		// Must produce the exact same message as the missing-header case
		// above - see servenv.AuthenticateBearer's use of a single sentinel
		// error. If a caller could distinguish "no header" from "bad token"
		// from the response, it could use that as an oracle to probe why a
		// request was rejected.
		assertGenericAuthFailedMessage(t, err)
	})

	t.Run("valid token accepted", func(t *testing.T) {
		_, client := newHandler(func() servenv.Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		req := connect.NewRequest(&multiadminpb.GetCellNamesRequest{})
		req.Header().Set("Authorization", "Bearer "+validToken)
		_, err := client.GetCellNames(t.Context(), req)
		require.NoError(t, err)
	})

	t.Run("REST transcoded path enforces the same auth", func(t *testing.T) {
		httpSrv, _ := newHandler(func() servenv.Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		resp, err := httpSrv.Client().Get(httpSrv.URL + "/api/v1/cells")
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		// Same sanitized message as the Connect-protocol cases above -
		// Vanguard transcodes the same underlying connect.Error into this
		// JSON body.
		assert.Contains(t, string(body), authFailedMessage)
	})
}

func TestHTTPAuthPluginRequiresEnableAuth(t *testing.T) {
	ma := NewMultiadmin()
	ma.grpcServer.SetAuthMode("mtls")

	assert.Nil(t, ma.httpAuthPlugin(),
		"native gRPC auth must not enable authentication on HTTP surfaces")

	ma.enableAuth.Set(true)
	assert.NotNil(t, ma.httpAuthPlugin(),
		"--enable-auth must fail closed until the JWT plugin is resolved")
}

func TestConnectAdapterGetDatabaseNames(t *testing.T) {
	adapter := newTestAdapter(t)
	resp, err := adapter.GetDatabaseNames(t.Context(), connect.NewRequest(&multiadminpb.GetDatabaseNamesRequest{}))
	require.NoError(t, err)
	assert.NotNil(t, resp.Msg)
	assert.Empty(t, resp.Msg.Names)
}
