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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiadminconnect "github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
)

func newTestAdapter(t *testing.T) *connectAdapter {
	t.Helper()
	ts := memorytopo.NewServer(t.Context())
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	srv := NewMultiAdminServer(ts, logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	path, handler := newConnectHandler(newTestServer(t))
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	client := multiadminconnect.NewMultiAdminServiceClient(httpSrv.Client(), httpSrv.URL)
	_, err := client.GetCell(t.Context(), connect.NewRequest(&multiadminpb.GetCellRequest{Name: "missing"}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err),
		"gRPC NotFound must survive serialization through the Connect handler")
}

func TestConnectAdapterGetDatabaseNames(t *testing.T) {
	adapter := newTestAdapter(t)
	resp, err := adapter.GetDatabaseNames(t.Context(), connect.NewRequest(&multiadminpb.GetDatabaseNamesRequest{}))
	require.NoError(t, err)
	assert.NotNil(t, resp.Msg)
	assert.Empty(t, resp.Msg.Names)
}
