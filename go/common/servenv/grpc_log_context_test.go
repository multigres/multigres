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

package servenv

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/multigres/multigres/go/tools/telemetry"
)

func TestShortMethodName(t *testing.T) {
	tests := map[string]string{
		"/multipoolermanager.MultiPoolerManager/Promote": "MultiPoolerManager/Promote",
		"/a.b.Service/Method":                            "Service/Method",
		"/Service/Method":                                "Service/Method",
		"/nomethod":                                      "nomethod",
	}
	for in, want := range tests {
		assert.Equal(t, want, shortMethodName(in), in)
	}
}

func TestLogContextUnaryInterceptor(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(telemetry.NewTelemetry().WrapSlogHandler(slog.NewJSONHandler(&buf, nil)))

	ctx := peer.NewContext(t.Context(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 54321},
	})
	info := &grpc.UnaryServerInfo{FullMethod: "/multipoolermanager.MultiPoolerManager/Promote"}

	_, err := logContextUnaryInterceptor(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
		logger.ErrorContext(ctx, "inside handler")
		return nil, nil
	})
	require.NoError(t, err)

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "MultiPoolerManager/Promote", rec["rpc"])
	assert.Equal(t, "10.1.2.3", rec["peer"])
}
