// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/client"
)

// TestSCRAMPlus_ClientRoundTrip_Require stands up the real server over
// direct TLS and connects with the production client configured for
// channel_binding=require. Because require fails the connection unless
// SCRAM-SHA-256-PLUS is actually negotiated, a successful Connect proves the
// client computed the tls-server-end-point binding from the server's cert and
// completed the PLUS handshake end to end.
func TestSCRAMPlus_ClientRoundTrip_Require(t *testing.T) {
	serverTLS, caPool := generateTestTLSConfig(t)
	ln, errCh := startDirectTLSServer(t, serverTLS, nil)

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := &client.Config{
		Host:           host,
		Port:           port,
		User:           "tlsuser",
		Password:       "postgres",
		Database:       "tlsdb",
		SSLMode:        client.SSLModeVerifyFull,
		SSLNegotiation: client.SSLNegotiationDirect,
		TLSConfig: &tls.Config{
			RootCAs:    caPool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		},
		ChannelBinding: client.ChannelBindingRequire,
		DialTimeout:    5 * time.Second,
	}

	conn, err := client.Connect(ctx, ctx, cfg)
	require.NoError(t, err, "client should negotiate SCRAM-SHA-256-PLUS and authenticate")
	t.Cleanup(func() { _ = conn.Close() })

	require.NoError(t, <-errCh, "server-side startup should succeed")
}
