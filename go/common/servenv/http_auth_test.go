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

package servenv

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv/servenvtest"
)

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		wantToken string
		wantErr   bool
	}{
		{name: "valid bearer token", header: "Bearer abc123", wantToken: "abc123"},
		{name: "case-insensitive scheme", header: "bearer abc123", wantToken: "abc123"},
		{name: "missing header", header: "", wantErr: true},
		{name: "wrong scheme", header: "Basic dXNlcjpwYXNz", wantErr: true},
		{name: "bearer with no token", header: "Bearer ", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := ExtractBearerToken(tt.header)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantToken, token)
			}
		})
	}
}

func TestRequireBearerAuth(t *testing.T) {
	const validToken = "valid-token"
	called := false
	next := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}

	tests := []struct {
		name       string
		authPlugin func() Authenticator
		header     string
		wantStatus int
		wantCalled bool
	}{
		{
			name:       "no auth plugin active - default off, passes through",
			authPlugin: func() Authenticator { return nil },
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name:       "non-TokenVerifier plugin active (e.g. mtls) - fails closed",
			authPlugin: func() Authenticator { return &servenvtest.AuthenticatorWithoutVerify{} },
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "plugin active, missing header rejected",
			authPlugin: func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} },
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "plugin active, malformed header rejected",
			authPlugin: func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} },
			header:     "Basic dXNlcjpwYXNz",
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "plugin active, invalid token rejected",
			authPlugin: func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} },
			header:     "Bearer wrong-token",
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "plugin active, valid token accepted",
			authPlugin: func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} },
			header:     "Bearer " + validToken,
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			handler := RequireBearerAuth(tt.authPlugin, next)

			req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			handler(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCalled, called)
			if tt.wantStatus == http.StatusUnauthorized {
				// Every rejection reason (missing header, malformed header,
				// invalid token, ...) must produce the exact same body - see
				// AuthenticateBearer's use of errAuthenticationFailed. If a
				// caller could distinguish these, it could use the response
				// as an oracle to probe why a request was rejected.
				assert.Equal(t, authFailedMessage+"\n", rec.Body.String())
			}
		})
	}
}

func TestClientCertAuthorized(t *testing.T) {
	certA := generateTestPeerCert(t, "client-a")
	certB := generateTestPeerCert(t, "client-b")

	tests := []struct {
		name       string
		tlsState   *tls.ConnectionState
		substrings []string
		want       bool
	}{
		{name: "no TLS at all", tlsState: nil, substrings: []string{"client-a"}, want: false},
		{
			name:       "leaf matches",
			tlsState:   &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certA}}},
			substrings: []string{"client-a"},
			want:       true,
		},
		{
			name:       "leaf does not match",
			tlsState:   &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certB}}},
			substrings: []string{"client-a"},
			want:       false,
		},
		{
			name:       "TLS present but no verified chains (e.g. RequestClientCert, no cert offered)",
			tlsState:   &tls.ConnectionState{VerifiedChains: nil},
			substrings: []string{"client-a"},
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.TLS = tt.tlsState
			assert.Equal(t, tt.want, clientCertAuthorized(req, tt.substrings))
		})
	}
}

func TestRequireClientCert(t *testing.T) {
	certA := generateTestPeerCert(t, "client-a")
	certB := generateTestPeerCert(t, "client-b")

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	request := func(path string, tlsState *tls.ConnectionState) *http.Request {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.TLS = tlsState
		return req
	}

	tests := []struct {
		name       string
		path       string
		tlsState   *tls.ConnectionState
		wantStatus int
		wantCalled bool
	}{
		{
			name:       "accepted cert",
			path:       "/proxy/",
			tlsState:   &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certA}}},
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
		{
			name:       "rejected cert",
			path:       "/proxy/",
			tlsState:   &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{certB}}},
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "no cert presented at all",
			path:       "/proxy/",
			tlsState:   nil,
			wantStatus: http.StatusUnauthorized,
			wantCalled: false,
		},
		{
			name:       "exempt path reached with no cert",
			path:       "/live",
			tlsState:   nil,
			wantStatus: http.StatusOK,
			wantCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			handler := requireClientCert([]string{"client-a"}, next)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, request(tt.path, tt.tlsState))

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}
