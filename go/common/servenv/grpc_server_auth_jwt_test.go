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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MicahParks/jwkset"
	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testJWTIssuer = "https://issuer.example.com"

// testJWKSet holds a single RSA keypair plus the marshaled JWKS JSON for it,
// used to build a static (non-network) keyfunc.Keyfunc for VerifyToken tests
// and to serve a real JWKS endpoint for the end-to-end initializer test.
type testJWKSet struct {
	privateKey *rsa.PrivateKey
	kid        string
	jwksJSON   json.RawMessage
}

func newTestJWKSet(t *testing.T) *testJWKSet {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	const kid = "test-key-1"
	jwk, err := jwkset.NewJWKFromKey(key.Public(), jwkset.JWKOptions{
		Metadata: jwkset.JWKMetadataOptions{KID: kid},
	})
	require.NoError(t, err)

	set := jwkset.JWKSMarshal{Keys: []jwkset.JWKMarshal{jwk.Marshal()}}
	raw, err := json.Marshal(set)
	require.NoError(t, err)

	return &testJWKSet{privateKey: key, kid: kid, jwksJSON: raw}
}

// staticVerifier builds a JWTAuthPlugin backed by a static (in-memory) JWKS,
// with no background refresh goroutine and no network calls, for pure
// claims-validation tests.
func (s *testJWKSet) staticVerifier(t *testing.T, issuer, audience string, allowedSubs ...string) *JWTAuthPlugin {
	t.Helper()
	kf, err := keyfunc.NewJWKSetJSON(s.jwksJSON)
	require.NoError(t, err)

	subs := make(map[string]struct{})
	for _, s := range allowedSubs {
		subs[s] = struct{}{}
	}
	return &JWTAuthPlugin{keyfunc: kf, issuer: issuer, audience: audience, allowedSubs: subs}
}

type testClaimOpt func(jwt.MapClaims)

func withSub(sub string) testClaimOpt { return func(c jwt.MapClaims) { c["sub"] = sub } }
func withAud(aud string) testClaimOpt { return func(c jwt.MapClaims) { c["aud"] = aud } }
func withIssuer(iss string) testClaimOpt {
	return func(c jwt.MapClaims) { c["iss"] = iss }
}

func withExpiry(t time.Time) testClaimOpt {
	return func(c jwt.MapClaims) { c["exp"] = t.Unix() }
}
func withoutExpiry() testClaimOpt { return func(c jwt.MapClaims) { delete(c, "exp") } }
func withNotBefore(t time.Time) testClaimOpt {
	return func(c jwt.MapClaims) { c["nbf"] = t.Unix() }
}

// signToken builds and signs a JWT with sensible defaults (valid issuer,
// subject, and a 5-minute expiry), overridable via opts, using the given
// signing method and key. Passing a mismatched method/key combination is how
// the "wrong algorithm" / "bad signature" test cases are constructed.
func signToken(t *testing.T, method jwt.SigningMethod, key any, kid string, opts ...testClaimOpt) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss": testJWTIssuer,
		"sub": "test-subject",
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	}
	for _, opt := range opts {
		opt(claims)
	}
	token := jwt.NewWithClaims(method, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

func TestJWTAuthPluginInitializer(t *testing.T) {
	origIssuer, origJWKSURI, origSubs, origAud := jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience
	origBudget, origBaseDelay, origMaxDelay := jwtJWKSStartupRetryBudget, jwtJWKSRetryBaseDelay, jwtJWKSRetryMaxDelay
	t.Cleanup(func() {
		jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience = origIssuer, origJWKSURI, origSubs, origAud
		jwtJWKSStartupRetryBudget, jwtJWKSRetryBaseDelay, jwtJWKSRetryMaxDelay = origBudget, origBaseDelay, origMaxDelay
	})
	// Shrink the retry budget so the "unreachable jwks uri" case below
	// exercises the same give-up-eventually code path without taking a
	// real 60s to run.
	jwtJWKSStartupRetryBudget = 50 * time.Millisecond
	jwtJWKSRetryBaseDelay = 1 * time.Millisecond
	jwtJWKSRetryMaxDelay = 5 * time.Millisecond

	jwks := newTestJWKSet(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(srv.Close)

	tests := []struct {
		name    string
		issuer  string
		jwksURI string
		subs    []string
		wantErr string
	}{
		{name: "missing issuer rejected", issuer: "", jwksURI: srv.URL, wantErr: "grpc-auth-jwt-issuer"},
		{name: "missing jwks uri rejected", issuer: testJWTIssuer, jwksURI: "", wantErr: "grpc-auth-jwt-jwks-uri"},
		{name: "empty sub entry rejected", issuer: testJWTIssuer, jwksURI: srv.URL, subs: []string{"a", ""}, wantErr: "grpc-auth-jwt-allowed-subs"},
		{name: "unreachable jwks uri fails closed", issuer: testJWTIssuer, jwksURI: "http://127.0.0.1:0", wantErr: "failed to initialize JWKS client"},
		{name: "valid config", issuer: testJWTIssuer, jwksURI: srv.URL, wantErr: ""},
		{
			name:    "ARN-shaped subjects (containing colons) accepted",
			issuer:  testJWTIssuer,
			jwksURI: srv.URL,
			subs:    []string{"arn:aws:iam::436098097459:role/aws-reserved/sso.amazonaws.com/ap-southeast-1/Example"},
			wantErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience = tt.issuer, tt.jwksURI, tt.subs, ""

			auth, err := jwtAuthPluginInitializer()
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, auth)
			}
		})
	}
}

// TestJWTAuthPluginInitializer_RetriesTransientFailure proves the retry loop
// added to ride out a brief JWKS outage at startup actually rides one out,
// rather than just giving the retry-exhaustion path (already covered by the
// "unreachable jwks uri" case above) something new to fail through.
func TestJWTAuthPluginInitializer_RetriesTransientFailure(t *testing.T) {
	origIssuer, origJWKSURI, origSubs, origAud := jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience
	origBudget, origBaseDelay, origMaxDelay := jwtJWKSStartupRetryBudget, jwtJWKSRetryBaseDelay, jwtJWKSRetryMaxDelay
	t.Cleanup(func() {
		jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience = origIssuer, origJWKSURI, origSubs, origAud
		jwtJWKSStartupRetryBudget, jwtJWKSRetryBaseDelay, jwtJWKSRetryMaxDelay = origBudget, origBaseDelay, origMaxDelay
	})
	jwtJWKSStartupRetryBudget = 5 * time.Second
	jwtJWKSRetryBaseDelay = 1 * time.Millisecond
	jwtJWKSRetryMaxDelay = 5 * time.Millisecond

	jwks := newTestJWKSet(t)
	const failuresBeforeSuccess = 3
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) <= failuresBeforeSuccess {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks.jwksJSON)
	}))
	t.Cleanup(srv.Close)

	jwtIssuer, jwtJWKSURI, jwtAllowedSubs, jwtAudience = testJWTIssuer, srv.URL, nil, ""
	auth, err := jwtAuthPluginInitializer()
	require.NoError(t, err, "should retry past transient failures and eventually succeed")
	require.NotNil(t, auth)
	assert.Greater(t, requests.Load(), int32(failuresBeforeSuccess), "must have actually retried, not just gotten lucky on request 1")
}

func TestJWTAuthPlugin_VerifyToken(t *testing.T) {
	jwks := newTestJWKSet(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name       string
		plugin     func(t *testing.T) *JWTAuthPlugin
		token      func(t *testing.T) string
		wantErrSub string
	}{
		{
			name:   "valid token accepted",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token:  func(t *testing.T) string { return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid) },
		},
		{
			name:   "expired token rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withExpiry(time.Now().Add(-time.Hour)))
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "missing exp claim rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withoutExpiry())
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "future nbf rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withNotBefore(time.Now().Add(time.Hour)))
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "wrong issuer rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withIssuer("https://someone-else.example.com"))
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "subject not in allow-list rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "", "allowed-subject") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withSub("someone-else"))
			},
			wantErrSub: "not authorized",
		},
		{
			name:   "empty allow-list accepts any subject",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withSub("whoever"))
			},
		},
		{
			name:   "subject in allow-list accepted",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "", "allowed-subject") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withSub("allowed-subject"))
			},
		},
		{
			name:       "missing audience rejected when configured",
			plugin:     func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "expected-aud") },
			token:      func(t *testing.T) string { return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid) },
			wantErrSub: "invalid token",
		},
		{
			name:   "wrong audience rejected when configured",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "expected-aud") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withAud("someone-else"))
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "correct audience accepted when configured",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "expected-aud") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withAud("expected-aud"))
			},
		},
		{
			name:   "bad signature rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				// Signed by a different, unrelated key - the kid header
				// still points at the legitimate key in the JWKS, so this
				// exercises signature mismatch specifically.
				return signToken(t, jwt.SigningMethodRS256, otherKey, jwks.kid)
			},
			wantErrSub: "invalid token",
		},
		{
			name:   "unsigned 'none' algorithm rejected",
			plugin: func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token: func(t *testing.T) string {
				return signToken(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, jwks.kid)
			},
			wantErrSub: "invalid token",
		},
		{
			name:       "malformed token rejected",
			plugin:     func(t *testing.T) *JWTAuthPlugin { return jwks.staticVerifier(t, testJWTIssuer, "") },
			token:      func(t *testing.T) string { return "not-a-jwt" },
			wantErrSub: "invalid token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := tt.plugin(t)
			claims, err := plugin.VerifyToken(tt.token(t))
			if tt.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrSub)
			} else {
				require.NoError(t, err)
				require.NotNil(t, claims)
			}
		})
	}
}

func TestJWTAuthPlugin_Authenticate(t *testing.T) {
	jwks := newTestJWKSet(t)
	plugin := jwks.staticVerifier(t, testJWTIssuer, "")
	validToken := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)

	tests := []struct {
		name    string
		ctx     func() context.Context
		wantErr bool
	}{
		{
			name:    "no metadata in context rejected",
			ctx:     func() context.Context { return context.Background() },
			wantErr: true,
		},
		{
			name: "missing authorization header rejected",
			ctx: func() context.Context {
				return metadata.NewIncomingContext(context.Background(), metadata.MD{})
			},
			wantErr: true,
		},
		{
			name: "non-bearer scheme rejected",
			ctx: func() context.Context {
				return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic dXNlcjpwYXNz"))
			},
			wantErr: true,
		},
		{
			name: "valid bearer token accepted",
			ctx: func() context.Context {
				return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+validToken))
			},
		},
		{
			name: "case-insensitive bearer scheme accepted",
			ctx: func() context.Context {
				return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bearer "+validToken))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := plugin.Authenticate(tt.ctx(), "/multiadmin.MultiadminService/GetCell")
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, codes.Unauthenticated, status.Code(err))
				// The error returned to callers must never leak *why*
				// authentication failed (missing header vs. bad token vs.
				// ...) - see JWTAuthPlugin.Authenticate's use of
				// errJWTAuthFailed.
				assert.Equal(t, authFailedMessage, status.Convert(err).Message())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
