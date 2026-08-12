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
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-jwt/jwt/v5"
)

var (
	_ Authenticator = (*MTLSOrJWTAuthPlugin)(nil)
	_ TokenVerifier = (*MTLSOrJWTAuthPlugin)(nil)
)

// MTLSOrJWTAuthPlugin lets a caller authenticate via *either* a trusted
// client certificate or a valid JWT bearer token - whichever it holds -
// rather than requiring both. It exists for internal callers (e.g. an
// operator polling pooler readiness across many clusters) that want to
// avoid building JWT-refresh machinery by using a long-lived client
// certificate instead.
//
// Two properties are deliberate and must not be relaxed:
//   - The certificate check validates a specific caller identity (via the
//     mtls plugin's existing allowed-substrings match), not just that the
//     certificate chains to a trusted CA - otherwise anything else holding a
//     certificate from the same CA (e.g. another internal component sharing
//     the same InternalTLS-issued CA) would get the same access.
//   - A successful certificate check grants access to *every* RPC (there is
//     no per-action authorization anywhere in this plugin registry today),
//     so the allowed-substrings list configured for this mode should be
//     scoped no more broadly than intended.
//
// This only ever applies to the native gRPC port: peer certificate
// information is only available on gRPC connections (see the mtls plugin's
// use of peer.FromContext). Multiadmin's HTTP/Connect/REST surface has no
// TLS support at all, so callers on that surface always go through the JWT
// check (see VerifyToken below).
type MTLSOrJWTAuthPlugin struct {
	mtls        certChecker
	jwt         Authenticator
	jwtVerifier TokenVerifier
	metrics     *authMetrics
}

// certChecker is implemented by MtlsAuthPlugin. MTLSOrJWTAuthPlugin uses
// this instead of calling MtlsAuthPlugin.Authenticate directly - see
// checkCert's doc comment for why: a cert miss here is an expected,
// silent fallthrough to the JWT check, not a rejection worth logging or
// counting.
type certChecker interface {
	checkCert(ctx context.Context) (context.Context, string)
}

// Authenticate implements Authenticator. The certificate check is tried
// first - it's already-available connection state, no cryptographic work
// beyond what the TLS handshake already did, cheaper than JWT verification -
// and, per the type's doc comment, its success grants access outright
// without a JWT ever being checked.
//
// The certificate check is deliberately silent on a miss: it calls
// checkCert directly rather than MtlsAuthPlugin.Authenticate, so a
// token-only caller doesn't produce a logged "rejection" and a failure
// metric for the cert check it was never going to pass, on every single
// request. Only a cert *success* is reported here; the JWT fallthrough
// reports its own outcome via jwt.Authenticate.
func (p *MTLSOrJWTAuthPlugin) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	if newCtx, outcome := p.mtls.checkCert(ctx); outcome == AuthOutcomeSuccess {
		p.metrics.record(ctx, "mtls", AuthOutcomeSuccess)
		return newCtx, nil
	}
	return p.jwt.Authenticate(ctx, fullMethod)
}

// VerifyToken implements TokenVerifier, delegating to the underlying JWT
// plugin. There is no certificate-based equivalent on non-gRPC transports,
// so this is the only check available to HTTP/Connect/REST callers when
// this mode is active.
func (p *MTLSOrJWTAuthPlugin) VerifyToken(tokenString string) (jwt.MapClaims, error) {
	return p.jwtVerifier.VerifyToken(tokenString)
}

// mtlsOrJWTAuthPluginInitializer builds both sub-plugins through the
// existing registry rather than duplicating their config/validation logic.
// This also means no new flags: --grpc-auth-mtls-allowed-substrings and the
// --grpc-auth-jwt-* flags (already registered unconditionally by their own
// plugins) fully configure this mode.
func mtlsOrJWTAuthPluginInitializer() (Authenticator, error) {
	mtlsInit, err := GetAuthenticator("mtls")
	if err != nil {
		return nil, fmt.Errorf("mtls-or-jwt: %w", err)
	}
	mtlsPlugin, err := mtlsInit()
	if err != nil {
		return nil, fmt.Errorf("mtls-or-jwt: initialize mtls sub-plugin: %w", err)
	}
	mtlsChecker, ok := mtlsPlugin.(certChecker)
	if !ok {
		// Unreachable in practice: the mtls plugin always implements
		// certChecker. Guarded explicitly so a future refactor that broke
		// this fails loudly at startup instead of via a confusing panic.
		return nil, errors.New("mtls-or-jwt: mtls sub-plugin does not implement certChecker")
	}

	jwtInit, err := GetAuthenticator("jwt")
	if err != nil {
		return nil, fmt.Errorf("mtls-or-jwt: %w", err)
	}
	jwtPlugin, err := jwtInit()
	if err != nil {
		return nil, fmt.Errorf("mtls-or-jwt: initialize jwt sub-plugin: %w", err)
	}
	jwtVerifier, ok := jwtPlugin.(TokenVerifier)
	if !ok {
		// Unreachable in practice: the jwt plugin always implements
		// TokenVerifier. Guarded explicitly so a future refactor that broke
		// this fails loudly at startup instead of via a confusing panic.
		return nil, errors.New("mtls-or-jwt: jwt sub-plugin does not implement TokenVerifier")
	}

	slog.Info("mtls-or-jwt auth plugin initialized successfully")
	return &MTLSOrJWTAuthPlugin{
		mtls:        mtlsChecker,
		jwt:         jwtPlugin,
		jwtVerifier: jwtVerifier,
		metrics:     newAuthMetrics(),
	}, nil
}

func init() {
	if err := RegisterAuthPlugin("mtls-or-jwt", mtlsOrJWTAuthPluginInitializer); err != nil {
		slog.Error("failed to register mtls-or-jwt auth plugin", "error", err)
		panic(fmt.Sprintf("failed to register mtls-or-jwt auth plugin: %v", err))
	}
}
