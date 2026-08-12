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
	"errors"
	"log/slog"
	"net/http"
	"strings"
)

// authFailedMessage is the only detail ever returned to an unauthenticated
// caller, across every transport (gRPC, HTTP, Connect). The real reason
// (missing header, expired token, unauthorized subject, ...) is logged
// server-side instead - returning it to the caller would let an attacker
// distinguish, say, "wrong signature" from "valid signature, unauthorized
// subject" and use that as an oracle to probe for valid credentials.
const authFailedMessage = "authentication failed"

// errAuthenticationFailed is returned by AuthenticateBearer, and therefore by
// every HTTP/Connect caller of it, whenever a request is rejected.
var errAuthenticationFailed = errors.New(authFailedMessage)

// ExtractBearerToken extracts the token from an HTTP `Authorization` header
// value of the form "Bearer <token>" (case-insensitive scheme). Shared by
// every plain-HTTP and Connect-protocol call site that needs to pull a
// bearer token off a request, so they all apply identical parsing.
func ExtractBearerToken(header string) (string, error) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", errors.New("authorization header is not a bearer token")
	}
	return header[len(prefix):], nil
}

// AuthenticateBearer is the transport-agnostic core of bearer-token gating,
// shared by RequireBearerAuth below (net/http) and Multiadmin's Connect
// interceptor (go/services/multiadmin/server_connect.go), which differ only
// in how they read a header off their respective request types and how they
// report a failure. It reports (true, nil) if no auth mode is configured at
// all (nothing to check - see the accessor-timing note on
// GrpcServer.AuthPlugin, which callers must respect here too) or if the
// supplied header carries a valid token; otherwise (false, err) with err
// describing why the request is rejected.
//
// An auth mode that IS configured but doesn't implement TokenVerifier (e.g.
// --grpc-auth-mode=mtls, which has no HTTP/Connect equivalent) fails closed
// here rather than passing every request through - an operator who
// explicitly turned auth on for this transport should get "rejected", not
// silently no-op'd. resolveAuthPlugin (grpc_server.go) logs a one-time
// warning at startup when this case is active.
func AuthenticateBearer(authPlugin func() Authenticator, authorizationHeader string) (bool, error) {
	plugin := authPlugin()
	if plugin == nil {
		return true, nil
	}
	verifier, ok := plugin.(TokenVerifier)
	if !ok {
		slog.Warn("bearer auth: rejected request", "reason", "active auth plugin does not support HTTP/Connect token verification")
		return false, errAuthenticationFailed
	}
	token, err := ExtractBearerToken(authorizationHeader)
	if err != nil {
		slog.Warn("bearer auth: rejected request", "reason", err)
		return false, errAuthenticationFailed
	}
	if _, err := verifier.VerifyToken(token); err != nil {
		slog.Warn("bearer auth: rejected request", "reason", err)
		return false, errAuthenticationFailed
	}
	return true, nil
}

// RequireBearerAuth wraps a plain net/http handler with AuthenticateBearer.
// See GrpcServer.AuthPlugin for why authPlugin must be an accessor, called
// fresh on every request, rather than a value resolved once at wiring time.
func RequireBearerAuth(authPlugin func() Authenticator, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ok, err := AuthenticateBearer(authPlugin, r.Header.Get("Authorization")); !ok {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}
