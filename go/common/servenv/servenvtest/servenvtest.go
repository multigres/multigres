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

// Package servenvtest provides shared test doubles for servenv's
// Authenticator/TokenVerifier interfaces, used both by servenv's own tests
// and by packages that wire servenv's auth plugins into their own routes
// (e.g. go/services/multiadmin), so both sides exercise the same fakes
// instead of maintaining independent, driftable copies.
package servenvtest

import (
	"context"
	"errors"

	"github.com/golang-jwt/jwt/v5"
)

// FakeTokenVerifier implements both servenv.Authenticator and
// servenv.TokenVerifier, standing in for a real JWT plugin in tests that
// only need to exercise the type-assertion-gated wiring around
// servenv.AuthenticateBearer/RequireBearerAuth, without needing real
// JWT/JWKS machinery (that's covered separately in
// grpc_server_auth_jwt_test.go).
type FakeTokenVerifier struct {
	ValidToken string
}

func (f *FakeTokenVerifier) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	return ctx, nil
}

func (f *FakeTokenVerifier) VerifyToken(token string) (jwt.MapClaims, error) {
	if token != f.ValidToken {
		return nil, errors.New("invalid token")
	}
	return jwt.MapClaims{}, nil
}

// AuthenticatorWithoutVerify implements servenv.Authenticator but NOT
// servenv.TokenVerifier, standing in for a real, unrelated auth plugin (e.g.
// mtls) so tests can confirm the type assertion - not just a nil check -
// gates JWT-specific checks.
type AuthenticatorWithoutVerify struct{}

func (a *AuthenticatorWithoutVerify) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	return ctx, nil
}
