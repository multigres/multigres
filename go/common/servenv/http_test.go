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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv/servenvtest"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestHTTPRegisterPprofProfile_GatedByAuthPlugin proves /debug/pprof/* is
// protected by whichever auth plugin SetAuthPlugin points at, while staying
// completely unauthenticated (today's behavior, unchanged) for every service
// that never calls SetAuthPlugin.
func TestHTTPRegisterPprofProfile_GatedByAuthPlugin(t *testing.T) {
	const validToken = "valid-token"

	newSE := func(authPlugin func() Authenticator) *ServEnv {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpPprof.Set(true)
		if authPlugin != nil {
			se.SetAuthPlugin(authPlugin)
		}
		se.HTTPRegisterPprofProfile()
		return se
	}

	get := func(se *ServEnv, path, authHeader string) int {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, req)
		return w.Code
	}

	t.Run("no auth plugin set - unauthenticated access unchanged", func(t *testing.T) {
		se := newSE(nil)
		require.Equal(t, http.StatusOK, get(se, "/debug/pprof/cmdline", ""))
	})

	t.Run("non-TokenVerifier plugin active (e.g. mtls) - fails closed", func(t *testing.T) {
		se := newSE(func() Authenticator { return &servenvtest.AuthenticatorWithoutVerify{} })
		require.Equal(t, http.StatusUnauthorized, get(se, "/debug/pprof/cmdline", ""))
	})

	t.Run("jwt plugin active, missing token rejected", func(t *testing.T) {
		se := newSE(func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		require.Equal(t, http.StatusUnauthorized, get(se, "/debug/pprof/cmdline", ""))
	})

	t.Run("jwt plugin active, valid token accepted", func(t *testing.T) {
		se := newSE(func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		require.Equal(t, http.StatusOK, get(se, "/debug/pprof/cmdline", "Bearer "+validToken))
	})

	t.Run("jwt plugin active, all five endpoints gated", func(t *testing.T) {
		se := newSE(func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/profile", "/debug/pprof/symbol", "/debug/pprof/trace"} {
			require.Equal(t, http.StatusUnauthorized, get(se, path, ""), "path %s should be gated", path)
		}
	})

	t.Run("--pprof-http=false still disables registration entirely, regardless of auth plugin", func(t *testing.T) {
		se := NewServEnv(viperutil.NewRegistry())
		se.httpPprof.Set(false)
		se.SetAuthPlugin(func() Authenticator { return &servenvtest.FakeTokenVerifier{ValidToken: validToken} })
		se.HTTPRegisterPprofProfile()
		require.Equal(t, http.StatusNotFound, get(se, "/debug/pprof/cmdline", ""))
	})
}
