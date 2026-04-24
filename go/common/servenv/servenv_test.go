// Copyright 2025 Supabase, Inc.
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestGenerateRandomServiceID(t *testing.T) {
	t.Run("generates 8 character string", func(t *testing.T) {
		id := GenerateRandomServiceID()
		require.Len(t, id, 8, "service ID should be 8 characters")
	})

	t.Run("uses valid characters", func(t *testing.T) {
		id := GenerateRandomServiceID()
		// stringutil.RandomString uses these characters (lowercase consonants + digits without confusables)
		validChars := "bcdfghjklmnpqrstvwxz2456789"
		for _, char := range id {
			require.Contains(t, validChars, string(char), "service ID should only contain valid characters")
		}
	})

	t.Run("generates unique IDs", func(t *testing.T) {
		seen := make(map[string]bool)
		for range 100 {
			id := GenerateRandomServiceID()
			require.False(t, seen[id], "should not generate duplicate IDs")
			seen[id] = true
		}
	})
}

func TestRegisterReadyCheck(t *testing.T) {
	newSE := func() *ServEnv {
		se := NewServEnv(viperutil.NewRegistry())
		se.RegisterCommonHTTPEndpoints()
		return se
	}

	t.Run("no checks returns 200", func(t *testing.T) {
		se := newSE()
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("failing check returns 503", func(t *testing.T) {
		se := newSE()
		se.RegisterReadyCheck(func() error { return errors.New("not ready") })
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})

	t.Run("first check fails short-circuits", func(t *testing.T) {
		se := newSE()
		secondCalled := false
		se.RegisterReadyCheck(func() error { return errors.New("first") })
		se.RegisterReadyCheck(func() error { secondCalled = true; return nil })
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.False(t, secondCalled)
	})

	t.Run("second check fails returns 503", func(t *testing.T) {
		se := newSE()
		se.RegisterReadyCheck(func() error { return nil })
		se.RegisterReadyCheck(func() error { return errors.New("second") })
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})

	t.Run("all checks pass returns 200", func(t *testing.T) {
		se := newSE()
		se.RegisterReadyCheck(func() error { return nil })
		se.RegisterReadyCheck(func() error { return nil })
		w := httptest.NewRecorder()
		se.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
		require.Equal(t, http.StatusOK, w.Code)
	})
}
