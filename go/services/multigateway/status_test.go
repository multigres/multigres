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

package multigateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"
)

func TestHandleQueriesDebugReturnsJSON(t *testing.T) {
	registry := queryregistry.NewForTest(queryregistry.DefaultConfig())
	defer registry.Close()

	// Populate a couple of fingerprints.
	registry.Record("fp1", "SELECT * FROM users WHERE id = $1", 2*time.Millisecond, 10, false)
	registry.Record("fp2", "SELECT * FROM orders WHERE user_id = $1", 5*time.Millisecond, 20, true)
	registry.Record("fp2", "SELECT * FROM orders WHERE user_id = $1", 5*time.Millisecond, 20, false)

	h := handler.NewMultiGatewayHandler(nil, slog.Default(), 0)
	h.SetQueryRegistry(registry)
	mg := &MultiGateway{pgHandler: h}

	req := httptest.NewRequest(http.MethodGet, "/debug/queries?limit=10&sort=calls&format=json", nil)
	rec := httptest.NewRecorder()
	mg.handleQueriesDebug(rec, req)

	resp := rec.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded struct {
		TrackedFingerprints int                      `json:"tracked_fingerprints"`
		Returned            int                      `json:"returned"`
		Queries             []queryregistry.Snapshot `json:"queries"`
	}
	require.NoError(t, json.Unmarshal(body, &decoded))

	assert.Equal(t, 2, decoded.TrackedFingerprints)
	assert.Equal(t, 2, decoded.Returned)
	require.Len(t, decoded.Queries, 2)
	// fp2 has 2 calls vs fp1's 1 call → fp2 first when sorted by calls.
	assert.Equal(t, "fp2", decoded.Queries[0].Fingerprint)
	assert.Equal(t, "fp1", decoded.Queries[1].Fingerprint)
}

func TestHandleQueriesDebugWithNilRegistry(t *testing.T) {
	h := handler.NewMultiGatewayHandler(nil, slog.Default(), 0)
	// No registry set.
	mg := &MultiGateway{pgHandler: h}

	req := httptest.NewRequest(http.MethodGet, "/debug/queries?format=json", nil)
	rec := httptest.NewRecorder()
	mg.handleQueriesDebug(rec, req)

	resp := rec.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.EqualValues(t, 0, decoded["tracked_fingerprints"])
	assert.EqualValues(t, 0, decoded["returned"])
}

func TestHandleQueriesDebugHTMLDefault(t *testing.T) {
	registry := queryregistry.NewForTest(queryregistry.DefaultConfig())
	defer registry.Close()
	registry.Record("fp1", "SELECT 1", time.Millisecond, 1, false)

	h := handler.NewMultiGatewayHandler(nil, slog.Default(), 0)
	h.SetQueryRegistry(registry)
	mg := &MultiGateway{pgHandler: h}

	req := httptest.NewRequest(http.MethodGet, "/debug/queries", nil)
	rec := httptest.NewRecorder()
	mg.handleQueriesDebug(rec, req)

	resp := rec.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	html := string(body)
	assert.Contains(t, html, "<!doctype html>")
	assert.Contains(t, html, "Per-Query Metrics")
	assert.Contains(t, html, "fp1")
	assert.Contains(t, html, "SELECT 1")
}
