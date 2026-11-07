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

package s3mock

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoggingMiddlewareQuotesRequest(t *testing.T) {
	// Do not parallelize: this test temporarily captures the global logger.
	// ServeHTTP is synchronous here; no server goroutine outlives the capture.
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	request := httptest.NewRequest(http.MethodGet, "/bucket/key%0AFORGED?value=test", nil)
	request.Method = "GET\nFORGED"
	called := false
	handler := loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		require.Same(t, request, r)
		w.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.True(t, called)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.Equal(t, 1, strings.Count(output.String(), "\n"))
	require.Contains(t, output.String(), `[S3] "GET\nFORGED" "/bucket/key\nFORGED?value=test"`)
}
