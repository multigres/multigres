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

package pgregresstest

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPBinServers exercises every endpoint shape the pgsql-http suite's
// assertions extract, against the real listeners on the fixed ports.
func TestHTTPBinServers(t *testing.T) {
	servers, err := startHTTPBinServers()
	require.NoError(t, err, "ports 9080/9443 must be free")
	t.Cleanup(servers.Stop)

	base := fmt.Sprintf("http://127.0.0.1:%d", httpbinPort)

	get := func(path string) (int, http.Header, map[string]any) {
		t.Helper()
		resp, err := http.Get(base + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var decoded map[string]any
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
			require.NoError(t, json.Unmarshal(body, &decoded), "body: %s", body)
		}
		return resp.StatusCode, resp.Header, decoded
	}

	t.Run("status", func(t *testing.T) {
		status, _, _ := get("/status/202")
		assert.Equal(t, 202, status)
		status, _, _ = get("/status/555")
		assert.Equal(t, 555, status)
	})

	t.Run("anything GET echoes args/method/url", func(t *testing.T) {
		status, _, body := get("/anything?foo=bar")
		assert.Equal(t, 200, status)
		assert.Equal(t, "bar", body["args"].(map[string]any)["foo"])
		assert.Equal(t, "GET", body["method"])
		assert.Equal(t, fmt.Sprintf("http://127.0.0.1:%d/anything?foo=bar", httpbinPort), body["url"])
		// Empty args must render as {} (the suite extracts it as text).
		_, _, body = get("/anything")
		assert.Equal(t, map[string]any{}, body["args"])
	})

	t.Run("anything POST form is parsed, raw body echoed otherwise", func(t *testing.T) {
		resp, err := http.Post(base+"/anything", "application/x-www-form-urlencoded",
			strings.NewReader("key1=value1&key2=value2"))
		require.NoError(t, err)
		var body map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		resp.Body.Close()
		form := body["form"].(map[string]any)
		assert.Equal(t, "value1", form["key1"])
		assert.Equal(t, "value2", form["key2"])

		resp, err = http.Post(base+"/anything?foo=bar", "text/plain", strings.NewReader("payload"))
		require.NoError(t, err)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		resp.Body.Close()
		assert.Equal(t, "payload", body["data"])
		assert.Equal(t, "POST", body["method"])
	})

	t.Run("response-headers echoes query as header", func(t *testing.T) {
		_, header, _ := get("/response-headers?Abcde=abcde")
		assert.Equal(t, "abcde", header.Get("Abcde"))
	})

	t.Run("redirect-to lands on /get", func(t *testing.T) {
		status, _, body := get("/redirect-to?url=get")
		assert.Equal(t, 200, status)
		assert.Equal(t, fmt.Sprintf("http://127.0.0.1:%d/get", httpbinPort), body["url"])
	})

	t.Run("image is exactly the pinned length", func(t *testing.T) {
		resp, err := http.Get(base + "/image/png")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "image/png", resp.Header.Get("Content-Type"))
		assert.Len(t, body, httpbinImageLength)
	})

	t.Run("https answers with self-signed cert", func(t *testing.T) {
		// InsecureSkipVerify mirrors the suite's CURLOPT_SSL_VERIFYPEER=0
		// probe — the server's cert is intentionally self-signed.
		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402
		}}
		resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/", httpbinTLSPort))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)

		// And verification must FAIL without InsecureSkipVerify — the suite's
		// bogus-CAINFO probe depends on that.
		respFail, err := http.Get(fmt.Sprintf("https://127.0.0.1:%d/", httpbinTLSPort))
		if err == nil {
			respFail.Body.Close()
		}
		assert.Error(t, err)
	})
}
