// Copyright 2025 Supabase, Inc.
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

package multiadmin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestRewriteHTML(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		basePath string
		// Use contains checks instead of exact equality because html.Render
		// may add attributes like xmlns or change whitespace/formatting
		wantContains   []string
		wantNotContain []string
	}{
		{
			name:     "injects base tag in head",
			input:    `<html><head><title>Test</title></head><body></body></html>`,
			basePath: "/proxy/gate/zone1/gw",
			wantContains: []string{
				`href="/proxy/gate/zone1/gw/"`, // flexible - works with both <base> and <base/>
				`<head>`,
			},
		},
		{
			name:     "rewrites absolute path in href",
			input:    `<html><head></head><body><a href="/config">Config</a></body></html>`,
			basePath: "/proxy/pool/zone1/pooler",
			wantContains: []string{
				`href="/proxy/pool/zone1/pooler/config"`,
			},
		},
		{
			name:     "rewrites absolute path in src",
			input:    `<html><head></head><body><img src="/favicon.ico"/></body></html>`,
			basePath: "/proxy/admin/multiadmin",
			wantContains: []string{
				`src="/proxy/admin/multiadmin/favicon.ico"`,
			},
		},
		{
			name:     "skips already-proxied URLs starting with /proxy/",
			input:    `<html><head></head><body><a href="/proxy/gate/zone2/gw2/config">Other</a></body></html>`,
			basePath: "/proxy/gate/zone1/gw1",
			wantContains: []string{
				`href="/proxy/gate/zone2/gw2/config"`, // unchanged
			},
			wantNotContain: []string{
				`/proxy/gate/zone1/gw1/proxy/`, // should not double-prefix
			},
		},
		{
			name:     "preserves relative URLs (handled by base tag)",
			input:    `<html><head></head><body><a href="config">Relative</a></body></html>`,
			basePath: "/proxy/pool/zone1/pooler",
			wantContains: []string{
				`href="config"`, // unchanged, <base> tag handles it
			},
			wantNotContain: []string{
				`href="/proxy/pool/zone1/pooler/config"`, // should NOT rewrite relative URLs
			},
		},
		{
			name:     "handles multiple absolute URLs",
			input:    `<html><head><link rel="stylesheet" href="/css/style.css"/></head><body><script src="/js/app.js"></script></body></html>`,
			basePath: "/proxy/orch/zone1/orch",
			wantContains: []string{
				`href="/proxy/orch/zone1/orch/css/style.css"`,
				`src="/proxy/orch/zone1/orch/js/app.js"`,
			},
		},
		{
			name:     "skips external URLs",
			input:    `<html><head></head><body><a href="https://example.com/path">External</a></body></html>`,
			basePath: "/proxy/admin/multiadmin",
			wantContains: []string{
				`href="https://example.com/path"`, // unchanged
			},
			wantNotContain: []string{
				`href="/proxy/admin/multiadmin/https://`, // should not rewrite external URLs
			},
		},
		{
			name:     "handles URLs with query strings and fragments",
			input:    `<html><head></head><body><a href="/config?debug=true#section">Link</a></body></html>`,
			basePath: "/proxy/gate/zone1/gw",
			wantContains: []string{
				`href="/proxy/gate/zone1/gw/config?debug=true#section"`,
			},
		},
		{
			name:     "handles empty href and src attributes",
			input:    `<html><head></head><body><a href="">Empty</a><img src=""/></body></html>`,
			basePath: "/proxy/pool/zone1/pooler",
			wantContains: []string{
				`href=""`,
				`src=""`,
			},
		},
		{
			name:     "handles mixed absolute and relative URLs",
			input:    `<html><head></head><body><a href="/absolute">Abs</a><a href="relative">Rel</a><a href="/proxy/other/path">Proxied</a></body></html>`,
			basePath: "/proxy/gate/zone1/gw",
			wantContains: []string{
				`href="/proxy/gate/zone1/gw/absolute"`, // rewritten
				`href="relative"`,                      // unchanged (relative)
				`href="/proxy/other/path"`,             // unchanged (already proxied)
			},
		},
		{
			name:     "preserves URLs that match the current proxy base path",
			input:    `<html><head></head><body><a href="/proxy/gate/zone1/gw/config">Self</a></body></html>`,
			basePath: "/proxy/gate/zone1/gw",
			wantContains: []string{
				`href="/proxy/gate/zone1/gw/config"`, // unchanged
			},
			wantNotContain: []string{
				`href="/proxy/gate/zone1/gw/proxy/gate/zone1/gw/config"`, // should not double-prefix
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rewriteHTML([]byte(tt.input), tt.basePath)
			if err != nil {
				t.Fatalf("rewriteHTML() error = %v", err)
			}

			gotStr := string(got)

			// Check for required content
			for _, want := range tt.wantContains {
				if !strings.Contains(gotStr, want) {
					t.Errorf("rewriteHTML() output missing expected content.\nWant substring: %q\nGot: %s", want, gotStr)
				}
			}

			// Check for content that should not be present
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(gotStr, notWant) {
					t.Errorf("rewriteHTML() output contains unexpected content.\nShould not contain: %q\nGot: %s", notWant, gotStr)
				}
			}
		})
	}
}

func TestRewriteHTML_InvalidHTML(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		basePath string
	}{
		{
			name:     "malformed HTML with unclosed tags",
			input:    `<html><head><body><a href="/test">Link`,
			basePath: "/proxy/gate/zone1/gw",
		},
		{
			name:     "HTML fragment without root element",
			input:    `<a href="/config">Link</a><img src="/image.png"/>`,
			basePath: "/proxy/admin/multiadmin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic or return error - html.Parse is lenient
			got, err := rewriteHTML([]byte(tt.input), tt.basePath)
			if err != nil {
				t.Fatalf("rewriteHTML() error = %v, should handle malformed HTML gracefully", err)
			}

			// Just verify it returns something and doesn't panic
			if len(got) == 0 {
				t.Error("rewriteHTML() returned empty output for malformed HTML")
			}
		})
	}
}

func TestRewriteHTML_EmptyInput(t *testing.T) {
	got, err := rewriteHTML([]byte(""), "/proxy/gate/zone1/gw")
	if err != nil {
		t.Fatalf("rewriteHTML() error = %v", err)
	}

	// Empty input should still produce valid HTML output
	if len(got) == 0 {
		t.Error("rewriteHTML() returned empty output for empty input")
	}
}

// newProxyTestAdmin builds a Multiadmin backed by an in-memory topology for the
// proxy routing tests. senv is left nil on purpose: the routing methods under
// test only consult it for the "admin" self-proxy branch and the
// HTML-rewrite-failure log path, neither of which these tests exercise.
func newProxyTestAdmin(t *testing.T, cells ...string) (*Multiadmin, topoclient.Store) {
	t.Helper()
	ts := memorytopo.NewServer(t.Context(), cells...)
	return &Multiadmin{ts: ts}, ts
}

func TestParseProxyPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    *proxyPathInfo
		wantErr bool
	}{
		{
			name: "three parts",
			path: "/proxy/gate/cell1/gw1",
			want: &proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "gw1"},
		},
		{
			name: "extra path beyond the service name is ignored for routing",
			path: "/proxy/pool/cell1/p1/metrics/x",
			want: &proxyPathInfo{serviceType: "pool", cellName: "cell1", serviceName: "p1"},
		},
		{
			name:    "too few parts",
			path:    "/proxy/gate/cell1",
			wantErr: true,
		},
		{
			name:    "empty after prefix",
			path:    "/proxy/",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProxyPath(tt.path)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLookupCellService(t *testing.T) {
	ma, ts := newProxyTestAdmin(t, "cell1")
	ctx := t.Context()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	require.NoError(t, ts.CreateMultigateway(ctx, &clustermetadatapb.Multigateway{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "cell1", Name: "gw1"},
		Hostname: "gw.host",
		PortMap:  map[string]int32{"http": 8080, "grpc": 1},
	}))
	require.NoError(t, ts.CreateMultipooler(ctx, &clustermetadatapb.Multipooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
		Hostname: "pool.host",
		PortMap:  map[string]int32{"http": 8081},
	}))
	require.NoError(t, ts.CreateMultiorch(ctx, &clustermetadatapb.Multiorch{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "o1"},
		Hostname: "orch.host",
		PortMap:  map[string]int32{"http": 8082},
	}))
	// A gateway that has no http port in its port map.
	require.NoError(t, ts.CreateMultigateway(ctx, &clustermetadatapb.Multigateway{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "cell1", Name: "noport"},
		Hostname: "gw.host",
		PortMap:  map[string]int32{"grpc": 1},
	}))

	t.Run("gate", func(t *testing.T) {
		host, port, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "gw1"})
		require.NoError(t, err)
		assert.Equal(t, "gw.host", host)
		assert.Equal(t, 8080, port)
	})
	t.Run("pool", func(t *testing.T) {
		host, port, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "pool", cellName: "cell1", serviceName: "p1"})
		require.NoError(t, err)
		assert.Equal(t, "pool.host", host)
		assert.Equal(t, 8081, port)
	})
	t.Run("orch", func(t *testing.T) {
		host, port, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "orch", cellName: "cell1", serviceName: "o1"})
		require.NoError(t, err)
		assert.Equal(t, "orch.host", host)
		assert.Equal(t, 8082, port)
	})
	t.Run("invalid service type", func(t *testing.T) {
		_, _, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "bogus", cellName: "cell1", serviceName: "x"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid service type")
	})
	t.Run("missing http port", func(t *testing.T) {
		_, _, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "noport"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service port not found")
	})
	t.Run("service not in topology", func(t *testing.T) {
		_, _, err := ma.lookupCellService(req, proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "missing"})
		require.Error(t, err)
	})
}

func TestResolveServiceTarget(t *testing.T) {
	ma, ts := newProxyTestAdmin(t, "cell1")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	require.NoError(t, ts.CreateMultigateway(t.Context(), &clustermetadatapb.Multigateway{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "cell1", Name: "gw1"},
		Hostname: "gw.host",
		PortMap:  map[string]int32{"http": 8080},
	}))

	t.Run("cell service builds target and base path", func(t *testing.T) {
		target, err := ma.resolveServiceTarget(req, proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "gw1"})
		require.NoError(t, err)
		assert.Equal(t, "gw.host", target.host)
		assert.Equal(t, 8080, target.port)
		assert.Equal(t, "/proxy/gate/cell1/gw1", target.proxyBasePath)
	})
	t.Run("invalid service type", func(t *testing.T) {
		_, err := ma.resolveServiceTarget(req, proxyPathInfo{serviceType: "bogus"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid service type")
	})
	t.Run("unknown cell service is not found", func(t *testing.T) {
		_, err := ma.resolveServiceTarget(req, proxyPathInfo{serviceType: "gate", cellName: "cell1", serviceName: "missing"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service not found")
	})
}

func TestHandleProxy(t *testing.T) {
	// Backend the proxy target resolves to. It echoes the path it received (so we
	// can assert the proxy prefix was stripped) and serves HTML on one route (so
	// we can assert the HTML rewrite ran).
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/htmlpage" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><head><title>t</title></head><body><a href="/foo">x</a></body></html>`)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "backend saw: "+r.URL.Path)
	}))
	defer backend.Close()

	u, err := url.Parse(backend.URL)
	require.NoError(t, err)
	port, err := strconv.ParseInt(u.Port(), 10, 32)
	require.NoError(t, err)

	ma, ts := newProxyTestAdmin(t, "cell1")
	require.NoError(t, ts.CreateMultigateway(t.Context(), &clustermetadatapb.Multigateway{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIGATEWAY, Cell: "cell1", Name: "gw1"},
		Hostname: u.Hostname(),
		PortMap:  map[string]int32{"http": int32(port)},
	}))

	t.Run("invalid path returns 400", func(t *testing.T) {
		rec := httptest.NewRecorder()
		ma.handleProxy(rec, httptest.NewRequest(http.MethodGet, "/proxy/gate/cell1", nil))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown service returns 404", func(t *testing.T) {
		rec := httptest.NewRecorder()
		ma.handleProxy(rec, httptest.NewRequest(http.MethodGet, "/proxy/gate/cell1/missing", nil))
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("proxies to the backend and strips the proxy prefix", func(t *testing.T) {
		rec := httptest.NewRecorder()
		ma.handleProxy(rec, httptest.NewRequest(http.MethodGet, "/proxy/gate/cell1/gw1/somepage", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "backend saw: /somepage", rec.Body.String())
	})

	t.Run("rewrites HTML responses to carry the proxy base path", func(t *testing.T) {
		rec := httptest.NewRecorder()
		ma.handleProxy(rec, httptest.NewRequest(http.MethodGet, "/proxy/gate/cell1/gw1/htmlpage", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
		// The rewrite injects a <base>/absolute-URL prefix so the browser keeps
		// routing subsequent requests through the proxy base path.
		assert.Contains(t, rec.Body.String(), "/proxy/gate/cell1/gw1")
	})
}
