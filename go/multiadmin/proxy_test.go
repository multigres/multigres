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
	"strings"
	"testing"
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
