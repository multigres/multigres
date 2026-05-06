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

package shardsetup

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// SpanProto is the protobuf Span type from the OTLP trace package.
// Exported as a convenience so test files don't need to import the proto package directly.
type SpanProto = tracepb.Span

// TestOTLPCollector is a lightweight in-process OTLP HTTP receiver that
// collects spans exported by child processes (e.g., multigateway). It
// listens on a random port and accepts POST /v1/traces with protobuf-encoded
// ExportTraceServiceRequest bodies.
type TestOTLPCollector struct {
	mu       sync.Mutex
	spans    []*tracepb.ResourceSpans
	server   *http.Server
	listener net.Listener
	endpoint string
}

// NewTestOTLPCollector starts a test OTLP HTTP collector on a random port.
// The collector is automatically stopped when the test completes.
func NewTestOTLPCollector(t *testing.T) *TestOTLPCollector {
	t.Helper()

	c := &TestOTLPCollector{}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", c.handleTraces)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start test OTLP collector: %v", err)
	}

	c.listener = listener
	c.endpoint = fmt.Sprintf("http://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)
	c.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		// Do not use t.Logf here — the goroutine may outlive the test function,
		// which causes a panic in Go 1.24+.
		_ = c.server.Serve(listener)
	}()

	t.Cleanup(func() {
		c.server.Close()
	})

	t.Logf("Test OTLP collector listening at %s", c.endpoint)
	return c
}

// Endpoint returns the collector's HTTP endpoint (e.g., "http://127.0.0.1:12345").
func (c *TestOTLPCollector) Endpoint() string {
	return c.endpoint
}

// GetSpans returns all collected ResourceSpans.
func (c *TestOTLPCollector) GetSpans() []*tracepb.ResourceSpans {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*tracepb.ResourceSpans(nil), c.spans...)
}

// WaitForSpans polls until at least count spans with the given name are
// collected, or the timeout expires.
func (c *TestOTLPCollector) WaitForSpans(t *testing.T, spanName string, count int, timeout time.Duration) []*tracepb.Span {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		spans := c.findSpans(spanName)
		if len(spans) >= count {
			return spans
		}
		time.Sleep(100 * time.Millisecond)
	}
	spans := c.findSpans(spanName)
	t.Fatalf("timed out waiting for %d %q spans (got %d after %v)", count, spanName, len(spans), timeout)
	return nil
}

// findSpans returns all Span protos matching the given name.
func (c *TestOTLPCollector) findSpans(name string) []*tracepb.Span {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*tracepb.Span
	for _, rs := range c.spans {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				if span.GetName() == name {
					result = append(result, span)
				}
			}
		}
	}
	return result
}

func (c *TestOTLPCollector) handleTraces(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, "failed to unmarshal protobuf", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	c.spans = append(c.spans, req.GetResourceSpans()...)
	c.mu.Unlock()

	// Return success response
	resp := &coltracepb.ExportTraceServiceResponse{}
	respBytes, _ := proto.Marshal(resp)
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBytes)
}

// FindSpanAttribute looks up an attribute by key on a span proto.
// Returns the string value and true if found, or ("", false) if not.
func FindSpanAttribute(span *tracepb.Span, key string) (string, bool) {
	for _, attr := range span.GetAttributes() {
		if attr.GetKey() == key {
			if sv := attr.GetValue().GetStringValue(); sv != "" {
				return sv, true
			}
			// Handle array values by returning a string representation
			if av := attr.GetValue().GetArrayValue(); av != nil {
				var vals []string
				for _, v := range av.GetValues() {
					vals = append(vals, v.GetStringValue())
				}
				return fmt.Sprintf("%v", vals), true
			}
		}
	}
	return "", false
}

// FindSpanAttributeArray looks up a string array attribute by key on a span proto.
func FindSpanAttributeArray(span *tracepb.Span, key string) ([]string, bool) {
	for _, attr := range span.GetAttributes() {
		if attr.GetKey() == key {
			if av := attr.GetValue().GetArrayValue(); av != nil {
				var vals []string
				for _, v := range av.GetValues() {
					vals = append(vals, v.GetStringValue())
				}
				return vals, true
			}
		}
	}
	return nil, false
}
