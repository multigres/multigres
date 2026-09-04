// Copyright 2023 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package servenv

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/multigres/multigres/go/tools/grpccommon"
)

// HTTPHandle registers the given handler for the internal servenv mux.
func (sv *ServEnv) HTTPHandle(pattern string, handler http.Handler) {
	sv.mux.Handle(pattern, handler)
}

// HTTPHandleFunc registers the given handler func for the internal servenv mux.
func (sv *ServEnv) HTTPHandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	sv.mux.HandleFunc(pattern, handler)
}

// corsMiddleware adds CORS headers to allow cross-origin requests.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle preflight OPTIONS requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// HTTPServe starts the HTTP server for the internal servenv mux on the listener.
func (sv *ServEnv) HTTPServe(l net.Listener) error {
	slog.Info("listening for HTTP calls on port", "http_port", sv.httpPort.Get())

	// Wrap the mux with CORS middleware, optional client-cert enforcement,
	// and OpenTelemetry instrumentation, in that order. The client-cert
	// middleware sits outside CORS so preflight OPTIONS requests are gated
	// too, and inside the otelhttp span so rejections still get traced.
	// If no OTEL exporters are configured, noop exporters are used with
	// minimal overhead.
	handler := corsMiddleware(sv.mux)
	if sv.httpClientCertRequired {
		handler = requireClientCert(sv.httpClientCertSubstrings, handler)
	}
	handler = otelhttp.NewHandler(handler, "http-server")

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// tls.VerifyClientCertIfGiven, not tls.RequireAndVerifyClientCert: this
	// listener is shared with Kubernetes kubelet probes, which cannot
	// present a client certificate, so the handshake must succeed with none
	// offered. requireClientCert (http_auth.go) enforces identity per-route
	// instead, exempting the probe/version paths in unauthenticatedHTTPPaths.
	tlsConfig, err := grpccommon.BuildServerTLSConfigWithClientAuth(
		sv.httpCert.Get(), sv.httpKey.Get(), sv.httpCA.Get(), "", tls.VerifyClientCertIfGiven,
	)
	if err != nil {
		return fmt.Errorf("http tls config: %w", err)
	}

	if tlsConfig != nil {
		server.TLSConfig = tlsConfig
		err = server.ServeTLS(l, "", "")
	} else {
		err = server.Serve(l)
	}
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// HTTPRegisterProfile registers the default pprof HTTP endpoints with the
// internal servenv mux. pprof discloses CPU/heap/goroutine internals and can
// itself be used as a cheap DoS vector (repeated profile/trace captures), so
// each endpoint is gated by RequireBearerAuth via resolveAuthPlugin. For
// every service that never calls SetAuthPlugin, resolveAuthPlugin always
// returns nil and these endpoints behave exactly as before (unauthenticated,
// gated only by --pprof-http).
func (sv *ServEnv) HTTPRegisterPprofProfile() {
	if !sv.httpPprof.Get() {
		return
	}

	sv.HTTPHandleFunc("/debug/pprof/", RequireBearerAuth(sv.resolveAuthPlugin, pprof.Index))
	sv.HTTPHandleFunc("/debug/pprof/cmdline", RequireBearerAuth(sv.resolveAuthPlugin, pprof.Cmdline))
	sv.HTTPHandleFunc("/debug/pprof/profile", RequireBearerAuth(sv.resolveAuthPlugin, pprof.Profile))
	sv.HTTPHandleFunc("/debug/pprof/symbol", RequireBearerAuth(sv.resolveAuthPlugin, pprof.Symbol))
	sv.HTTPHandleFunc("/debug/pprof/trace", RequireBearerAuth(sv.resolveAuthPlugin, pprof.Trace))
}
