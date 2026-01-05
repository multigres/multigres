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
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
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
	slog.Info("Listening for HTTP calls on port", "httpPort", sv.httpPort.Get())

	// Wrap the mux with CORS middleware and OpenTelemetry instrumentation
	// If no OTEL exporters are configured, noop exporters are used with minimal overhead
	handler := otelhttp.NewHandler(corsMiddleware(sv.mux), "http-server")

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	err := server.Serve(l)
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// HTTPRegisterProfile registers the default pprof HTTP endpoints with the internal servenv mux.
func (sv *ServEnv) HTTPRegisterPprofProfile() {
	if !sv.httpPprof.Get() {
		return
	}

	sv.HTTPHandleFunc("/debug/pprof/", pprof.Index)
	sv.HTTPHandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	sv.HTTPHandleFunc("/debug/pprof/profile", pprof.Profile)
	sv.HTTPHandleFunc("/debug/pprof/symbol", pprof.Symbol)
	sv.HTTPHandleFunc("/debug/pprof/trace", pprof.Trace)
}
