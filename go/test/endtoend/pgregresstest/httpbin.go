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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// This file provides the local httpbin-compatible servers behind
// ExternalExtension.NeedsHTTPBin.
//
// pgsql-http's shipped suite is built to run against a LOCAL httpbin instance:
// its first statements are `SET http.server_host = 'http://localhost:9080'`
// and a probe that falls back to live httpbin.org only when nothing answers
// locally (upstream CI provisions a local httpbin the same way). Serving the
// handful of httpbin endpoints the suite uses in-process keeps that portion
// local — no live httpbin.org fallback — while the extension's full client
// stack (libcurl inside the postgres backend, reached through multigateway) is
// exercised for real.
//
//   - httpbinPort (9080) is fixed by the suite's own SET statement.
//
// Responses mirror the httpbin response shapes the suite's assertions extract:
// args/form/data/method/url echo for /anything and /get, query→header echo for
// /response-headers, exact byte length for /image/png (the expected output
// pins length_binary=8090).
const (
	httpbinPort = 9080
	// httpbinImageLength is the body size of httpbin's /image/png (the "pig"
	// image the upstream expected file was captured against): the suite checks
	// content_type and byte length, not the pixels, so the local server returns
	// that many ASCII bytes (NUL-free, since http's content column is text).
	httpbinImageLength = 8090
)

// httpbinServers owns the listener for one suite run.
type httpbinServers struct {
	httpSrv *http.Server
}

// startHTTPBinServers starts the HTTP server on 9080 and returns a handle to
// stop it. Fails loudly when the port is taken: the suite would otherwise
// silently fall back to live httpbin.org.
func startHTTPBinServers() (*httpbinServers, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/anything", httpbinAnything)
	mux.HandleFunc("/anything/", httpbinAnything)
	mux.HandleFunc("/get", httpbinAnything)
	mux.HandleFunc("/status/", httpbinStatus)
	mux.HandleFunc("/response-headers", httpbinResponseHeaders)
	mux.HandleFunc("/delay/", httpbinDelay)
	mux.HandleFunc("/redirect-to", httpbinRedirectTo)
	mux.HandleFunc("/image/png", httpbinImage)

	httpLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpbinPort))
	if err != nil {
		return nil, fmt.Errorf("httpbin: listen :%d (the pgsql-http suite hard-codes this port): %w", httpbinPort, err)
	}
	s := &httpbinServers{
		httpSrv: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
	}
	go func() { _ = s.httpSrv.Serve(httpLn) }()
	return s, nil
}

func (s *httpbinServers) Stop() {
	_ = s.httpSrv.Close()
}

// httpbinAnything echoes the request the way httpbin's /anything and /get do,
// limited to the fields the pgsql-http assertions read: args (single-valued
// query params), data (raw body), form (parsed when urlencoded), headers,
// method, and the absolute url.
func httpbinAnything(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	args := map[string]string{}
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			args[k] = vs[0]
		}
	}

	form := map[string]string{}
	data := string(body)
	if ct := r.Header.Get("Content-Type"); strings.HasPrefix(ct, "application/x-www-form-urlencoded") && len(body) > 0 {
		if vals, err := parseQueryString(string(body)); err == nil {
			form = vals
			data = ""
		}
	}

	headers := map[string]string{}
	for k, vs := range r.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	resp := map[string]any{
		"args":    args,
		"data":    data,
		"form":    form,
		"headers": headers,
		"method":  r.Method,
		"url":     "http://" + r.Host + r.RequestURI,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func parseQueryString(s string) (map[string]string, error) {
	vals, err := url.ParseQuery(s)
	if err != nil {
		return nil, err
	}

	out := map[string]string{}
	for k, vs := range vals {
		if len(vs) > 0 {
			out[k] = vs[0]
		}
	}
	return out, nil
}

func httpbinStatus(w http.ResponseWriter, r *http.Request) {
	code, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/status/"))
	if err != nil || code < 100 || code > 599 {
		code = http.StatusBadRequest
	}
	w.WriteHeader(code)
}

func httpbinResponseHeaders(w http.ResponseWriter, r *http.Request) {
	resp := map[string]string{}
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			w.Header().Set(k, vs[0])
			resp[k] = vs[0]
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func httpbinDelay(w http.ResponseWriter, r *http.Request) {
	secs, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/delay/"))
	if err != nil || secs < 0 || secs > 60 {
		secs = 0
	}
	select {
	case <-time.After(time.Duration(secs) * time.Second):
	case <-r.Context().Done():
		// Client gave up (curl timeout or statement cancellation) — nothing to
		// write on a dead connection.
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, "{}")
}

func httpbinRedirectTo(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")
	if url == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, url, http.StatusFound)
}

func httpbinImage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", strconv.Itoa(httpbinImageLength))
	_, _ = w.Write([]byte(strings.Repeat("x", httpbinImageLength)))
}
