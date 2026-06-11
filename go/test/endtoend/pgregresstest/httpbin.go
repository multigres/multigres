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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
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
// handful of httpbin endpoints the suite uses in-process keeps the run
// hermetic — no live internet, no flaky fallback — while the extension's full
// client stack (libcurl inside the postgres backend, reached through
// multigateway) is exercised for real.
//
//   - httpbinPort (9080) is fixed by the suite's own SET statement.
//   - httpbinTLSPort (9443) serves HTTPS with an in-memory self-signed
//     certificate; the suite's TLS probes (bogus CAINFO must fail, VERIFYPEER=0
//     must succeed) are redirected here via TextRewrites, exercising exactly
//     the same libcurl TLS paths as the live endpoint they replace.
//
// Responses mirror the httpbin response shapes the suite's assertions extract:
// args/form/data/method/url echo for /anything and /get, query→header echo for
// /response-headers, exact byte length for /image/png (the expected output
// pins length_binary=8090).
const (
	httpbinPort    = 9080
	httpbinTLSPort = 9443
	// httpbinImageLength is the body size of httpbin's /image/png (the "pig"
	// image the upstream expected file was captured against): the suite checks
	// content_type and byte length, not the pixels, so the local server returns
	// that many ASCII bytes (NUL-free, since http's content column is text).
	httpbinImageLength = 8090
)

// httpbinServers owns the two listeners for one suite run.
type httpbinServers struct {
	httpSrv  *http.Server
	httpsSrv *http.Server
}

// startHTTPBinServers starts the HTTP (9080) and HTTPS (9443) servers and
// returns a handle to stop them. Fails loudly when a port is taken: the suite
// would otherwise silently fall back to live httpbin.org.
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
	tlsLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpbinTLSPort))
	if err != nil {
		httpLn.Close()
		return nil, fmt.Errorf("httpbin: listen :%d: %w", httpbinTLSPort, err)
	}

	cert, err := selfSignedCert()
	if err != nil {
		httpLn.Close()
		tlsLn.Close()
		return nil, fmt.Errorf("httpbin: generate TLS cert: %w", err)
	}

	s := &httpbinServers{
		httpSrv: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		httpsSrv: &http.Server{
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/plain")
				fmt.Fprintln(w, "ok")
			}),
			ReadHeaderTimeout: 10 * time.Second,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   tls.VersionTLS12,
			},
		},
	}
	go func() { _ = s.httpSrv.Serve(httpLn) }()
	go func() { _ = s.httpsSrv.ServeTLS(tlsLn, "", "") }()
	return s, nil
}

func (s *httpbinServers) Stop() {
	_ = s.httpSrv.Close()
	_ = s.httpsSrv.Close()
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
	out := map[string]string{}
	for pair := range strings.SplitSeq(s, "&") {
		k, v, _ := strings.Cut(pair, "=")
		out[k] = v
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

// selfSignedCert builds an in-memory ECDSA certificate for 127.0.0.1 /
// localhost. The suite never trusts it (that's the point: the bogus-CAINFO
// probe must FAIL verification, and the VERIFYPEER=0 probe must succeed
// anyway), so nothing is written to disk.
func selfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgregresstest-httpbin"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
