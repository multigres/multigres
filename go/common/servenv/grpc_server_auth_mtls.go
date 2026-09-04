// Copyright 2019 The Vitess Authors.
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
	"context"
	"crypto/x509"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/spf13/pflag"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var (
	// clientCertSubstrings list of substrings of at least one of the client certificate names to use during authorization.
	// Must be non-empty when mtls auth is enabled; empty entries are rejected at startup.
	clientCertSubstrings string
	// MtlsAuthPlugin implements AuthPlugin interface
	_ Authenticator = (*MtlsAuthPlugin)(nil)
)

func registerGRPCServerAuthMTLSFlags(fs *pflag.FlagSet) {
	fs.StringVar(&clientCertSubstrings, "grpc-auth-mtls-allowed-substrings", clientCertSubstrings, "List of substrings of at least one of the client certificate names (separated by colon). Required when --grpc-auth-mode=mtls; must not contain empty entries.")
}

// MtlsAuthPlugin implements mTLS authentication for grpc. A client is authorized if any of the
// configured substrings occurs in the subject of any of its verified certificates.
type MtlsAuthPlugin struct {
	clientCertSubstrings []string
	metrics              *authMetrics
}

// Authenticate implements Authenticator interface. This method will be used inside a middleware in grpc_server to authenticate
// incoming requests. Every outcome is logged and recorded.
func (ma *MtlsAuthPlugin) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	newCtx, outcome := ma.checkCert(ctx)
	ma.metrics.record(ctx, "mtls", outcome)
	if outcome != AuthOutcomeSuccess {
		slog.WarnContext(ctx, "mtls auth: rejected request", "method", fullMethod, "reason", outcome)
		return nil, errGRPCAuthFailed
	}
	return newCtx, nil
}

// checkCert is the certificate-matching logic itself, deliberately with no
// logging or metrics attached, so Authenticate (above) is the sole place
// that decides what a miss means and reports it.
func (ma *MtlsAuthPlugin) checkCert(ctx context.Context) (context.Context, string) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, AuthOutcomeNoPeerInfo
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, AuthOutcomeNotTLS
	}
	if certSubjectMatches(tlsInfo.State.PeerCertificates, ma.clientCertSubstrings) {
		return ctx, AuthOutcomeSuccess
	}
	return nil, AuthOutcomeCertNotAuthorized
}

// certSubjectMatches reports whether the subject of any of certs contains at
// least one of substrings. Shared by the gRPC mtls Authenticator (checkCert
// above) and Multiadmin's HTTP client-cert middleware (clientCertAuthorized,
// http_auth.go), so both transports apply identical matching semantics - see
// the doc there for the one deliberate difference (HTTP matches the leaf
// certificate only, not the full chain).
//
// This is an unanchored substring test against the rendered DN, so an
// allow-list entry authorizes anything that merely extends it: "CN=ns-team-a"
// also admits "CN=ns-team-a-evil". That matters wherever the allow-list is a
// trust boundary between holders of certificates from the same CA, as it is
// for Multiadmin's per-tenant HTTP auth.
//
// An entry can be anchored by including the delimiter of the following RDN
// ("CN=ns-team-a,O=..."), which is safe because pkix escapes commas inside
// attribute values - a caller cannot forge a boundary from within its own CN.
// But a subject with no RDN after the anchored one renders without a trailing
// delimiter ("CN=ns-team-a"), so a bare-CN identity cannot be anchored this
// way at all, and substring matching simply cannot express "exactly this CN"
// for it.
//
// Matching on a parsed, exactly-compared identity is the real fix; it needs
// its own change, since it would alter the meaning of every existing
// allow-list.
func certSubjectMatches(certs []*x509.Certificate, substrings []string) bool {
	for _, substring := range substrings {
		for _, cert := range certs {
			if strings.Contains(cert.Subject.String(), substring) {
				return true
			}
		}
	}
	return false
}

// ParseCertSubstrings splits and validates the colon-separated allow-list
// carried by --grpc-auth-mtls-allowed-substrings. Exported so that
// Multiadmin's --enable-http-mtls-auth (go/services/multiadmin/init.go),
// which reuses this same flag value for HTTP client-cert auth rather than
// introducing a second allow-list, validates it identically to the gRPC
// mtls plugin below.
func ParseCertSubstrings(raw string) ([]string, error) {
	substrings := strings.Split(raw, ":")
	// An empty substring matches every certificate subject, authorizing all clients.
	if slices.Contains(substrings, "") {
		return nil, fmt.Errorf("--grpc-auth-mtls-allowed-substrings must be a non-empty colon-separated list without empty entries, got %q", raw)
	}
	return substrings, nil
}

func mtlsAuthPluginInitializer() (Authenticator, error) {
	substrings, err := ParseCertSubstrings(clientCertSubstrings)
	if err != nil {
		return nil, err
	}
	mtlsAuthPlugin := &MtlsAuthPlugin{
		clientCertSubstrings: substrings,
		metrics:              newAuthMetrics(),
	}
	slog.Info("mtls auth plugin have initialized successfully with allowed client cert name substrings", "client_substrings", clientCertSubstrings)
	return mtlsAuthPlugin, nil
}

// ClientCertSubstrings returns the value of the
// `--grpc-auth-mtls-allowed-substrings` flag.
func ClientCertSubstrings() string {
	return clientCertSubstrings
}

func init() {
	if err := RegisterAuthPlugin("mtls", mtlsAuthPluginInitializer); err != nil {
		slog.Error("failed to register mtls auth plugin", "error", err)
		panic(fmt.Sprintf("failed to register mtls auth plugin: %v", err))
	}
	grpcAuthServerFlagHooks = append(grpcAuthServerFlagHooks, registerGRPCServerAuthMTLSFlags)
}
