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

package servenv

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// AuthOutcome* are the values used for the "outcome" attribute on
// mg.servenv.auth.attempts. Every rejection reason gets its own value -
// unlike the sanitized message returned to callers (see authFailedMessage in
// http_auth.go), these are server-side-only telemetry, so keeping them
// specific is exactly the point: an operator watching a dashboard should be
// able to tell "JWKS/issuer misconfigured" (a spike in AuthOutcomeInvalidToken)
// apart from "a valid caller hitting the wrong allow-list"
// (AuthOutcomeSubjectNotAuthorized) without grepping logs.
const (
	AuthOutcomeSuccess = "success"

	// JWTAuthPlugin.Authenticate (gRPC transport only - see the scope note
	// on AuthenticateBearer for why the HTTP/Connect equivalent of these
	// three isn't covered yet).
	AuthOutcomeNoMetadata      = "no_metadata"
	AuthOutcomeMissingHeader   = "missing_header"
	AuthOutcomeMalformedHeader = "malformed_header"

	// JWTAuthPlugin.VerifyToken - shared by the gRPC and HTTP/Connect paths.
	AuthOutcomeInvalidToken         = "invalid_token"
	AuthOutcomeSubjectNotAuthorized = "subject_not_authorized"

	// MtlsAuthPlugin.Authenticate.
	AuthOutcomeNoPeerInfo        = "no_peer_info"
	AuthOutcomeNotTLS            = "not_tls"
	AuthOutcomeCertNotAuthorized = "cert_not_authorized"
)

// authMetrics holds the shared-shape OTel counter used by every
// Authenticator implementation (mtls, jwt) to report authentication
// decisions, tagged by which plugin decided and what happened.
//
// One instance is constructed per resolved plugin (see
// jwtAuthPluginInitializer, mtlsAuthPluginInitializer) rather than a
// package-level singleton, so construction happens at the same point in
// each service's startup sequence that multigateway.NewGatewayMetrics uses -
// late enough that the real MeterProvider is already installed. A
// package-level var initialized at import time would still work correctly
// in production (OTel's global Meter delegates to whatever MeterProvider is
// registered first), but breaks test isolation: only the *first*
// SetMeterProvider call in a test binary triggers that delegation, so a
// second test function installing its own ManualReader would silently
// observe nothing.
type authMetrics struct {
	attempts metric.Int64Counter
}

func newAuthMetrics() *authMetrics {
	meter := otel.Meter("github.com/multigres/multigres/go/common/servenv")
	m := &authMetrics{}
	var err error
	m.attempts, err = meter.Int64Counter(
		"mg.servenv.auth.attempts",
		metric.WithDescription("Authenticator plugin decisions (mtls, jwt), tagged by plugin and outcome"),
		metric.WithUnit("{attempt}"),
	)
	if err != nil {
		slog.Error("failed to initialize servenv auth metrics", "error", fmt.Errorf("mg.servenv.auth.attempts counter: %w", err))
		m.attempts = noop.Int64Counter{}
	}
	return m
}

// record increments the auth-attempts counter tagged by plugin and outcome.
// Safe to call on a nil receiver so call sites can stay unconditional.
func (m *authMetrics) record(ctx context.Context, plugin, outcome string) {
	if m == nil {
		return
	}
	m.attempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("plugin", plugin),
		attribute.String("outcome", outcome),
	))
}
