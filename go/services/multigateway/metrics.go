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

package multigateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// Re-export the canonical auth/TLS label constants from pgprotocol/server so
// callers inside the multigateway package (e.g. tests) don't need a second
// import. The vocabulary is defined alongside the AuthMetricsRecorder
// interface so the listener can emit metrics without depending on this
// package.
const (
	AuthOutcomeSuccess         = server.AuthOutcomeSuccess
	AuthOutcomeUserNotFound    = server.AuthOutcomeUserNotFound
	AuthOutcomeBadPassword     = server.AuthOutcomeBadPassword
	AuthOutcomeProtocolError   = server.AuthOutcomeProtocolError
	AuthOutcomeLoginDisabled   = server.AuthOutcomeLoginDisabled
	AuthOutcomePasswordExpired = server.AuthOutcomePasswordExpired
	AuthOutcomeLookupError     = server.AuthOutcomeLookupError

	TLSOutcomeSuccess          = server.TLSOutcomeSuccess
	TLSOutcomeHandshakeFailure = server.TLSOutcomeHandshakeFailure
	TLSOutcomeClientAborted    = server.TLSOutcomeClientAborted

	PlaintextRejectedReasonNoSSLRequest        = server.PlaintextRejectedReasonNoSSLRequest
	PlaintextRejectedReasonTLSDisabledByServer = server.PlaintextRejectedReasonTLSDisabledByServer
)

// GatewayMetrics holds OTel metrics for the multigateway service.
type GatewayMetrics struct {
	meter             metric.Meter
	clientConnections metric.Int64ObservableGauge

	// Auth metrics (mg.gateway.auth.*).
	authSCRAMDuration            metric.Float64Histogram
	authAttempts                 metric.Int64Counter
	authCredentialLookupDuration metric.Float64Histogram
	authCredentialLookupRate     metric.Int64Counter

	// TLS metrics (mg.gateway.tls.*).
	tlsHandshakeDuration  metric.Float64Histogram
	tlsConnections        metric.Int64Counter
	tlsPlaintextRejected  metric.Int64Counter
	tlsSSLRequestDeclined metric.Int64Counter
}

// NewGatewayMetrics initializes OTel metrics for the multigateway service.
func NewGatewayMetrics() (*GatewayMetrics, error) {
	meter := otel.Meter("github.com/multigres/multigres/go/services/multigateway")

	m := &GatewayMetrics{meter: meter}
	var errs []error

	var err error
	m.clientConnections, err = meter.Int64ObservableGauge(
		"mg.gateway.client.connections",
		metric.WithDescription("Number of active client connections to the gateway"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.client.connections gauge: %w", err))
	}

	m.authSCRAMDuration, err = meter.Float64Histogram(
		"mg.gateway.auth.scram.duration",
		metric.WithDescription("Full SCRAM handshake duration (client-first to server-final)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.auth.scram.duration histogram: %w", err))
	}

	m.authAttempts, err = meter.Int64Counter(
		"mg.gateway.auth.attempts",
		metric.WithDescription("Client authentication attempts"),
		metric.WithUnit("{attempt}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.auth.attempts counter: %w", err))
	}

	m.authCredentialLookupDuration, err = meter.Float64Histogram(
		"mg.gateway.auth.credential_lookup.duration",
		metric.WithDescription("GetAuthCredentials RPC latency observed from the gateway"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.auth.credential_lookup.duration histogram: %w", err))
	}

	m.authCredentialLookupRate, err = meter.Int64Counter(
		"mg.gateway.auth.credential_lookup.rate",
		metric.WithDescription("GetAuthCredentials RPC invocations; quantifies benefit of a credential cache"),
		metric.WithUnit("{lookup}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.auth.credential_lookup.rate counter: %w", err))
	}

	m.tlsHandshakeDuration, err = meter.Float64Histogram(
		"mg.gateway.tls.handshake.duration",
		metric.WithDescription("TLS handshake duration in handleSSLRequest"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.tls.handshake.duration histogram: %w", err))
	}

	m.tlsConnections, err = meter.Int64Counter(
		"mg.gateway.tls.connections",
		metric.WithDescription("Connections that completed TLS negotiation"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.tls.connections counter: %w", err))
	}

	m.tlsPlaintextRejected, err = meter.Int64Counter(
		"mg.gateway.tls.plaintext_rejected",
		metric.WithDescription("Connections rejected because --pg-require-ssl=true and client did not negotiate TLS"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.tls.plaintext_rejected counter: %w", err))
	}

	m.tlsSSLRequestDeclined, err = meter.Int64Counter(
		"mg.gateway.tls.sslrequest_declined",
		metric.WithDescription("SSLRequest replied with 'N' because no cert/key configured"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("mg.gateway.tls.sslrequest_declined counter: %w", err))
	}

	if len(errs) > 0 {
		return m, errors.Join(errs...)
	}
	return m, nil
}

// RegisterClientConnectionsCallback registers a callback that reports the current
// number of active client connections, broken down by endpoint.
// The replica getter may be nil when the replica-reads port is disabled.
func (m *GatewayMetrics) RegisterClientConnectionsCallback(primaryGetter, replicaGetter func() int) error {
	if m.clientConnections == nil || primaryGetter == nil {
		return nil
	}

	primaryAttr := metric.WithAttributes(attribute.String("endpoint", "primary"))
	replicaAttr := metric.WithAttributes(attribute.String("endpoint", "replica"))

	_, err := m.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(m.clientConnections, int64(primaryGetter()), primaryAttr)
			if replicaGetter != nil {
				o.ObserveInt64(m.clientConnections, int64(replicaGetter()), replicaAttr)
			}
			return nil
		},
		m.clientConnections,
	)
	return err
}

// RecordSCRAMDuration records the SCRAM handshake duration tagged by outcome.
// Safe to call on a nil receiver — no-op so call sites can stay unconditional.
func (m *GatewayMetrics) RecordSCRAMDuration(ctx context.Context, outcome string, d time.Duration) {
	if m == nil || m.authSCRAMDuration == nil {
		return
	}
	m.authSCRAMDuration.Record(ctx, d.Seconds(),
		metric.WithAttributes(attribute.String("outcome", outcome)))
}

// RecordAuthAttempt increments the auth-attempts counter tagged by outcome.
func (m *GatewayMetrics) RecordAuthAttempt(ctx context.Context, outcome string) {
	if m == nil || m.authAttempts == nil {
		return
	}
	m.authAttempts.Add(ctx, 1,
		metric.WithAttributes(attribute.String("outcome", outcome)))
}

// RecordCredentialLookup records the GetAuthCredentials RPC latency from the
// gateway side and increments the lookup-rate counter.
func (m *GatewayMetrics) RecordCredentialLookup(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	if m.authCredentialLookupDuration != nil {
		m.authCredentialLookupDuration.Record(ctx, d.Seconds())
	}
	if m.authCredentialLookupRate != nil {
		m.authCredentialLookupRate.Add(ctx, 1)
	}
}

// RecordTLSHandshake records the TLS handshake duration tagged by outcome.
func (m *GatewayMetrics) RecordTLSHandshake(ctx context.Context, outcome string, d time.Duration) {
	if m == nil || m.tlsHandshakeDuration == nil {
		return
	}
	m.tlsHandshakeDuration.Record(ctx, d.Seconds(),
		metric.WithAttributes(attribute.String("outcome", outcome)))
}

// RecordTLSConnection increments the completed-TLS-connections counter tagged
// with the negotiated tls_version and cipher_suite. Names come from
// crypto/tls so they match Go's published constants (e.g. "TLS 1.3",
// "TLS_AES_128_GCM_SHA256").
func (m *GatewayMetrics) RecordTLSConnection(ctx context.Context, version, cipher uint16) {
	if m == nil || m.tlsConnections == nil {
		return
	}
	m.tlsConnections.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tls_version", tls.VersionName(version)),
		attribute.String("cipher_suite", tls.CipherSuiteName(cipher)),
	))
}

// RecordPlaintextRejected increments the plaintext-rejection counter tagged
// by reason.
func (m *GatewayMetrics) RecordPlaintextRejected(ctx context.Context, reason string) {
	if m == nil || m.tlsPlaintextRejected == nil {
		return
	}
	m.tlsPlaintextRejected.Add(ctx, 1,
		metric.WithAttributes(attribute.String("reason", reason)))
}

// RecordSSLRequestDeclined increments the SSL-declined counter, used before
// enforcement is enabled to size the impact of flipping --pg-require-ssl.
func (m *GatewayMetrics) RecordSSLRequestDeclined(ctx context.Context) {
	if m == nil || m.tlsSSLRequestDeclined == nil {
		return
	}
	m.tlsSSLRequestDeclined.Add(ctx, 1)
}
