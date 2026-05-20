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

package server

import (
	"context"
	"time"
)

// Auth outcome label values used when emitting auth metrics. The set is
// closed (bounded cardinality) and mirrors the rejection sites in
// authenticateSCRAM. Keeping the canonical strings here lets the recorder
// implementations in higher layers share the vocabulary without inverting
// the dependency direction (pgprotocol/server must not import service code).
const (
	AuthOutcomeSuccess         = "success"
	AuthOutcomeUserNotFound    = "user_not_found"
	AuthOutcomeBadPassword     = "bad_password"
	AuthOutcomeProtocolError   = "protocol_error"
	AuthOutcomeLoginDisabled   = "login_disabled"
	AuthOutcomePasswordExpired = "password_expired"
	AuthOutcomeLookupError     = "lookup_error"
)

// TLS handshake outcome label values.
const (
	TLSOutcomeSuccess          = "success"
	TLSOutcomeHandshakeFailure = "handshake_failure"
	TLSOutcomeClientAborted    = "client_aborted"
)

// Plaintext-rejection reason labels.
const (
	PlaintextRejectedReasonNoSSLRequest        = "no_sslrequest"
	PlaintextRejectedReasonTLSDisabledByServer = "tls_disabled_by_server"
)

// AuthMetricsRecorder is the sink the listener calls during the startup
// phase to publish auth- and TLS-path metrics. Implementations live in
// service code (multigateway) so this package stays OTel-free.
//
// All methods must be safe to call concurrently. A nil receiver pattern is
// allowed in implementations — callers do not check before invoking — so
// implementations should also be safe when constructed but not yet wired
// to a meter (e.g. tests).
type AuthMetricsRecorder interface {
	// RecordSCRAMDuration is called once per SCRAM handshake attempt with
	// the wall-clock duration and one of the AuthOutcome* values.
	RecordSCRAMDuration(ctx context.Context, outcome string, d time.Duration)

	// RecordAuthAttempt is called once per client authentication attempt
	// — including trust auth — tagged with the outcome. Counter, not
	// histogram: cardinality is bounded by the outcome set.
	RecordAuthAttempt(ctx context.Context, outcome string)

	// RecordCredentialLookup is called by the gateway's credential
	// provider after each backing lookup (e.g. the GetAuthCredentials
	// RPC). Records the latency and bumps the lookup-rate counter so
	// future cache work has a baseline.
	RecordCredentialLookup(ctx context.Context, d time.Duration)

	// RecordTLSHandshake is called once per TLS negotiation with the
	// handshake wall-clock duration and one of the TLSOutcome* values.
	RecordTLSHandshake(ctx context.Context, outcome string, d time.Duration)

	// RecordTLSConnection is called once per connection that completed
	// TLS negotiation, tagged with the negotiated tls_version and
	// cipher_suite (raw uint16 values from crypto/tls; the recorder
	// stringifies via tls.VersionName / tls.CipherSuiteName).
	RecordTLSConnection(ctx context.Context, version, cipher uint16)

	// RecordPlaintextRejected is called when a plaintext StartupMessage
	// arrives on a RequireTLS listener, tagged with one of the
	// PlaintextRejectedReason* values.
	RecordPlaintextRejected(ctx context.Context, reason string)

	// RecordSSLRequestDeclined is called when SSLRequest is declined
	// with 'N' because no TLS config is present. Useful before
	// enforcement to size the impact of flipping --pg-require-ssl.
	RecordSSLRequestDeclined(ctx context.Context)
}

// noopAuthMetrics is used when the listener is constructed without an
// explicit recorder, so callers in startup.go can invoke methods
// unconditionally. Eliminates per-call nil checks at every emission site.
type noopAuthMetrics struct{}

func (noopAuthMetrics) RecordSCRAMDuration(context.Context, string, time.Duration) {}
func (noopAuthMetrics) RecordAuthAttempt(context.Context, string)                  {}
func (noopAuthMetrics) RecordCredentialLookup(context.Context, time.Duration)      {}
func (noopAuthMetrics) RecordTLSHandshake(context.Context, string, time.Duration)  {}
func (noopAuthMetrics) RecordTLSConnection(context.Context, uint16, uint16)        {}
func (noopAuthMetrics) RecordPlaintextRejected(context.Context, string)            {}
func (noopAuthMetrics) RecordSSLRequestDeclined(context.Context)                   {}

// metrics returns the connection's auth metrics sink, substituting a noop
// when none was injected. Tests that construct *Conn directly (rather than
// going through the listener accept path) leave authMetrics unset; the
// helper keeps startup-phase call sites free of nil checks without forcing
// every test fixture to wire a recorder.
func (c *Conn) metrics() AuthMetricsRecorder {
	if c.authMetrics == nil {
		return noopAuthMetrics{}
	}
	return c.authMetrics
}
