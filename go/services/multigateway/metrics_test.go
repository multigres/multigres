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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// setupGatewayMetrics installs a ManualReader-backed MeterProvider and
// constructs a fresh GatewayMetrics. The provider is restored on cleanup
// so other tests are not affected. Returns the metrics handle and the
// reader so the caller can collect snapshots.
func setupGatewayMetrics(t *testing.T) (*GatewayMetrics, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	m, err := NewGatewayMetrics()
	require.NoError(t, err)
	return m, reader
}

// findMetric returns the named metric's data from the most-recent collect,
// or nil when the metric has not been emitted yet.
func findMetric(t *testing.T, reader *sdkmetric.ManualReader, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

// attrLookup returns the string-valued attribute on the data point matching
// key, or "" if absent. Used to assert outcome / reason label values.
func attrLookup(t *testing.T, attrs attribute.Set, key string) string {
	t.Helper()
	v, ok := attrs.Value(attribute.Key(key))
	if !ok {
		return ""
	}
	return v.AsString()
}

func TestGatewayMetrics_RecordSCRAMDuration_TagsOutcome(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordSCRAMDuration(ctx, AuthOutcomeSuccess, 5*time.Millisecond)
	m.RecordSCRAMDuration(ctx, AuthOutcomeBadPassword, 7*time.Millisecond)
	m.RecordSCRAMDuration(ctx, AuthOutcomeBadPassword, 9*time.Millisecond)

	agg := findMetric(t, reader, "mg.gateway.auth.scram.duration")
	require.NotNil(t, agg, "scram duration histogram not emitted")

	hist, ok := agg.(metricdata.Histogram[float64])
	require.True(t, ok, "expected Histogram[float64]")
	require.Len(t, hist.DataPoints, 2, "expected one bucket per outcome")

	counts := map[string]uint64{}
	for _, dp := range hist.DataPoints {
		counts[attrLookup(t, dp.Attributes, "outcome")] = dp.Count
	}
	require.Equal(t, uint64(1), counts[AuthOutcomeSuccess])
	require.Equal(t, uint64(2), counts[AuthOutcomeBadPassword])
}

func TestGatewayMetrics_RecordAuthAttempt_Increments(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordAuthAttempt(ctx, AuthOutcomeSuccess)
	m.RecordAuthAttempt(ctx, AuthOutcomeSuccess)
	m.RecordAuthAttempt(ctx, AuthOutcomeUserNotFound)

	agg := findMetric(t, reader, "mg.gateway.auth.attempts")
	require.NotNil(t, agg)
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok, "expected Sum[int64]")

	values := map[string]int64{}
	for _, dp := range sum.DataPoints {
		values[attrLookup(t, dp.Attributes, "outcome")] = dp.Value
	}
	require.Equal(t, int64(2), values[AuthOutcomeSuccess])
	require.Equal(t, int64(1), values[AuthOutcomeUserNotFound])
}

func TestGatewayMetrics_RecordCredentialLookup_RateAndDuration(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordCredentialLookup(ctx, 3*time.Millisecond)
	m.RecordCredentialLookup(ctx, 4*time.Millisecond)

	durAgg := findMetric(t, reader, "mg.gateway.auth.credential_lookup.duration")
	require.NotNil(t, durAgg)
	hist, ok := durAgg.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, hist.DataPoints, 1)
	require.Equal(t, uint64(2), hist.DataPoints[0].Count)

	rateAgg := findMetric(t, reader, "mg.gateway.auth.credential_lookup.rate")
	require.NotNil(t, rateAgg)
	sum, ok := rateAgg.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(2), sum.DataPoints[0].Value)
}

func TestGatewayMetrics_RecordTLSConnection_TagsVersionAndCipher(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordTLSConnection(ctx, tls.VersionTLS13, tls.TLS_AES_128_GCM_SHA256)

	agg := findMetric(t, reader, "mg.gateway.tls.connections")
	require.NotNil(t, agg)
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	dp := sum.DataPoints[0]
	require.Equal(t, "TLS 1.3", attrLookup(t, dp.Attributes, "tls_version"))
	require.Equal(t, tls.CipherSuiteName(tls.TLS_AES_128_GCM_SHA256),
		attrLookup(t, dp.Attributes, "cipher_suite"))
	require.Equal(t, int64(1), dp.Value)
}

func TestGatewayMetrics_RecordPlaintextRejected_TagsReason(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordPlaintextRejected(ctx, PlaintextRejectedReasonNoSSLRequest)
	m.RecordPlaintextRejected(ctx, PlaintextRejectedReasonNoSSLRequest)
	m.RecordPlaintextRejected(ctx, PlaintextRejectedReasonTLSDisabledByServer)

	agg := findMetric(t, reader, "mg.gateway.tls.plaintext_rejected")
	require.NotNil(t, agg)
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok)

	values := map[string]int64{}
	for _, dp := range sum.DataPoints {
		values[attrLookup(t, dp.Attributes, "reason")] = dp.Value
	}
	require.Equal(t, int64(2), values[PlaintextRejectedReasonNoSSLRequest])
	require.Equal(t, int64(1), values[PlaintextRejectedReasonTLSDisabledByServer])
}

func TestGatewayMetrics_RecordSSLRequestDeclined_Increments(t *testing.T) {
	m, reader := setupGatewayMetrics(t)
	ctx := context.Background()

	m.RecordSSLRequestDeclined(ctx)
	m.RecordSSLRequestDeclined(ctx)
	m.RecordSSLRequestDeclined(ctx)

	agg := findMetric(t, reader, "mg.gateway.tls.sslrequest_declined")
	require.NotNil(t, agg)
	sum, ok := agg.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, sum.DataPoints, 1)
	require.Equal(t, int64(3), sum.DataPoints[0].Value)
}

func TestGatewayMetrics_NilReceiverIsNoop(t *testing.T) {
	// All recorders must tolerate a nil receiver so call sites can stay
	// unconditional even when metric init failed. Panic here would
	// surface as a crash at the auth-rejection hot path.
	var m *GatewayMetrics
	ctx := context.Background()

	require.NotPanics(t, func() {
		m.RecordSCRAMDuration(ctx, AuthOutcomeSuccess, time.Second)
		m.RecordAuthAttempt(ctx, AuthOutcomeSuccess)
		m.RecordCredentialLookup(ctx, time.Second)
		m.RecordTLSHandshake(ctx, TLSOutcomeSuccess, time.Second)
		m.RecordTLSConnection(ctx, tls.VersionTLS13, tls.TLS_AES_128_GCM_SHA256)
		m.RecordPlaintextRejected(ctx, PlaintextRejectedReasonNoSSLRequest)
		m.RecordSSLRequestDeclined(ctx)
	})
}
