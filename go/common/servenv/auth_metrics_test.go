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
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/metadata"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// setupAuthMetricsTest installs the shared test telemetry stack and returns
// a fresh authMetrics wired to its ManualReader - mirroring
// multigateway's setupGatewayMetrics, this must construct authMetrics
// *after* InitTelemetry, in every test, rather than reuse a package-level
// singleton (see the comment on authMetrics for why).
func setupAuthMetricsTest(t *testing.T) (*authMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-servenv"))
	return newAuthMetrics(), setup.MetricReader
}

func findAuthAttempts(t *testing.T, reader *sdkmetric.ManualReader) metricdata.Sum[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "mg.servenv.auth.attempts" {
				sum, ok := m.Data.(metricdata.Sum[int64])
				require.True(t, ok, "expected Sum[int64]")
				return sum
			}
		}
	}
	t.Fatal("mg.servenv.auth.attempts not emitted")
	return metricdata.Sum[int64]{}
}

// countsByOutcome collapses data points to outcome -> value, for a given
// plugin tag, so tests don't need to hardcode attribute ordering.
func countsByOutcome(t *testing.T, sum metricdata.Sum[int64], plugin string) map[string]int64 {
	t.Helper()
	counts := map[string]int64{}
	for _, dp := range sum.DataPoints {
		p, _ := dp.Attributes.Value(attribute.Key("plugin"))
		if p.AsString() != plugin {
			continue
		}
		o, _ := dp.Attributes.Value(attribute.Key("outcome"))
		counts[o.AsString()] += dp.Value
	}
	return counts
}

func TestJWTAuthPlugin_VerifyToken_RecordsMetrics(t *testing.T) {
	m, reader := setupAuthMetricsTest(t)
	jwks := newTestJWKSet(t)

	t.Run("success", func(t *testing.T) {
		plugin := jwks.staticVerifier(t, testJWTIssuer, "")
		plugin.metrics = m
		token := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid)
		_, err := plugin.VerifyToken(token)
		require.NoError(t, err)
	})

	t.Run("invalid token", func(t *testing.T) {
		plugin := jwks.staticVerifier(t, testJWTIssuer, "")
		plugin.metrics = m
		_, err := plugin.VerifyToken("not-a-valid-jwt")
		require.Error(t, err)
	})

	t.Run("subject not authorized", func(t *testing.T) {
		plugin := jwks.staticVerifier(t, testJWTIssuer, "", "allowed-subject")
		plugin.metrics = m
		token := signToken(t, jwt.SigningMethodRS256, jwks.privateKey, jwks.kid, withSub("someone-else"))
		_, err := plugin.VerifyToken(token)
		require.Error(t, err)
	})

	counts := countsByOutcome(t, findAuthAttempts(t, reader), "jwt")
	assert.Equal(t, int64(1), counts[AuthOutcomeSuccess])
	assert.Equal(t, int64(1), counts[AuthOutcomeInvalidToken])
	assert.Equal(t, int64(1), counts[AuthOutcomeSubjectNotAuthorized])
}

func TestJWTAuthPlugin_Authenticate_RecordsMetrics(t *testing.T) {
	m, reader := setupAuthMetricsTest(t)
	jwks := newTestJWKSet(t)
	plugin := jwks.staticVerifier(t, testJWTIssuer, "")
	plugin.metrics = m

	_, err := plugin.Authenticate(context.Background(), "/multiadmin.MultiadminService/GetCell")
	require.Error(t, err, "no metadata in context")

	_, err = plugin.Authenticate(metadata.NewIncomingContext(context.Background(), metadata.MD{}), "/x")
	require.Error(t, err, "no authorization header")

	_, err = plugin.Authenticate(
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic dXNlcjpwYXNz")),
		"/x")
	require.Error(t, err, "not a bearer token")

	counts := countsByOutcome(t, findAuthAttempts(t, reader), "jwt")
	assert.Equal(t, int64(1), counts[AuthOutcomeNoMetadata])
	assert.Equal(t, int64(1), counts[AuthOutcomeMissingHeader])
	assert.Equal(t, int64(1), counts[AuthOutcomeMalformedHeader])
}

func TestMtlsAuthPlugin_Authenticate_RecordsMetrics(t *testing.T) {
	m, reader := setupAuthMetricsTest(t)
	plugin := &MtlsAuthPlugin{clientCertSubstrings: []string{"operator"}, metrics: m}

	_, err := plugin.Authenticate(context.Background(), "/x")
	require.Error(t, err, "no peer connection info")

	_, err = plugin.Authenticate(fakeGRPCContext(generateTestPeerCert(t, "some-other-service"), ""), "/x")
	require.Error(t, err, "cert doesn't match allowed substrings")

	_, err = plugin.Authenticate(fakeGRPCContext(generateTestPeerCert(t, "operator.internal"), ""), "/x")
	require.NoError(t, err, "cert matches allowed substrings")

	counts := countsByOutcome(t, findAuthAttempts(t, reader), "mtls")
	assert.Equal(t, int64(1), counts[AuthOutcomeNoPeerInfo])
	assert.Equal(t, int64(1), counts[AuthOutcomeCertNotAuthorized])
	assert.Equal(t, int64(1), counts[AuthOutcomeSuccess])
}
