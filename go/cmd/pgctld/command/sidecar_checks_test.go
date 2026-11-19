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

package command

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/repocreds"
	"github.com/multigres/multigres/go/tools/telemetry"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// fakeCurlOnPath puts an executable named curl first on PATH.
func fakeCurlOnPath(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "curl"), []byte("#!/bin/sh\n"), 0o755))
	t.Setenv("PATH", bin)
}

// getGaugeInt64 extracts a named Int64 gauge from collected metric data.
func getGaugeInt64(t *testing.T, reader *sdkmetric.ManualReader, name string) *metricdata.Gauge[int64] {
	t.Helper()

	var metricData metricdata.ResourceMetrics
	err := reader.Collect(t.Context(), &metricData)
	require.NoError(t, err)

	for _, scopeMetric := range metricData.ScopeMetrics {
		for _, m := range scopeMetric.Metrics {
			if m.Name == name {
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok, "expected Gauge[int64] data type for %s", name)
				return &gauge
			}
		}
	}
	return nil
}

// gaugeValue returns the single data point value from a gauge, or 0 if nil.
func gaugeValue(g *metricdata.Gauge[int64]) int64 {
	if g == nil || len(g.DataPoints) == 0 {
		return 0
	}
	return g.DataPoints[0].Value
}

// credentialEnvCases names the variables pgctld must report. It is written
// out here, not read from the list under test, so a name dropped from that
// list fails a test.
func credentialEnvCases() []string {
	return []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_DEFAULT_PROFILE",
		"AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE", "AWS_PROFILE",
		"AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"PGBACKREST_REPO1_S3_KEY", "PGBACKREST_REPO1_S3_KEY_SECRET", "PGBACKREST_REPO2_S3_TOKEN", "PGBACKREST_REPO1_CIPHER_PASS",
	}
}

// clearCredentialEnv removes every credential variable for the test: a
// variable set to the empty string still counts as exposed.
func clearCredentialEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); repocreds.IsEnvVar(name) {
			t.Setenv(name, "")
			require.NoError(t, os.Unsetenv(name))
		}
	}
}

func TestStartup_RunsNoPgbackrestServer(t *testing.T) {
	fakeCurlOnPath(t)
	clearCredentialEnv(t)
	t.Setenv(constants.PgDataDirEnvVar, filepath.Join(t.TempDir(), "pg_data"))
	t.Setenv("PGPASSFILE", "")

	setup := telemetry.SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))

	poolerDir := t.TempDir()
	svc, err := NewPgCtldService(testLogger(), testServiceConfig, 30, poolerDir, "localhost")
	require.NoError(t, err)
	t.Cleanup(svc.Close)

	assert.NoDirExists(t, filepath.Join(poolerDir, "pgbackrest"))
	assert.Empty(t, os.Getenv("PGPASSFILE"), "pgctld must not point pgbackrest at a pgpass")

	resp, err := svc.Status(t.Context(), &pb.StatusRequest{})
	require.NoError(t, err)
	assert.Zero(t, resp.PgbackrestPort)
	assert.Empty(t, resp.PgbackrestCertDir)
	assert.Nil(t, resp.PgbackrestStatus)

	// The sidecar's own gauge reports the 8432 server; pgctld publishes none.
	assert.Nil(t, getGaugeInt64(t, setup.MetricReader, "pgbackrest.server.up"))
	assert.Nil(t, getGaugeInt64(t, setup.MetricReader, "pgbackrest.server.restart_count"))
	assert.Nil(t, getGaugeInt64(t, setup.MetricReader, "pgbackrest.server.uptime"))
}

func TestStartup_RefusesWithoutCurl(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	_, err := NewPgCtldService(testLogger(), testServiceConfig, 30, t.TempDir(), "localhost")
	require.Error(t, err)
	assert.Equal(t, errSidecarNeedsCurl.Error(), err.Error())
}

func TestStartup_CredentialExposedMetric(t *testing.T) {
	for _, name := range credentialEnvCases() {
		t.Run(name, func(t *testing.T) {
			fakeCurlOnPath(t)
			clearCredentialEnv(t)
			setup := telemetry.SetupTestTelemetry(t)
			require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))

			t.Setenv(name, "x")
			_, err := startSidecarChecks(testLogger())
			require.NoError(t, err, "an exposed credential must not stop startup")

			g := getGaugeInt64(t, setup.MetricReader, "credential_exposed")
			require.NotNil(t, g)
			assert.Equal(t, int64(1), gaugeValue(g))
		})
	}

	t.Run("set but empty", func(t *testing.T) {
		fakeCurlOnPath(t)
		clearCredentialEnv(t)
		setup := telemetry.SetupTestTelemetry(t)
		require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))

		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		_, err := startSidecarChecks(testLogger())
		require.NoError(t, err)
		g := getGaugeInt64(t, setup.MetricReader, "credential_exposed")
		require.NotNil(t, g)
		assert.Equal(t, int64(1), gaugeValue(g), "a variable that is present counts, whatever its value")
	})

	t.Run("clean environment", func(t *testing.T) {
		fakeCurlOnPath(t)
		clearCredentialEnv(t)
		setup := telemetry.SetupTestTelemetry(t)
		require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))

		_, err := startSidecarChecks(testLogger())
		require.NoError(t, err)

		g := getGaugeInt64(t, setup.MetricReader, "credential_exposed")
		require.NotNil(t, g)
		assert.Equal(t, int64(0), gaugeValue(g))
	})
}
