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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// Without this the pgbackrest.server.* gauges resolve against the global
// no-op provider and export nowhere.
func TestServe_InitialisesTelemetry(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	tel := telemetry.NewTelemetry()
	shutdown, err := initServeTelemetry(t.Context(), tel)
	require.NoError(t, err)
	defer shutdown()

	assert.IsType(t, &sdkmetric.MeterProvider{}, otel.GetMeterProvider())
	assert.IsType(t, &sdkmetric.MeterProvider{}, tel.GetMeterProvider())
}

func sandboxFlag(t *testing.T) string {
	t.Helper()
	cmd := newServeCommand(func() *slog.Logger { return slog.Default() }, telemetry.NewTelemetry())
	f := cmd.Flags().Lookup("sandbox")
	require.NotNil(t, f)
	return f.Value.String()
}

func TestServe_SandboxFlagDefaultsFromTheEnvironment(t *testing.T) {
	t.Setenv(constants.PgBackRestSandboxEnvVar, "")
	assert.Equal(t, constants.SandboxRequired, sandboxFlag(t))

	t.Setenv(constants.PgBackRestSandboxEnvVar, constants.SandboxAuto)
	assert.Equal(t, constants.SandboxAuto, sandboxFlag(t))

	t.Setenv(constants.PgBackRestSandboxEnvVar, constants.SandboxOff)
	assert.Equal(t, constants.SandboxOff, sandboxFlag(t))
}

func TestServe_SandboxFlagWinsOverTheEnvironment(t *testing.T) {
	t.Setenv(constants.PgBackRestSandboxEnvVar, constants.SandboxOff)
	cmd := newServeCommand(func() *slog.Logger { return slog.Default() }, telemetry.NewTelemetry())
	require.NoError(t, cmd.Flags().Parse([]string{"--sandbox=required"}))
	assert.Equal(t, constants.SandboxRequired, cmd.Flags().Lookup("sandbox").Value.String())
}
