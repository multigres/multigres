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

package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/provisioner"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectGateways(t *testing.T) {
	results := []*provisioner.ProvisionResult{
		{ServiceName: "etcd", FQDN: "localhost", Ports: map[string]int{"tcp": 2379}},
		{ServiceName: "multigateway", FQDN: "host-a", Ports: map[string]int{"pg_port": 6432, "http_port": 8080}},
		{ServiceName: "multipooler", FQDN: "localhost", Ports: map[string]int{"grpc_port": 5000}},
		{ServiceName: "multigateway", FQDN: "host-b", Ports: map[string]int{"pg_port": 6433}},
		// A multigateway with no pg_port should be skipped.
		{ServiceName: "multigateway", FQDN: "host-c", Ports: map[string]int{"http_port": 8081}},
	}

	got := collectGateways(results)
	require.Len(t, got, 2)
	assert.Equal(t, gatewayEndpoint{name: "multigateway", host: "host-a", port: 6432}, got[0])
	assert.Equal(t, gatewayEndpoint{name: "multigateway", host: "host-b", port: 6433}, got[1])
}

func TestExtractBootstrapCredentials_Defaults(t *testing.T) {
	// Empty config → all defaults.
	got := extractBootstrapCredentials(map[string]any{})
	assert.Equal(t, bootstrapCredentials{
		user:     defaultPostgresUser,
		password: defaultPostgresPassword,
		database: defaultPostgresDatabase,
	}, got)
}

func TestExtractBootstrapCredentials_ReadsFromConfig(t *testing.T) {
	provConfig := map[string]any{
		"cells": map[string]any{
			"zone1": map[string]any{
				"pgctld": map[string]any{
					"pg-user":     "supabase_admin",
					"pg-password": "s3cret",
				},
				"multipooler": map[string]any{
					"database": "supabase",
				},
			},
		},
	}
	got := extractBootstrapCredentials(provConfig)
	assert.Equal(t, bootstrapCredentials{
		user:     "supabase_admin",
		password: "s3cret",
		database: "supabase",
	}, got)
}

func TestExtractBootstrapCredentials_PartialOverridesFallToDefault(t *testing.T) {
	// Only the password is set; user and database should fall back.
	provConfig := map[string]any{
		"cells": map[string]any{
			"zone1": map[string]any{
				"pgctld": map[string]any{
					"pg-password": "rotated",
				},
			},
		},
	}
	got := extractBootstrapCredentials(provConfig)
	assert.Equal(t, bootstrapCredentials{
		user:     defaultPostgresUser,
		password: "rotated",
		database: defaultPostgresDatabase,
	}, got)
}

func TestWaitForGatewaysReady_AllReadyImmediately(t *testing.T) {
	probe := func(_ context.Context, _ string, _ int) error { return nil }
	gws := []gatewayEndpoint{
		{name: "multigateway", host: "host-a", port: 6432},
		{name: "multigateway", host: "host-b", port: 6433},
	}
	require.NoError(t, waitForGatewaysReady(context.Background(), gws, probe))
}

func TestWaitForGatewaysReady_BecomesReadyAfterRetries(t *testing.T) {
	var (
		mu    sync.Mutex
		calls = map[string]int{}
	)
	probe := func(_ context.Context, host string, _ int) error {
		mu.Lock()
		defer mu.Unlock()
		calls[host]++
		// host-a ready on its second poll; host-b on its third.
		switch host {
		case "host-a":
			if calls[host] >= 2 {
				return nil
			}
		case "host-b":
			if calls[host] >= 3 {
				return nil
			}
		}
		return errors.New("not ready")
	}
	gws := []gatewayEndpoint{
		{name: "multigateway", host: "host-a", port: 6432},
		{name: "multigateway", host: "host-b", port: 6433},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, waitForGatewaysReady(ctx, gws, probe))

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, calls["host-a"], 2)
	assert.GreaterOrEqual(t, calls["host-b"], 3)
}

func TestWaitForGatewaysReady_TimeoutReportsPending(t *testing.T) {
	probe := func(_ context.Context, host string, _ int) error {
		if host == "host-a" {
			return nil
		}
		return errors.New("not ready")
	}
	gws := []gatewayEndpoint{
		{name: "multigateway", host: "host-a", port: 6432},
		{name: "multigateway", host: "host-b", port: 6433},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	err := waitForGatewaysReady(ctx, gws, probe)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host-b")
	assert.Contains(t, err.Error(), "6433")
	assert.NotContains(t, err.Error(), "host-a")
}

func TestStartCommand_WaitForBootstrapFlagDefault(t *testing.T) {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddStartCommand(clusterCmd)

	startCmd, _, err := clusterCmd.Find([]string{"start"})
	require.NoError(t, err)

	flag := startCmd.Flag("wait-for-bootstrap")
	require.NotNil(t, flag, "expected --wait-for-bootstrap to be registered")
	assert.Equal(t, "true", flag.DefValue)
}

func TestStartCommand_WaitForBootstrapFalseSkipsProbe(t *testing.T) {
	runStartWithFakeProvisioner(t, "", []*provisioner.ProvisionResult{
		{ServiceName: "multigateway", FQDN: "host-a", Ports: map[string]int{"pg_port": 6432}},
	}, "--wait-for-bootstrap=false", func(err error, probeCalls *int) {
		require.NoError(t, err)
		assert.Zero(t, *probeCalls, "gateway probe must not be invoked when --wait-for-bootstrap=false")
	})
}

func TestStartCommand_WaitForBootstrapTrueWithNoGateways(t *testing.T) {
	runStartWithFakeProvisioner(t, "", []*provisioner.ProvisionResult{
		{ServiceName: "etcd", FQDN: "localhost", Ports: map[string]int{"tcp": 2379}},
	}, "--wait-for-bootstrap=true", func(err error, probeCalls *int) {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no multigateways")
		assert.Zero(t, *probeCalls, "gateway probe must not be invoked when there are no gateways")
	})
}

func TestStartCommand_WaitForBootstrapTrueRunsGatewayProbe(t *testing.T) {
	results := []*provisioner.ProvisionResult{
		{ServiceName: "multigateway", FQDN: "host-a", Ports: map[string]int{"pg_port": 6432}},
	}
	runStartWithFakeProvisioner(t, "", results, "--wait-for-bootstrap=true", func(err error, probeCalls *int) {
		require.NoError(t, err)
		assert.Equal(t, 1, *probeCalls, "gateway probe should run once when it succeeds immediately")
	})
}

// runStartWithFakeProvisioner runs `multigres cluster start` with a fake
// provisioner registered for this test only, the given Bootstrap results,
// and the given extra flag. The gateway probe is replaced with a stub
// that counts calls and returns success. The optional extraYAML is
// appended to the temp multigres.yaml so tests can exercise password /
// user / database extraction.
func runStartWithFakeProvisioner(
	t *testing.T,
	extraYAML string,
	bootstrapResults []*provisioner.ProvisionResult,
	extraFlag string,
	check func(err error, probeCalls *int),
) {
	t.Helper()

	var probeCalls int
	restoreProbe := overrideRunGatewayProbe(func(_ context.Context, _ string, _ int, _ bootstrapCredentials) error {
		probeCalls++
		return nil
	})
	defer restoreProbe()

	const provisionerName = "fake-wait-for-bootstrap"
	provisioner.RegisterProvisioner(provisionerName, func() (provisioner.Provisioner, error) {
		return &fakeProvisioner{bootstrap: bootstrapResults}, nil
	})

	configDir := t.TempDir()
	configFile := filepath.Join(configDir, "multigres.yaml")
	yaml := "provisioner: " + provisionerName + "\n" + extraYAML
	require.NoError(t, os.WriteFile(configFile, []byte(yaml), 0o600))

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")
	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddStartCommand(clusterCmd)

	args := []string{"cluster", "start", "--config-path", configDir}
	if extraFlag != "" {
		args = append(args, extraFlag)
	}
	rootCmd.SetArgs(args)

	check(rootCmd.Execute(), &probeCalls)
}

// overrideRunGatewayProbe swaps the package-level runGatewayProbeFn used
// by start() for the duration of a test and returns a restorer. Tests
// need this hook because the production probe is constructed as a
// closure in start() that captures bootstrapCredentials.
func overrideRunGatewayProbe(fn func(context.Context, string, int, bootstrapCredentials) error) func() {
	prev := runGatewayProbeFn
	runGatewayProbeFn = fn
	return func() { runGatewayProbeFn = prev }
}

// fakeProvisioner is a stub Provisioner used only by start_test.go to drive
// the start command without touching real services.
type fakeProvisioner struct {
	bootstrap []*provisioner.ProvisionResult
}

func (f *fakeProvisioner) Name() string                          { return "fake-wait-for-bootstrap" }
func (f *fakeProvisioner) LoadConfig(_ []string) error           { return nil }
func (f *fakeProvisioner) ValidateConfig(_ map[string]any) error { return nil }
func (f *fakeProvisioner) DefaultConfig(_ []string, _ map[string]string) map[string]any {
	return nil
}

func (f *fakeProvisioner) Bootstrap(_ context.Context) ([]*provisioner.ProvisionResult, error) {
	return f.bootstrap, nil
}

func (f *fakeProvisioner) Teardown(_ context.Context, _ bool) error { return nil }

func (f *fakeProvisioner) ProvisionDatabase(_ context.Context, _ string, _ string) ([]*provisioner.ProvisionResult, error) {
	return nil, nil
}

func (f *fakeProvisioner) DeprovisionDatabase(_ context.Context, _ string, _ string) error {
	return nil
}
