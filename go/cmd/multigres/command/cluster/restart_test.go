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
	"os"
	"path/filepath"
	"testing"

	"github.com/multigres/multigres/go/provisioner"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartCommand_WaitForBootstrapFlagDefault(t *testing.T) {
	clusterCmd := &cobra.Command{Use: "cluster"}
	AddRestartCommand(clusterCmd)

	restartCmd, _, err := clusterCmd.Find([]string{"restart"})
	require.NoError(t, err)

	flag := restartCmd.Flag("wait-for-bootstrap")
	require.NotNil(t, flag, "expected --wait-for-bootstrap to be registered")
	assert.Equal(t, "true", flag.DefValue)
}

func TestRestartCommand_WaitForBootstrapDefaultRunsGatewayProbe(t *testing.T) {
	runRestartWithFakeProvisioner(t, "", func(err error, events []string, probeCalls int) {
		require.NoError(t, err)
		assert.Equal(t, []string{"teardown", "bootstrap", "probe"}, events)
		assert.Equal(t, 1, probeCalls, "gateway probe should run once when it succeeds immediately")
	})
}

func TestRestartCommand_WaitForBootstrapFalseSkipsGatewayProbe(t *testing.T) {
	runRestartWithFakeProvisioner(t, "--wait-for-bootstrap=false", func(err error, events []string, probeCalls int) {
		require.NoError(t, err)
		assert.Equal(t, []string{"teardown", "bootstrap"}, events)
		assert.Zero(t, probeCalls, "gateway probe must not be invoked when --wait-for-bootstrap=false")
	})
}

func runRestartWithFakeProvisioner(
	t *testing.T,
	extraFlag string,
	check func(err error, events []string, probeCalls int),
) {
	t.Helper()

	var events []string
	var probeCalls int
	restoreProbe := overrideRunGatewayProbe(func(_ context.Context, _ string, _ int, _ bootstrapCredentials) error {
		events = append(events, "probe")
		probeCalls++
		return nil
	})
	t.Cleanup(restoreProbe)

	const provisionerName = "fake-wait-for-bootstrap"
	bootstrapResults := []*provisioner.ProvisionResult{
		{ServiceName: "multigateway", FQDN: "host-a", Ports: map[string]int{"pg_port": 6432}},
	}
	provisioner.RegisterProvisioner(provisionerName, func() (provisioner.Provisioner, error) {
		return &restartFakeProvisioner{
			fakeProvisioner: fakeProvisioner{bootstrap: bootstrapResults},
			events:          &events,
		}, nil
	})
	t.Cleanup(func() {
		provisioner.RegisterProvisioner(provisionerName, func() (provisioner.Provisioner, error) {
			return &fakeProvisioner{}, nil
		})
	})

	configDir := t.TempDir()
	configFile := filepath.Join(configDir, "multigres.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("provisioner: "+provisionerName+"\n"), 0o600))

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")
	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddRestartCommand(clusterCmd)

	args := []string{"cluster", "restart", "--config-path", configDir}
	if extraFlag != "" {
		args = append(args, extraFlag)
	}
	rootCmd.SetArgs(args)

	check(rootCmd.Execute(), events, probeCalls)
}

type restartFakeProvisioner struct {
	fakeProvisioner
	events *[]string
}

func (f *restartFakeProvisioner) Bootstrap(ctx context.Context) ([]*provisioner.ProvisionResult, error) {
	*f.events = append(*f.events, "bootstrap")
	return f.fakeProvisioner.Bootstrap(ctx)
}

func (f *restartFakeProvisioner) Teardown(_ context.Context, _ bool) error {
	*f.events = append(*f.events, "teardown")
	return nil
}
