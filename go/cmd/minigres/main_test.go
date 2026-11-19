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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestMinigres returns a minigres with its flags registered on fs.
func newTestMinigres(t *testing.T) (*minigres, *pflag.FlagSet) {
	t.Helper()
	m := newMinigres()
	fs := pflag.NewFlagSet("minigres", pflag.ContinueOnError)
	require.NoError(t, m.registerFlags(fs))
	return m, fs
}

func TestCreateMinigresCommand_RegistersAllFlags(t *testing.T) {
	cmd, err := CreateMinigresCommand()
	require.NoError(t, err)

	for _, name := range []string{
		"http-port", "grpc-port", "topo-global-server-addresses", "topo-global-root", // shared, defined once
		"cell", "service-id", "pg-port", // gateway definitions, shared with the pooler where meaningful
		"pgctld-addr", "database", "table-group", "shard", "pooler-dir", // pooler-only
	} {
		assert.NotNil(t, cmd.Flags().Lookup(name), "flag %q should be registered", name)
	}
}

// TestCreateMinigresCommand_RejectsSeveralPoolersFlags checks that the features
// that need several poolers or gateways cannot be configured.
func TestCreateMinigresCommand_RejectsSeveralPoolersFlags(t *testing.T) {
	for _, name := range []string{
		"pg-replica-port", "low-replication-lag-ms", "high-replication-lag-tolerance-ms", // replica reads
		"buffer-enabled",                                        // failover buffering
		"multipooler-grpc-cert", "multipooler-grpc-require-tls", // remote poolers, cancel forwarding
	} {
		t.Run(name, func(t *testing.T) {
			_, fs := newTestMinigres(t)
			assert.Nil(t, fs.Lookup(name))
			assert.ErrorContains(t, fs.Parse([]string{"--" + name + "=1"}), "unknown flag")
		})
	}
}

func TestPoolerPgPortNotSetByGatewayPgPort(t *testing.T) {
	m, fs := newTestMinigres(t)
	require.NoError(t, fs.Parse([]string{"--pg-port=15432"}))

	poolerPgPort := m.poolerFlags.Lookup("pg-port")
	require.NotNil(t, poolerPgPort)
	assert.False(t, poolerPgPort.Changed, "the pooler must adopt pg-port from pgctld, not take the gateway's client port")
}

func TestPoolerOnlyFlagsReachPoolerFlagSet(t *testing.T) {
	m, fs := newTestMinigres(t)
	require.NoError(t, fs.Parse([]string{"--pgctld-addr=localhost:9999"}))

	f := m.poolerFlags.Lookup("pgctld-addr")
	require.NotNil(t, f)
	assert.True(t, f.Changed)
	assert.Equal(t, "localhost:9999", f.Value.String())
}

func TestCopySharedFlags(t *testing.T) {
	m, fs := newTestMinigres(t)
	require.NoError(t, fs.Parse([]string{"--cell=zone1", "--service-id=svc1", "--enable-slot-based-replication=true"}))

	require.NoError(t, copySharedFlags(fs, m.poolerFlags))

	for name, want := range map[string]string{
		"cell":                          "zone1",
		"service-id":                    "svc1",
		"enable-slot-based-replication": "true",
	} {
		f := m.poolerFlags.Lookup(name)
		require.NotNil(t, f, name)
		assert.True(t, f.Changed, name)
		assert.Equal(t, want, f.Value.String(), name)
	}
}

func TestCopySharedFlags_UnsetFlagsStayUnset(t *testing.T) {
	m, fs := newTestMinigres(t)
	require.NoError(t, fs.Parse(nil))

	require.NoError(t, copySharedFlags(fs, m.poolerFlags))

	assert.False(t, m.poolerFlags.Lookup("cell").Changed)
}

func TestMergePoolerFlags_Collision(t *testing.T) {
	fs := pflag.NewFlagSet("root", pflag.ContinueOnError)
	fs.String("pooler-dir", "", "")
	poolerFlags := pflag.NewFlagSet("pooler", pflag.ContinueOnError)
	poolerFlags.String("pooler-dir", "", "")

	err := mergePoolerFlags(fs, poolerFlags)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `"pooler-dir"`)
}

func TestSetDefaultIfUnset(t *testing.T) {
	fs := pflag.NewFlagSet("root", pflag.ContinueOnError)
	fs.String("table-group", "", "")
	fs.String("shard", "", "")
	require.NoError(t, fs.Parse([]string{"--shard=explicit"}))

	require.NoError(t, setDefaultIfUnset(fs, "table-group", "default"))
	require.NoError(t, setDefaultIfUnset(fs, "shard", "0-inf"))

	assert.Equal(t, "default", fs.Lookup("table-group").Value.String())
	assert.Equal(t, "explicit", fs.Lookup("shard").Value.String(), "an operator-set value must win")
}

// TestRun_TopoMissingAddresses runs the command end to end up to opening the
// topology, which fails without addresses.
func TestRun_TopoMissingAddresses(t *testing.T) {
	cmd, err := CreateMinigresCommand()
	require.NoError(t, err)
	cmd.SetArgs([]string{"--config-file-not-found-handling", "ignore"})

	err = cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topo-global-server-addresses must be configured")
}

// newPreRunCommand returns a minigres and a command with its flags registered
// and parsed from args, ready for preRun.
func newPreRunCommand(t *testing.T, args ...string) (*minigres, *cobra.Command) {
	t.Helper()
	m := newMinigres()
	cmd := &cobra.Command{Use: "minigres"}
	require.NoError(t, m.registerFlags(cmd.Flags()))
	require.NoError(t, cmd.Flags().Parse(args))
	return m, cmd
}

func TestPreRun_OneServiceIDForProcessAndBothHalves(t *testing.T) {
	m, cmd := newPreRunCommand(t)

	require.NoError(t, m.preRun(cmd))

	id := cmd.Flags().Lookup("service-id").Value.String()
	require.NotEmpty(t, id)
	assert.Equal(t, id, m.poolerFlags.Lookup("service-id").Value.String())
	assert.Equal(t, id, m.pooler.ServiceIdentity().ServiceInstanceID)
}

func TestPreRun_KeepsConfiguredServiceID(t *testing.T) {
	m, cmd := newPreRunCommand(t, "--service-id=svc1")

	require.NoError(t, m.preRun(cmd))

	assert.Equal(t, "svc1", cmd.Flags().Lookup("service-id").Value.String())
	assert.Equal(t, "svc1", m.pooler.ServiceIdentity().ServiceInstanceID)
}

func TestPreRun_KeepsServiceIDFromEnvironment(t *testing.T) {
	t.Setenv("MT_SERVICE_ID", "svc-env")
	m, cmd := newPreRunCommand(t)

	require.NoError(t, m.preRun(cmd))

	assert.Equal(t, "svc-env", m.pooler.ServiceIdentity().ServiceInstanceID)
	assert.False(t, cmd.Flags().Lookup("service-id").Changed, "an ID from the environment must not be replaced")
}

// Minigres takes the multipooler's flags, so it refuses the removed pgBackRest
// flags the same way: named, and before anything starts.
func TestRemovedPgBackRestFlagsRefuseStartup(t *testing.T) {
	for _, arg := range []string{
		"--pgbackrest-cert-file=/certs/pgbackrest.crt",
		"--pgbackrest-key-file=/certs/pgbackrest.key",
		"--pgbackrest-ca-file=/certs/ca.crt",
		"--pgbackrest-port=8432",
		"--pgbackrest-cipher-key-file=/secrets/pgbackrest-cipher/keys.json",
	} {
		t.Run(arg, func(t *testing.T) {
			cmd, err := CreateMinigresCommand()
			require.NoError(t, err)
			cmd.SetArgs([]string{"--cell", "zone1", "--pooler-dir", t.TempDir(), arg})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			ran := false
			cmd.RunE = func(*cobra.Command, []string) error { ran = true; return nil }

			err = cmd.Execute()

			name, _, _ := strings.Cut(arg, "=")
			require.EqualError(t, err, "flag "+name+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this minigres needs a multigres-operator that deploys that sidecar and does not pass the flag")
			assert.False(t, ran, "minigres must not start")
		})
	}
}

// See the multipooler's test of the same name: the environment variable and
// the config file keys of the removed settings stop minigres as well.
func TestRemovedPgBackRestSettingsRefuseStartupFromEnvironmentAndConfigFile(t *testing.T) {
	execute := func(t *testing.T, extra ...string) (error, bool) {
		t.Helper()
		cmd, err := CreateMinigresCommand()
		require.NoError(t, err)
		cmd.SetArgs(append([]string{"--cell", "zone1", "--pooler-dir", t.TempDir()}, extra...))
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		ran := false
		cmd.RunE = func(*cobra.Command, []string) error { ran = true; return nil }
		return cmd.Execute(), ran
	}

	t.Run("environment variable", func(t *testing.T) {
		t.Setenv("MULTIGRES_PGBACKREST_CIPHER_KEY_FILE", "/secrets/pgbackrest-cipher/keys.json")
		err, ran := execute(t)
		require.EqualError(t, err, "environment variable MULTIGRES_PGBACKREST_CIPHER_KEY_FILE was removed: pgBackRest runs in the pgbackrestd sidecar, which holds the cipher keys, and this minigres needs a multigres-operator that deploys that sidecar and does not set the variable")
		assert.False(t, ran, "minigres must not start")
	})

	for _, key := range []string{
		"multipooler.pgbackrest-cert-file", "multipooler.pgbackrest-key-file", "multipooler.pgbackrest-ca-file",
		"multipooler.pgbackrest-port", "multipooler.pgbackrest-cipher-key-file", "pgbackrest-port",
	} {
		t.Run("config file key "+key, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "minigres.yaml")
			body := key + ": 8432\n"
			if half, name, nested := strings.Cut(key, "."); nested {
				body = half + ":\n  " + name + ": 8432\n"
			}
			require.NoError(t, os.WriteFile(configFile, []byte(body), 0o600))
			err, ran := execute(t, "--config-file", configFile)
			require.ErrorContains(t, err, "config file setting "+key+" was removed: pgBackRest runs in the pgbackrestd sidecar, and this minigres needs a multigres-operator that deploys that sidecar and does not set it")
			assert.False(t, ran, "minigres must not start")
		})
	}
}
