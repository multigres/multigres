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
	"testing"

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
