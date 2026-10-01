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

package multipooler

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// flagNames returns the names of the flags registered on fs.
func flagNames(fs *pflag.FlagSet) map[string]bool {
	names := map[string]bool{}
	fs.VisitAll(func(f *pflag.Flag) { names[f.Name] = true })
	return names
}

func TestRegisterFlags_SingleProcessModeSkipsSharedFlags(t *testing.T) {
	sharedReg := viperutil.NewRegistry()
	tel := telemetry.NewTelemetry()
	senv := servenv.NewServEnvWithConfig(sharedReg, servenv.NewLogger(sharedReg, tel), viperutil.NewViperConfig(sharedReg), tel)
	grpcServer := servenv.NewGrpcServer(sharedReg)

	mp := NewMultipooler(tel, WithSingleProcessMode(servenv.ProcessResources{
		ServEnv:    senv,
		GrpcServer: grpcServer,
		TopoStore:  func() topoclient.Store { return nil },
	}))
	fs := pflag.NewFlagSet("pooler", pflag.ContinueOnError)
	mp.RegisterFlags(fs)
	names := flagNames(fs)

	for _, name := range []string{"http-port", "grpc-port", "topo-global-root"} {
		assert.False(t, names[name], "flag %q is registered by main in single-process mode", name)
	}
	for _, name := range []string{"pgctld-addr", "database", "pg-port"} {
		assert.True(t, names[name], "pooler flag %q should still be registered", name)
	}
}

func TestWithSingleProcessMode_PanicsOnIncompleteResources(t *testing.T) {
	assert.PanicsWithValue(t,
		"multipooler: invalid ProcessResources: GrpcServer is required\nTopoStore is required",
		func() {
			reg := viperutil.NewRegistry()
			WithSingleProcessMode(servenv.ProcessResources{ServEnv: servenv.NewServEnv(reg)})
		})
}

func TestRegisterFlags_DefaultOwnsEverything(t *testing.T) {
	mp := NewMultipooler(telemetry.NewTelemetry())
	fs := pflag.NewFlagSet("pooler", pflag.ContinueOnError)
	mp.RegisterFlags(fs)
	names := flagNames(fs)

	for _, name := range []string{"http-port", "grpc-port", "topo-global-root", "pgctld-addr"} {
		require.True(t, names[name], "flag %q should be registered without options", name)
	}
}

func TestConsensusEnabled_NeverForStaticLeader(t *testing.T) {
	sharedReg := viperutil.NewRegistry()
	tel := telemetry.NewTelemetry()
	resources := servenv.ProcessResources{
		ServEnv:    servenv.NewServEnvWithConfig(sharedReg, servenv.NewLogger(sharedReg, tel), viperutil.NewViperConfig(sharedReg), tel),
		GrpcServer: servenv.NewGrpcServer(sharedReg),
		TopoStore:  func() topoclient.Store { return nil },
	}
	mp := NewMultipooler(tel, WithSingleProcessMode(resources))

	// The consensus service stays in the service map, as in Multigres, so only
	// the static leader keeps it from being registered.
	assert.False(t, mp.consensusEnabled())
}

// newSingleProcessResources returns complete process resources whose topology
// store is store, as main hands them to a component.
func newSingleProcessResources(store topoclient.Store) servenv.ProcessResources {
	reg := viperutil.NewRegistry()
	tel := telemetry.NewTelemetry()
	return servenv.ProcessResources{
		ServEnv:    servenv.NewServEnvWithConfig(reg, servenv.NewLogger(reg, tel), viperutil.NewViperConfig(reg), tel),
		GrpcServer: servenv.NewGrpcServer(reg),
		TopoStore:  func() topoclient.Store { return store },
	}
}

func TestInitProcess_SingleProcessModeFailsWithoutOpenStore(t *testing.T) {
	resources := newSingleProcessResources(nil)
	mp := NewMultipooler(telemetry.NewTelemetry(), WithSingleProcessMode(resources))

	err := mp.initProcess("id", "zone1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "topology store is not open")
}

func TestInitProcess_SingleProcessModeUsesMainsStore(t *testing.T) {
	store := memorytopo.NewServer(t.Context(), "zone1")
	defer store.Close()
	resources := newSingleProcessResources(store)
	mp := NewMultipooler(telemetry.NewTelemetry(), WithSingleProcessMode(resources))

	require.NoError(t, mp.initProcess("id", "zone1"))
	assert.Same(t, store, mp.ts)
}
