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
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// multiPoolerFlags are the flags of the features that need several poolers or
// gateways: replica reads, failover buffering and the multipooler gRPC TLS
// settings, which also secure cancel forwarding.
var multiPoolerFlags = []string{
	"pg-replica-port", "low-replication-lag-ms", "high-replication-lag-tolerance-ms",
	"buffer-enabled", "buffer-window",
	"multipooler-grpc-cert", "multipooler-grpc-require-tls",
}

func newTestMultigateway(t *testing.T, singlePooler bool) (*Multigateway, *pflag.FlagSet) {
	t.Helper()
	reg := viperutil.NewRegistry()
	grpcServer := servenv.NewGrpcServer(reg)
	mg := NewMultigateway(reg, servenv.ProcessResources{
		ServEnv:         servenv.NewServEnv(reg),
		GrpcServer:      grpcServer,
		QueryGrpcServer: grpcServer,
	}, "/", nil, singlePooler)
	fs := pflag.NewFlagSet("multigateway", pflag.ContinueOnError)
	mg.RegisterFlags(fs)
	return mg, fs
}

func TestNewMultigateway_RegistersMultiPoolerFlags(t *testing.T) {
	_, fs := newTestMultigateway(t, false)
	for _, name := range multiPoolerFlags {
		assert.NotNil(t, fs.Lookup(name), "flag %q should be registered", name)
	}
}

func TestNewMultigateway_SinglePoolerOmitsMultiPoolerFlags(t *testing.T) {
	_, fs := newTestMultigateway(t, true)
	for _, name := range multiPoolerFlags {
		assert.Nil(t, fs.Lookup(name), "flag %q should not be registered", name)
	}
	assert.NotNil(t, fs.Lookup("pg-port"), "the gateway's other flags stay")
}

func TestNewMultigateway_SinglePoolerIgnoresEnvironment(t *testing.T) {
	t.Setenv("MT_PG_REPLICA_PORT", "15433")
	t.Setenv("MT_LOW_REPLICATION_LAG_MS", "100")
	t.Setenv("MT_BUFFER_ENABLED", "true")

	mg, _ := newTestMultigateway(t, true)

	assert.Equal(t, 0, mg.replicaConfig.Port(), "no replica listener")
	assert.Equal(t, time.Duration(0), mg.replicaConfig.LowLag())
	assert.Nil(t, mg.bufferConfig, "no failover buffer")
	assert.Nil(t, mg.connConfig, "no multipooler gRPC TLS settings")
}

func TestReplicaConfig(t *testing.T) {
	mg, fs := newTestMultigateway(t, false)
	require.NoError(t, fs.Parse([]string{
		"--pg-replica-port=15433", "--low-replication-lag-ms=100", "--high-replication-lag-tolerance-ms=2000",
	}))

	assert.Equal(t, 15433, mg.replicaConfig.Port())
	assert.Equal(t, 100*time.Millisecond, mg.replicaConfig.LowLag())
	assert.Equal(t, 2*time.Second, mg.replicaConfig.HighTolerance())
}
