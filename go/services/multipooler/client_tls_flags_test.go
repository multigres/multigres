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
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

var clientTLSFlagNames = []string{
	"multipooler-grpc-cert", "multipooler-grpc-key", "multipooler-grpc-ca",
	"multipooler-grpc-crl", "multipooler-grpc-server-name", "multipooler-grpc-require-tls",
}

// TestTheClientTLSFlagsOfTheDefaultPrimaryDialAreRegistered: a pooler that dials the default primary
// takes the TLS settings of the dial from the --multipooler-grpc-* flags, so they exist on the
// multipooler, next to the flags every other component of the process registers (a clash of names
// would panic here, as it would in the binary), and TLSConfigured follows them.
func TestTheClientTLSFlagsOfTheDefaultPrimaryDialAreRegistered(t *testing.T) {
	reg := viperutil.NewRegistry()
	senv := servenv.NewServEnv(reg)
	grpcServer := servenv.NewGrpcServer(reg)
	mp := NewMultipooler(telemetry.NewTelemetry(), reg, servenv.ProcessResources{
		ServEnv: senv, GrpcServer: grpcServer, QueryGrpcServer: grpcServer,
	}, "/", false)

	// What the standalone binary registers.
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	senv.RegisterFlags(fs)
	grpcServer.RegisterFlags(fs)
	mp.RegisterFlags(fs)
	for _, name := range clientTLSFlagNames {
		require.NotNil(t, fs.Lookup(name), name)
	}

	require.NotNil(t, mp.defaultPrimaryConn)
	require.False(t, mp.defaultPrimaryConn.TLSConfigured(), "no client flag set")
	require.NoError(t, fs.Parse([]string{"--multipooler-grpc-ca=/ca.pem"}))
	require.True(t, mp.defaultPrimaryConn.TLSConfigured(), "a CA is enough to build TLS credentials")
}

// TestAStaticLeaderRegistersNoClientTLSFlags: a static leader is the single-process pooler, which
// never dials a default primary; its process also hosts a gateway that owns flags of these names.
func TestAStaticLeaderRegistersNoClientTLSFlags(t *testing.T) {
	reg := viperutil.NewRegistry()
	grpcServer := servenv.NewGrpcServer(reg)
	mp := NewMultipooler(telemetry.NewTelemetry(), reg, servenv.ProcessResources{
		ServEnv: servenv.NewServEnv(reg), GrpcServer: grpcServer, QueryGrpcServer: grpcServer,
	}, "/", true)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	mp.RegisterFlags(fs)
	require.Nil(t, mp.defaultPrimaryConn)
	for _, name := range clientTLSFlagNames {
		require.Nil(t, fs.Lookup(name), name)
	}
}
