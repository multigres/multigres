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
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// The otelgrpc stats handler runs on every RPC. These tests pin the switch
// that removes it: the server option disappears, and gRPC clients created by
// grpccommon.NewClient in the same process follow the same setting.

func TestGrpcServerOTelInstrumentationDefaultOn(t *testing.T) {
	t.Cleanup(func() { grpccommon.SetOTelInstrumentationEnabled(nil) })

	g := NewGrpcServer(viperutil.NewRegistry())

	assert.True(t, g.OTelInstrumentationEnabled(), "instrumentation must stay on unless configured off")
	assert.NotNil(t, g.otelServerOption(), "server option attaches the stats handler by default")
	assert.True(t, grpccommon.OTelInstrumentationEnabled(), "clients follow the server setting")
}

func TestGrpcServerOTelInstrumentationOff(t *testing.T) {
	t.Cleanup(func() { grpccommon.SetOTelInstrumentationEnabled(nil) })

	g := NewGrpcServer(viperutil.NewRegistry())
	g.otelInstrumentation.Set(false)

	assert.False(t, g.OTelInstrumentationEnabled())
	assert.Nil(t, g.otelServerOption(), "no stats handler when disabled")
	assert.False(t, grpccommon.OTelInstrumentationEnabled(),
		"the client-side decision is evaluated lazily, so it sees the value set after construction")
}

func TestGrpcServerOTelInstrumentationFlag(t *testing.T) {
	t.Cleanup(func() { grpccommon.SetOTelInstrumentationEnabled(nil) })

	g := NewGrpcServer(viperutil.NewRegistry())
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	g.RegisterFlags(fs)

	f := fs.Lookup("grpc-otel-instrumentation")
	require.NotNil(t, f, "flag must be registered")
	assert.Equal(t, "true", f.DefValue)

	require.NoError(t, fs.Parse([]string{"--grpc-otel-instrumentation=false"}))
	assert.False(t, g.OTelInstrumentationEnabled(), "parsed flag value is visible through the viper binding")
	assert.Nil(t, g.otelServerOption())
}
