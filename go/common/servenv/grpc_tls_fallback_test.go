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

	"github.com/multigres/multigres/go/tools/viperutil"
)

// setupGRPCAndServEnv builds a GrpcServer and ServEnv on one registry with
// both RegisterFlags called, parses args, and loads config - the same shape
// every real command follows, so a legacy flag registered on GrpcServer (like
// --grpc-cert) and a canonical one on ServEnv (like --tls-cert) are both live
// by the time Create runs.
func setupGRPCAndServEnv(t *testing.T, args ...string) (*GrpcServer, *ServEnv) {
	t.Helper()
	reg := viperutil.NewRegistry()
	g := NewGrpcServer(reg)
	g.port.Set(12345)
	sv := NewServEnv(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	sv.RegisterFlags(fs)
	g.RegisterFlags(fs)
	require.NoError(t, fs.Parse(append([]string{"--config-file-not-found-handling", "ignore"}, args...)))
	cancel, err := sv.vc.LoadConfig(reg)
	require.NoError(t, err)
	t.Cleanup(cancel)
	return g, sv
}

// TestGRPCTLSFallback_LegacyFlag proves --grpc-cert/-key/-ca are real,
// independent flags that still configure gRPC TLS on their own - not aliases
// of --tls-cert/-key/-ca, but a fallback Create resolves explicitly.
func TestGRPCTLSFallback_LegacyFlag(t *testing.T) {
	cert, key := writeTestCertKey(t)

	t.Run("--grpc-cert alone still enables gRPC TLS", func(t *testing.T) {
		g, sv := setupGRPCAndServEnv(t, "--grpc-cert", cert, "--grpc-key", key)
		require.NoError(t, g.Create(sv))
		require.NotNil(t, g.Server)
		assert.Equal(t, cert, g.Cert(), "Cert() must reflect the resolved (fallback) value")
		assert.Equal(t, key, g.Key())
	})

	t.Run("--grpc-cert is hidden from help but still a registered flag", func(t *testing.T) {
		reg := viperutil.NewRegistry()
		g := NewGrpcServer(reg)
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		g.RegisterFlags(fs)

		flag := fs.Lookup("grpc-cert")
		require.NotNil(t, flag)
		assert.True(t, flag.Hidden, "deprecated flags must not show up in --help")
		assert.NotEmpty(t, flag.Deprecated)
	})
}

// TestGRPCTLSFallback_CanonicalWins proves --tls-cert/-key/-ca take precedence
// over the deprecated --grpc-cert/-key/-ca when both are set, rather than one
// silently overwriting the other or erroring.
func TestGRPCTLSFallback_CanonicalWins(t *testing.T) {
	canonicalCert, canonicalKey := writeTestCertKey(t)
	legacyCert, legacyKey := writeTestCertKey(t)
	require.NotEqual(t, canonicalCert, legacyCert, "test needs two distinct cert files to prove which one wins")

	g, sv := setupGRPCAndServEnv(t,
		"--tls-cert", canonicalCert, "--tls-key", canonicalKey,
		"--grpc-cert", legacyCert, "--grpc-key", legacyKey,
	)
	require.NoError(t, g.Create(sv))
	assert.Equal(t, canonicalCert, g.Cert(), "--tls-cert must win over --grpc-cert when both are set")
	assert.Equal(t, canonicalKey, g.Key())
}

// TestResolveTLSPath pins the precedence rule in isolation, independent of any
// flag or TLS machinery.
func TestResolveTLSPath(t *testing.T) {
	assert.Equal(t, "canonical", resolveTLSPath("canonical", "legacy"))
	assert.Equal(t, "legacy", resolveTLSPath("", "legacy"))
	assert.Equal(t, "", resolveTLSPath("", ""))
}
