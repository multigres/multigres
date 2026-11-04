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

package pgbackrestd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigRejectsInvalidLaunchInputs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"relative state", func(c *Config) { c.StateDir = "relative" }, "--state-dir must be an absolute path"},
		{"invalid anchor", func(c *Config) { c.SandboxAnchor = "unknown" }, "--sandbox-anchor must be"},
		{"missing bucket", func(c *Config) { c.S3Bucket = "" }, "requires --s3-bucket and --s3-region"},
		{"missing region", func(c *Config) { c.S3Region = "" }, "requires --s3-bucket and --s3-region"},
		{"missing static directory", func(c *Config) { c.S3StaticKeyDir = "" }, "requires --s3-static-key-dir"},
		{"missing credential endpoint", func(c *Config) { c.S3CredSource = CredEndpoint }, "requires --s3-cred-endpoint"},
		{"unknown credential source", func(c *Config) { c.S3CredSource = "unknown" }, "unknown --s3-cred-source"},
		{"unknown location", func(c *Config) { c.LocationType = "unknown" }, "--location-type must be"},
		{"zero port", func(c *Config) { c.ServerPort = 0 }, "--server-port 0 out of range"},
		{"overflow port", func(c *Config) { c.ServerPort = 65536 }, "--server-port 65536 out of range"},
		{"zero fetch concurrency", func(c *Config) { c.MaxFetches = 0 }, "--max-fetches must be at least 1"},
		{"malformed queue size", func(c *Config) { c.ArchiveGetQueueMax = "256 MiB" }, "is not a size"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			require.NoError(t, cfg.Validate(), "fixture must otherwise be valid")
			tc.mutate(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}
}

func TestConfigAcceptsCredentialModesAndPortBoundaries(t *testing.T) {
	for _, source := range []string{CredWebIdentity, CredPodIdentity, CredNone, CredStatic, CredEndpoint} {
		for _, port := range []int{1, 65535} {
			cfg := testConfig(t)
			cfg.S3CredSource, cfg.ServerPort = source, port
			cfg.S3CredEndpoint = "https://credentials.invalid/session"
			require.NoError(t, cfg.Validate(), "source=%s port=%d", source, port)
		}
	}
}
