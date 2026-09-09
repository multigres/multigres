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
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

// TestHTTPAuthMtlsAllowedSubjectsFlag pins how the allow-list is supplied: one
// subject per flag occurrence, reaching the same value from a repeated command
// line or a config-file list, with entry commas intact.
func TestHTTPAuthMtlsAllowedSubjectsFlag(t *testing.T) {
	setup := func(t *testing.T, args ...string) *ServEnv {
		t.Helper()
		reg := viperutil.NewRegistry()
		se := NewServEnv(reg)
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		se.RegisterFlags(fs)
		require.NoError(t, fs.Parse(args))
		cancel, err := se.vc.LoadConfig(reg)
		require.NoError(t, err)
		t.Cleanup(cancel)
		return se
	}

	t.Run("repeated on the command line", func(t *testing.T) {
		se := setup(t,
			"--config-file-not-found-handling", "ignore",
			"--http-auth-mtls-allowed-subjects", "CN=ns-a,O=acme",
			"--http-auth-mtls-allowed-subjects", "CN=ns-b",
		)
		assert.Equal(t, []string{"CN=ns-a,O=acme", "CN=ns-b"}, se.httpAuthMtlsAllowedSubjects.Get())
	})

	// Entries carry commas of their own, which is what one-per-occurrence buys.
	t.Run("entry commas survive", func(t *testing.T) {
		se := setup(t,
			"--config-file-not-found-handling", "ignore",
			"--http-auth-mtls-allowed-subjects", "CN=ns-a,O=acme,OU=platform",
		)
		require.Equal(t, []string{"CN=ns-a,O=acme,OU=platform"}, se.httpAuthMtlsAllowedSubjects.Get())

		subjects, err := parseCertSubjects(se.httpAuthMtlsAllowedSubjects.Get())
		require.NoError(t, err)
		require.Len(t, subjects, 1)
		assert.Equal(t, map[string][]string{"CN": {"ns-a"}, "O": {"acme"}, "OU": {"platform"}}, subjects[0].attrs)
	})

	t.Run("read from a config file as a list", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "multigres.yaml")
		require.NoError(t, os.WriteFile(path, []byte(
			"http-auth-mtls-allowed-subjects:\n  - CN=ns-a,O=acme\n  - CN=ns-b\n"), 0o600))

		se := setup(t, "--config-file", path)
		assert.Equal(t, []string{"CN=ns-a,O=acme", "CN=ns-b"}, se.httpAuthMtlsAllowedSubjects.Get(),
			"the allow-list must be settable from the config file, not just the command line")
	})

	t.Run("unset is empty", func(t *testing.T) {
		se := setup(t, "--config-file-not-found-handling", "ignore")
		assert.Empty(t, se.httpAuthMtlsAllowedSubjects.Get())
	})
}
