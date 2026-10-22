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

package viperutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadConfigWithFile runs LoadConfig on a fresh registry with --config-file set
// to file (unset when empty).
func loadConfigWithFile(t *testing.T, file string) *Registry {
	t.Helper()
	reg := NewRegistry()
	vc := NewViperConfig(reg)
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	vc.RegisterFlags(fs)
	var args []string
	if file != "" {
		args = append(args, "--config-file="+file)
	}
	require.NoError(t, fs.Parse(args))
	cancel, err := vc.LoadConfig(reg)
	require.NoError(t, err)
	t.Cleanup(cancel)
	return reg
}

func TestConfigFileLoaded(t *testing.T) {
	t.Run("no config file", func(t *testing.T) {
		reg := loadConfigWithFile(t, "")
		assert.Empty(t, reg.ConfigFileLoaded())
	})

	t.Run("config file read", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(file, []byte("foo: bar\n"), 0o600))
		reg := loadConfigWithFile(t, file)
		assert.Equal(t, file, reg.ConfigFileLoaded())
	})

	t.Run("configured file missing", func(t *testing.T) {
		reg := loadConfigWithFile(t, filepath.Join(t.TempDir(), "missing.yaml"))
		assert.Empty(t, reg.ConfigFileLoaded())
	})
}
