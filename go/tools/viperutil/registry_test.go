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
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/testpoll"
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

// prefixedSetup configures pg-port and slots on two views of one registry, the
// way a process running two components does.
type prefixedSetup struct {
	reg          *Registry
	gatewayPort  Value[int]
	poolerPort   Value[int]
	gatewaySlots Value[bool]
	poolerSlots  Value[bool]
	flags        *pflag.FlagSet
}

func newPrefixedSetup(t *testing.T) *prefixedSetup {
	t.Helper()
	reg := NewRegistry()
	gateway, pooler := reg.WithPrefix("multigateway"), reg.WithPrefix("multipooler")
	s := &prefixedSetup{
		reg:          reg,
		gatewayPort:  Configure(gateway, "pg-port", Options[int]{Default: 1, FlagName: "pg-port", EnvVars: []string{"MT_TEST_PG_PORT"}}),
		poolerPort:   Configure(pooler, "pg-port", Options[int]{Default: 2, FlagName: "pooler-pg-port"}),
		gatewaySlots: Configure(gateway, "slots", Options[bool]{FlagName: "slots", Dynamic: true}),
		poolerSlots:  Configure(pooler, "slots", Options[bool]{Dynamic: true}),
		flags:        pflag.NewFlagSet("test", pflag.ContinueOnError),
	}
	s.flags.Int("pg-port", 1, "")
	s.flags.Int("pooler-pg-port", 2, "")
	s.flags.Bool("slots", false, "")
	BindFlags(s.flags, s.gatewayPort, s.poolerPort, s.gatewaySlots)
	return s
}

// load writes yaml to a config file, loads it and returns its path.
func (s *prefixedSetup) load(t *testing.T, yaml string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(file, []byte(yaml), 0o600))
	vc := NewViperConfig(s.reg)
	fs := pflag.NewFlagSet("config", pflag.ContinueOnError)
	vc.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config-file=" + file}))
	cancel, err := vc.LoadConfig(s.reg)
	require.NoError(t, err)
	t.Cleanup(cancel)
	return file
}

func TestWithPrefix(t *testing.T) {
	s := newPrefixedSetup(t)
	assert.Equal(t, "multigateway.pg-port", s.gatewayPort.Key())
	assert.Equal(t, "multipooler.pooler-pg-port", s.poolerPort.FlagAlias(), "the flag alias carries the prefix too")
	assert.Equal(t, "pg-port", NewRegistry().Key("pg-port"), "a registry without a prefix is unchanged")

	t.Setenv("MT_TEST_PG_PORT", "16000")
	require.NoError(t, s.flags.Parse([]string{"--pooler-pg-port=6000"}))
	s.load(t, "multigateway:\n  pg-port: 15432\nmultipooler.pg-port: 5432\nmultipooler.slots: true\n")
	assert.Equal(t, 16000, s.gatewayPort.Get(), "flag and environment variable names are unchanged and win over the file")
	assert.Equal(t, 6000, s.poolerPort.Get())
	assert.False(t, s.gatewaySlots.Get(), "the same name in another view is another key, dynamic or not")
	assert.True(t, s.poolerSlots.Get(), "flat dotted and nested forms are both read")
}

// copySlots copies a top-level "slots" into both views and rejects "bogus".
func copySlots(settings map[string]any) (map[string]any, error) {
	if _, ok := settings["bogus"]; ok {
		return nil, errors.New("unknown setting bogus")
	}
	return map[string]any{"multigateway.slots": settings["slots"], "multipooler.slots": settings["slots"]}, nil
}

func TestConfigPreparer(t *testing.T) {
	s := newPrefixedSetup(t)
	s.reg.SetConfigPreparer(copySlots)
	reloaded := make(chan struct{}, 1)
	NotifyConfigReload(s.reg, reloaded)
	require.NoError(t, s.flags.Parse([]string{"--slots=false"}))
	file := s.load(t, "slots: true\n")
	assert.False(t, s.gatewaySlots.Get(), "a flag wins over an added setting")
	assert.True(t, s.poolerSlots.Get(), "settings are added at load")

	// A rejected change leaves the previous settings live; the next valid one
	// goes live.
	writeConfigChange(t, file, "slots: false\nbogus: 1\n")
	testpoll.Never(t, func() bool { return !s.poolerSlots.Get() }, time.Second, 50*time.Millisecond)
	writeConfigChange(t, file, "slots: false\n")
	select {
	case <-reloaded:
	case <-time.After(10 * time.Second):
		t.Fatal("the change was not reloaded")
	}
	assert.False(t, s.poolerSlots.Get())
}

// writeConfigChange replaces file's contents so the config watcher sees one
// change, as internal/sync's tests do: in place on darwin, by rename on linux.
func writeConfigChange(t *testing.T, file, contents string) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		require.NoError(t, os.WriteFile(file, []byte(contents), 0o600))
		return
	}
	require.NoError(t, os.WriteFile(file+".tmp", []byte(contents), 0o600))
	require.NoError(t, os.Rename(file+".tmp", file))
}
