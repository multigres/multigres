// Copyright 2025 Supabase, Inc.
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

package viperutil

import (
	"maps"
	"os"
	"slices"
	gosync "sync"

	"github.com/spf13/viper"

	"github.com/multigres/multigres/go/tools/viperutil/internal/sync"
)

// Registry holds the static and dynamic viper instances for configuration.
// This replaces the global registry pattern, allowing each service/command to
// have its own isolated configuration registry.
//
// Static registry values never change after LoadConfig is called.
// Dynamic registry values can be updated by watching a config file for changes.
type Registry struct {
	// static is the registry for static config variables. These variables will
	// never be affected by a Watch-ed config, and maintain their original
	// values for the lifetime of the process.
	static *viper.Viper

	// dynamic is the registry for dynamic config variables. If a config file is
	// found by viper, it will be watched by a threadsafe wrapper around a
	// second viper (see sync.Viper), and variables registered to it will pick
	// up changes to that config file throughout the lifetime of the process.
	dynamic *sync.Viper

	// prefix is put in front of every key configured on this view (WithPrefix).
	prefix string

	// prepare and configured are shared by a registry and its views.
	prepare    *ConfigPreparer
	configured *configuredKeys
}

type configuredKeys struct {
	mu   gosync.Mutex
	keys map[string]struct{}
	env  map[string][]string
}

func (c *configuredKeys) record(key string, envVars []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[key] = struct{}{}
	for _, env := range envVars {
		c.env[env] = append(c.env[env], key)
	}
}

// Keys returns the full key of every value configured on reg and its views.
func (reg *Registry) Keys() []string {
	reg.configured.mu.Lock()
	defer reg.configured.mu.Unlock()
	return slices.Collect(maps.Keys(reg.configured.keys))
}

// EnvVars maps each environment variable read by values on reg and its views
// to their full keys.
func (reg *Registry) EnvVars() map[string][]string {
	reg.configured.mu.Lock()
	defer reg.configured.mu.Unlock()
	env := make(map[string][]string, len(reg.configured.env))
	for name, keys := range reg.configured.env {
		env[name] = slices.Clone(keys)
	}
	return env
}

// ConfigPreparer gets the config file's settings, keyed in dotted form
// ("multigateway.pg-port"), and returns settings to add to them, ranking as the
// file's. An error rejects the file: LoadConfig fails, or a reload is skipped
// and the previous settings stay.
type ConfigPreparer func(settings map[string]any) (map[string]any, error)

// NewRegistry creates a new isolated configuration registry.
// This is the preferred way to create registries for services and commands,
// as it provides complete isolation from other parts of the application.
//
// Example usage:
//
//	reg := viperutil.NewRegistry()
//	poolerDir := viperutil.Configure(reg, "pooler-dir", viperutil.Options[string]{
//	    Default: "",
//	    FlagName: "pooler-dir",
//	})
func NewRegistry() *Registry {
	return &Registry{
		static:  viper.New(),
		dynamic: sync.New(),
		prepare: new(ConfigPreparer),
		configured: &configuredKeys{
			keys: map[string]struct{}{},
			env:  map[string][]string{},
		},
	}
}

// SetConfigPreparer runs prepare on every load and reload of the config file.
// Must be called before LoadConfig.
func (reg *Registry) SetConfigPreparer(prepare ConfigPreparer) {
	*reg.prepare = prepare
}

// WithPrefix returns a view of reg that stores a value configured as "pg-port"
// under "<prefix>.pg-port", so components with keys of the same name can share
// one registry and config file. Flag and environment variable names do not
// change.
func (reg *Registry) WithPrefix(prefix string) *Registry {
	return &Registry{
		static:     reg.static,
		dynamic:    reg.dynamic,
		prefix:     reg.Key(prefix),
		prepare:    reg.prepare,
		configured: reg.configured,
	}
}

// Key returns the full key under which reg stores name.
func (reg *Registry) Key(name string) string {
	if reg.prefix == "" {
		return name
	}
	return reg.prefix + "." + name
}

// InStaticConfig reports whether the key was present in the loaded static
// config file. Unlike viper's IsSet — which is true for every registered value
// because Configure installs its default via SetDefault — this consults only
// the parsed config-file contents, so callers can distinguish "operator wrote
// this key" from "value fell through to the default". Always false before
// LoadConfig has run (or when no config file was found).
func (reg *Registry) InStaticConfig(key string) bool {
	return reg.static.InConfig(key)
}

// ConfigFileLoaded returns the path of the static config file that LoadConfig
// read, or "" when no file was read (none configured, or a configured file that
// does not exist and was tolerated by the not-found handling).
func (reg *Registry) ConfigFileLoaded() string {
	file := reg.static.ConfigFileUsed()
	if file == "" {
		return ""
	}
	if _, err := os.Stat(file); err != nil {
		return ""
	}
	return file
}

// Combined returns a viper instance combining the static and dynamic registries.
// This is useful for debug handlers and other utilities that need to access
// all configuration values.
func (reg *Registry) Combined() *viper.Viper {
	v := viper.New()
	_ = v.MergeConfigMap(reg.static.AllSettings())
	_ = v.MergeConfigMap(reg.dynamic.AllSettings())

	v.SetConfigFile(reg.static.ConfigFileUsed())
	return v
}
