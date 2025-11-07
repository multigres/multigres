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
	"github.com/spf13/viper"

	"github.com/multigres/multigres/go/viperutil/internal/sync"
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
}

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
	}
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
