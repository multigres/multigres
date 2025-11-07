// Copyright 2023 The Vitess Authors.
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
//
// Modifications Copyright 2025 Supabase, Inc.

package debug

import (
	"github.com/multigres/multigres/go/viperutil"
)

// Debug provides the Debug functionality normally accessible to a given viper
// instance, but for a combination of the static and dynamic registries.
func Debug(reg *viperutil.Registry) {
	reg.Combined().Debug()
}

// WriteConfigAs writes the config into the given filename.
func WriteConfigAs(reg *viperutil.Registry, filename string) error {
	return reg.Combined().WriteConfigAs(filename)
}

// AllSettings gets all the settings in the configuration.
func AllSettings(reg *viperutil.Registry) map[string]any {
	return reg.Combined().AllSettings()
}
