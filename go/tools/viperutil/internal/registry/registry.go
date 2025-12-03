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

package registry

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/multigres/multigres/go/tools/viperutil/internal/sync"
)

var (
	_ Bindable = (*viper.Viper)(nil)
	_ Bindable = (*sync.Viper)(nil)
)

// Bindable represents the methods needed to bind a value.Value to a given
// registry. It exists primarily to allow us to treat a sync.Viper as a
// viper.Viper for configuration registration purposes.
type Bindable interface {
	BindEnv(vars ...string) error
	BindPFlag(key string, flag *pflag.Flag) error
	RegisterAlias(alias string, key string)
	SetDefault(key string, value any)
}
