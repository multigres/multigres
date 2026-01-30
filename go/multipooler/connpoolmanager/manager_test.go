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

package connpoolmanager

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestManager_InternalUser_Default(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager()

	// Manager should return the default internal user from config
	assert.Equal(t, "postgres", manager.InternalUser())
}

func TestManager_InternalUser_CustomValue(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Register flags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	// Parse a custom value
	err := fs.Parse([]string{"--connpool-internal-user", "replication_user"})
	require.NoError(t, err)

	// Bind to viper (this is what happens in the real application)
	v := viper.New()
	err = v.BindPFlags(fs)
	require.NoError(t, err)

	manager := config.NewManager()

	// Manager should delegate to config and return the configured value
	// The viperutil binding picks up the flag value correctly
	assert.Equal(t, "replication_user", manager.InternalUser(),
		"Manager should return the custom internal user set via flag")
}

func TestManager_InternalUser_DelegatestoConfig(t *testing.T) {
	// Create a config with default values
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Create manager from config
	manager := config.NewManager()

	// Verify Manager.InternalUser() delegates to Config.InternalUser()
	assert.Equal(t, config.InternalUser(), manager.InternalUser(),
		"Manager.InternalUser() should return the same value as Config.InternalUser()")
}
