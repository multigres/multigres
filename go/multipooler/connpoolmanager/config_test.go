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

func TestConfig_InternalUser_Default(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Default value should be "postgres"
	assert.Equal(t, "postgres", config.InternalUser())
}

func TestConfig_InternalUser_CustomValue(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Register flags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	// Parse a custom value
	err := fs.Parse([]string{"--connpool-internal-user", "custom_internal_user"})
	require.NoError(t, err)

	// Bind to viper
	v := viper.New()
	err = v.BindPFlags(fs)
	require.NoError(t, err)

	// The value should be available through viper
	assert.Equal(t, "custom_internal_user", v.GetString("connpool-internal-user"))
}

func TestConfig_InternalUser_EnvVar(t *testing.T) {
	// Set environment variable
	t.Setenv("CONNPOOL_INTERNAL_USER", "env_internal_user")

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Register flags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	// Bind to viper
	v := viper.New()
	v.AutomaticEnv()
	err := v.BindPFlags(fs)
	require.NoError(t, err)

	// The environment variable should be picked up
	assert.Equal(t, "env_internal_user", v.GetString("CONNPOOL_INTERNAL_USER"))
}

func TestConfig_InternalUserFlagRegistration(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	// Verify the flag is registered
	flag := fs.Lookup("connpool-internal-user")
	require.NotNil(t, flag, "connpool-internal-user flag should be registered")
	assert.Equal(t, "postgres", flag.DefValue, "default value should be postgres")
	assert.Contains(t, flag.Usage, "Internal user for multipooler system queries")
}
