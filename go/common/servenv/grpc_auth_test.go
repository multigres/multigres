// Copyright 2025 Supabase, Inc.
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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAuthenticator_UnknownPlugin(t *testing.T) {
	_, err := GetAuthenticator("nonexistent-plugin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent-plugin")
	assert.Contains(t, err.Error(), "no AuthPlugin")
}

func TestRegisterAuthPlugin_Duplicate(t *testing.T) {
	// Create a unique plugin name for this test to avoid conflicts
	pluginName := "test-duplicate-plugin"

	// First registration should succeed
	dummyPlugin := func() (Authenticator, error) {
		return nil, nil
	}
	err := RegisterAuthPlugin(pluginName, dummyPlugin)
	require.NoError(t, err)

	// Second registration with same name should fail
	err = RegisterAuthPlugin(pluginName, dummyPlugin)
	require.Error(t, err)
	assert.Contains(t, err.Error(), pluginName)
	assert.Contains(t, err.Error(), "already registered")
}

func TestRegisterAndGetAuthPlugin(t *testing.T) {
	pluginName := "test-success-plugin"

	// Create a mock authenticator
	mockAuth := &mockAuthenticator{}
	pluginInit := func() (Authenticator, error) {
		return mockAuth, nil
	}

	// Register the plugin
	err := RegisterAuthPlugin(pluginName, pluginInit)
	require.NoError(t, err)

	// Get the plugin
	initializer, err := GetAuthenticator(pluginName)
	require.NoError(t, err)
	require.NotNil(t, initializer)

	// Initialize and verify it returns our mock
	auth, err := initializer()
	require.NoError(t, err)
	assert.Equal(t, mockAuth, auth)
}

type mockAuthenticator struct{}

func (m *mockAuthenticator) Authenticate(ctx context.Context, fullMethod string) (context.Context, error) {
	return ctx, nil
}
